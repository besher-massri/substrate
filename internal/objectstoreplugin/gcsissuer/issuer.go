// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package gcsissuer mints GCS tokens downscoped to an actor's grants. It is the
// GCS half of the control plugin's credential exchange: actorscope decides what
// an actor may touch, this package turns that into a credential GCS enforces.
//
// The source credential comes from the default credential chain, which on GKE
// with Workload Identity is the federated token. The downscope package
// exchanges it at STS with a Credential Access Boundary and returns an access
// token GCS limits to the boundary. STS reports no lifetime for that token, so
// the package falls back to the source token's expiry, and the two expire
// together.
package gcsissuer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/google/downscope"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/lru"

	"go.ate.dev/substrate/internal/objectstoreplugin/actorscope"
	"go.ate.dev/substrate/internal/objectstoreplugin/credential"
	"go.ate.dev/substrate/internal/objectstoreplugin/gcsboundary"
)

const (
	// cloudPlatformScope is the scope the source token is requested with. The
	// boundary narrows it to the actor's prefixes.
	cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

	// minServeLifetime is the least remaining lifetime a token is cached and
	// served with, the same on the node plugin's side.
	minServeLifetime = credential.MinServeLifetime
	// defaultCacheSize bounds the per-actor cache. A miss costs an exchange,
	// never correctness.
	defaultCacheSize = 100_000
	// defaultROCacheSize bounds the shared boundary-rule cache (used when
	// cold actors restore from a shared template tag or when actor prefixes
	// are sharded by hex prefix).
	defaultROCacheSize = 10_000
	// defaultActorPrefixLen is the default hex prefix shard length used for
	// ReadWrite actor snapshot prefixes so STS token minting stays within
	// project rate limits.
	defaultActorPrefixLen = 1
	// stsTimeout bounds one exchange. The downscope package does not take a
	// per-call context, so the client's timeout is the only bound.
	stsTimeout = 30 * time.Second
)

// Issuer exchanges the pod's credential for per-actor downscoped tokens and
// caches them by actor UID (and by boundary rules for shared tag or prefix-sharded grants).
type Issuer struct {
	source         oauth2.TokenSource
	client         *http.Client
	now            func() time.Time
	actorPrefixLen int

	mu       sync.Mutex
	cache    *lru.Cache
	roCache  *lru.Cache
	inflight map[string]*inflightMint
}

type inflightMint struct {
	done chan struct{}
	tok  credential.Token
	err  error
}

// entry is a cached token with the grants it was minted for, so a request can
// tell whether the token covers it.
type entry struct {
	token  credential.Token
	grants []actorscope.Grant
}

// Option configures an Issuer.
type Option func(*Issuer)

// WithTokenSource overrides the source credential, for tests.
func WithTokenSource(ts oauth2.TokenSource) Option { return func(i *Issuer) { i.source = ts } }

// WithHTTPClient overrides the client used for STS, for tests.
func WithHTTPClient(c *http.Client) Option { return func(i *Issuer) { i.client = c } }

// WithClock overrides the clock, for tests.
func WithClock(now func() time.Time) Option { return func(i *Issuer) { i.now = now } }

// WithCacheSize bounds the token cache. A size below one is treated as one,
// since lru.New(0) would mean no bound at all.
func WithCacheSize(n int) Option {
	return func(i *Issuer) { i.cache = lru.New(max(n, 1)) }
}

// WithActorPrefixLen configures the hex prefix shard length for ReadWrite
// actor snapshot prefixes (0 disables prefix sharding and uses full actor UIDs).
func WithActorPrefixLen(n int) Option {
	return func(i *Issuer) { i.actorPrefixLen = max(n, 0) }
}

// New returns an Issuer whose source credential is the default credential
// chain, unless an option replaces it. The credential keeps refreshing for the
// Issuer's whole life, so ctx is detached from cancellation.
func New(ctx context.Context, opts ...Option) (*Issuer, error) {
	i := &Issuer{
		client:         &http.Client{Timeout: stsTimeout},
		now:            time.Now,
		actorPrefixLen: defaultActorPrefixLen,
		cache:          lru.New(defaultCacheSize),
		roCache:        lru.New(defaultROCacheSize),
		inflight:       make(map[string]*inflightMint),
	}
	for _, o := range opts {
		o(i)
	}
	if i.source == nil {
		creds, err := google.FindDefaultCredentialsWithParams(context.WithoutCancel(ctx), google.CredentialsParams{
			Scopes: []string{cloudPlatformScope},
		})
		if err != nil {
			return nil, fmt.Errorf("while loading the default credential: %w", err)
		}
		i.source = creds.TokenSource
	}
	return i, nil
}

// Issue returns a token downscoped to grants for the actor. A cached token is
// returned while it covers the grants and has at least minServeLifetime left,
// unless force is set, which mints regardless and is what the node plugin
// sends after GCS refused a token. A minted token with less than
// minServeLifetime left is handed out anyway, since a short token beats a
// failed transfer, but it is not cached. That happens for a stretch at the
// end of every hour of the source credential's life: the downscoped token
// expires with its source, and the GKE metadata server serves the same cached
// source token until shortly before it expires, so no early refresh helps. In
// that stretch every exchange reaches STS and the node plugin re-exchanges
// once if a transfer outlives its token.
//
// The cache is keyed by actor UID and each entry remembers its grants. Grants
// narrow when the tag an actor was created from drops out after its first
// completed suspend, and the cached token still covers that, a hit. They
// widen when a never-suspended actor is repointed to another storage
// location, and the cached token lacks it, a mint. So the common path costs
// nothing and a repoint never hands out a token GCS would refuse. When all
// requested grants are ReadOnly (such as a cold restore from a shared template
// tag) or when actorPrefixLen > 0 shards ReadWrite actor prefixes, the token
// is also cached by its canonical access boundary rules in roCache.
//
// A grant set the access boundary cannot express fails with codes.Internal.
// Every other failure is an STS or network error and carries no code.
func (i *Issuer) Issue(ctx context.Context, actorUID string, grants []actorscope.Grant, force bool) (credential.Token, error) {
	if len(grants) == 0 {
		// Checked before the cache: an empty set is covered by any cached
		// token, but an actor with no grants must get nothing.
		return credential.Token{}, status.Error(codes.Internal, "actor has no storage grants")
	}
	rules, err := gcsboundary.FromGrantsWithActorPrefixLen(grants, i.actorPrefixLen)
	if err != nil {
		return credential.Token{}, status.Errorf(codes.Internal, "while building the access boundary: %v", err)
	}
	_, isRO := roGrantsKey(grants)
	shareByRules := isRO || i.actorPrefixLen > 0
	rKey := rulesCacheKey(rules)

	if !force {
		i.mu.Lock()
		if v, ok := i.cache.Get(actorUID); ok {
			if e := v.(entry); covers(e.grants, grants) && e.token.Expiry.Sub(i.now()) >= minServeLifetime {
				i.mu.Unlock()
				return e.token, nil
			}
		}
		if shareByRules && i.roCache != nil {
			if v, ok := i.roCache.Get(rKey); ok {
				if e := v.(entry); e.token.Expiry.Sub(i.now()) >= minServeLifetime {
					i.mu.Unlock()
					return e.token, nil
				}
			}
			if call, ok := i.inflight[rKey]; ok {
				i.mu.Unlock()
				select {
				case <-call.done:
					if call.err == nil && call.tok.Expiry.Sub(i.now()) >= minServeLifetime {
						i.mu.Lock()
						i.cache.Add(actorUID, entry{token: call.tok, grants: slices.Clone(grants)})
						i.mu.Unlock()
					}
					return call.tok, call.err
				case <-ctx.Done():
					return credential.Token{}, ctx.Err()
				}
			}
			call := &inflightMint{done: make(chan struct{})}
			i.inflight[rKey] = call
			i.mu.Unlock()
			tok, err := i.mintRules(ctx, rules)
			call.tok, call.err = tok, err
			i.mu.Lock()
			delete(i.inflight, rKey)
			if err == nil && tok.Expiry.Sub(i.now()) >= minServeLifetime {
				ent := entry{token: tok, grants: slices.Clone(grants)}
				i.cache.Add(actorUID, ent)
				i.roCache.Add(rKey, ent)
			}
			close(call.done)
			i.mu.Unlock()
			return tok, err
		}
		i.mu.Unlock()
	}
	tok, err := i.mintRules(ctx, rules)
	if err != nil {
		return credential.Token{}, err
	}
	if tok.Expiry.Sub(i.now()) >= minServeLifetime {
		ent := entry{token: tok, grants: slices.Clone(grants)}
		i.mu.Lock()
		i.cache.Add(actorUID, ent)
		if shareByRules && i.roCache != nil {
			i.roCache.Add(rKey, ent)
		}
		i.mu.Unlock()
	}
	return tok, nil
}

// rulesCacheKey returns a canonical cache key for a slice of access boundary
// rules so requests producing identical rules share the same cached token.
func rulesCacheKey(rules []downscope.AccessBoundaryRule) string {
	parts := make([]string, len(rules))
	for idx, r := range rules {
		var expr string
		if r.Condition != nil {
			expr = r.Condition.Expression
		}
		parts[idx] = r.AvailableResource + "\x00" + strings.Join(r.AvailablePermissions, ",") + "\x00" + expr
	}
	slices.Sort(parts)
	return strings.Join(parts, "\x01")
}

// roGrantsKey returns a canonical cache key and true when every grant in
// grants is ReadOnly, or ("", false) when any grant permits writes.
func roGrantsKey(grants []actorscope.Grant) (string, bool) {
	for _, g := range grants {
		if g.Access != actorscope.ReadOnly {
			return "", false
		}
	}
	parts := make([]string, len(grants))
	for idx, g := range grants {
		parts[idx] = g.Prefix.String()
	}
	slices.Sort(parts)
	return strings.Join(parts, "\x00"), true
}

// covers reports whether a token minted with the cached grants can serve a
// request for the requested grants: each requested prefix must appear in the
// cached grants with the same or higher access. Prefixes are compared as
// written, since both sides build them with the same helper.
func covers(cached, requested []actorscope.Grant) bool {
	for _, r := range requested {
		if !slices.ContainsFunc(cached, func(c actorscope.Grant) bool {
			return c.Prefix == r.Prefix && c.Access >= r.Access
		}) {
			return false
		}
	}
	return true
}

// mintRules exchanges the root token at STS using the pre-validated access
// boundary rules.
func (i *Issuer) mintRules(ctx context.Context, rules []downscope.AccessBoundaryRule) (credential.Token, error) {
	// The downscope package reads its HTTP client from the context but sends
	// the STS request without one, so the caller's deadline is attached by the
	// transport instead.
	ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{
		Transport: contextTransport{ctx: ctx, base: i.client.Transport},
		Timeout:   i.client.Timeout,
	})
	ts, err := downscope.NewTokenSource(ctx, downscope.DownscopingConfig{RootSource: i.source, Rules: rules})
	if err != nil {
		return credential.Token{}, status.Errorf(codes.Internal, "while building the access boundary: %v", err)
	}
	tok, err := ts.Token()
	if err != nil {
		return credential.Token{}, fmt.Errorf("while exchanging at STS: %w", err)
	}
	if tok.AccessToken == "" {
		return credential.Token{}, errors.New("STS returned no access token")
	}
	if tok.Expiry.IsZero() {
		// Without an expiry the node plugin could not know when to stop using
		// the token.
		return credential.Token{}, errors.New("downscoped token has no expiry")
	}
	return credential.Token{AccessToken: tok.AccessToken, Expiry: tok.Expiry}, nil
}

// contextTransport sends every request with ctx, so a cancelled or expired
// caller stops an STS exchange in flight instead of waiting out the timeout.
type contextTransport struct {
	ctx  context.Context
	base http.RoundTripper
}

func (t contextTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(req.WithContext(t.ctx))
}
