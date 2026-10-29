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

// Package credential holds the credential type the exchange hands out and
// its issuers produce. It is a leaf so an issuer does not depend on the
// handler that serves it.
package credential

import "time"

// Token is a storage credential and when it expires.
type Token struct {
	AccessToken string
	Expiry      time.Time
}

// MinServeLifetime is the least remaining lifetime a cached Token is served
// with, on both sides of the exchange, so a transfer never starts on a token
// about to expire. It is set to 10s (well below the GKE/GCE metadata server's
// early-refresh window and the 60s EarlyTokenRefresh buffer on the root token
// source) so that when cached downscoped tokens reach MinServeLifetime, the
// underlying root token has already rotated to a fresh 1-hour expiry.
const MinServeLifetime = 10 * time.Second
