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

package boomerutil

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"

	"go.ate.dev/substrate/internal/ateapiauth"
	"go.ate.dev/substrate/internal/credbundle"
	_ "go.ate.dev/substrate/internal/k8sresolver"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	// ateapiCAFile verifies ateapi's servicedns serving cert;
	// ateapiCredBundle is the benchmark's own pod certificate. Both are
	// projected in by benchmarking/locust/manifests/locust.yaml.
	ateapiCAFile     = "/run/servicedns-ca/ca.crt"
	ateapiCredBundle = "/run/podidentity.podcert.ate.dev/credential-bundle.pem"

	// ateapiServerName is the DNS SAN on the apiserver's serving cert, which
	// is shorter than the endpoint the benchmark dials.
	ateapiServerName = "api.ate-system.svc"

	// routerServerName is the DNS SAN on atenet-router's servicedns serving cert.
	routerServerName = "atenet-router.ate-system.svc"
)

// DialControl opens an authenticated gRPC connection to ateapi.
//
// ateapi rejects calls that carry no credential, so present the pod
// certificate — the same client auth the base install gives ate-controller
// and atenet-router. ClientCredBundle re-reads the bundle on every handshake,
// so rotations are picked up without a restart.
func DialControl(endpoint string) (*grpc.ClientConn, ateapipb.ControlClient, error) {
	conns, stubs, err := DialControlPool(endpoint, 1)
	if err != nil {
		return nil, nil, err
	}
	return conns[0], stubs[0], nil
}

// DialControlPool opens a pool of authenticated gRPC connections to ateapi so
// high-throughput load generators can spread HTTP/2 streams across multiple TCP
// connections per apiserver replica.
func DialControlPool(endpoint string, size int) ([]*grpc.ClientConn, []ateapipb.ControlClient, error) {
	if size < 1 {
		size = 1
	}
	k8sConfig, err := rest.InClusterConfig()
	if err != nil {
		loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
		configOverrides := &clientcmd.ConfigOverrides{}
		k8sConfig, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
		if err != nil {
			return nil, nil, fmt.Errorf("k8s config: %w", err)
		}
	}
	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("k8s client: %w", err)
	}

	dialOpts, err := ateapiauth.DialOptions(ateapiauth.ClientConfig{
		K8sClient:        k8sClient,
		CAFile:           ateapiCAFile,
		ClientCredBundle: ateapiCredBundle,
		ServerName:       ateapiServerName,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("ateapi dial options: %w", err)
	}
	dialOpts = append(dialOpts, grpc.WithStatsHandler(otelgrpc.NewClientHandler()))

	conns := make([]*grpc.ClientConn, 0, size)
	stubs := make([]ateapipb.ControlClient, 0, size)
	for i := 0; i < size; i++ {
		conn, err := grpc.NewClient(endpoint, dialOpts...)
		if err != nil {
			for _, c := range conns {
				_ = c.Close()
			}
			return nil, nil, fmt.Errorf("dial %s: %w", endpoint, err)
		}
		conns = append(conns, conn)
		stubs = append(stubs, ateapipb.NewControlClient(conn))
	}
	return conns, stubs, nil
}

// RouterTLSConfig returns a mutual-TLS configuration for calling atenet-router's
// HTTPS ingress listener using the projected servicedns CA bundle and podidentity
// client credential bundle. It forces HTTP/1.1 in NextProtos so Go's HTTP transport
// opens a full pool of TCP connections across all router pods behind the ClusterIP
// instead of multiplexing every VU over a single HTTP/2 connection.
func RouterTLSConfig() (*tls.Config, error) {
	loadRootCAs := credbundle.PoolLoader(ateapiCAFile)
	if _, err := loadRootCAs(); err != nil {
		return nil, fmt.Errorf("loading router CA file: %w", err)
	}
	return &tls.Config{
		MinVersion:         tls.VersionTLS13,
		NextProtos:         []string{"http/1.1"},
		ClientSessionCache: tls.NewLRUClientSessionCache(1024),
		InsecureSkipVerify: true, //nolint:gosec
		VerifyConnection: func(cs tls.ConnectionState) error {
			pool, err := loadRootCAs()
			if err != nil {
				return err
			}
			if len(cs.PeerCertificates) == 0 {
				return errors.New("router presented no certificate")
			}
			inter := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				inter.AddCert(c)
			}
			_, err = cs.PeerCertificates[0].Verify(x509.VerifyOptions{
				Roots:         pool,
				Intermediates: inter,
				DNSName:       routerServerName,
			})
			return err
		},
		GetClientCertificate: credbundle.ClientLoader(ateapiCredBundle),
	}, nil
}
