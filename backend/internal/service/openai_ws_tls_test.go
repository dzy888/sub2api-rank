package service

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	coderws "github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
	"github.com/stretchr/testify/require"
)

func TestCoderOpenAIWSClientDialer_TLSResumption(t *testing.T) {
	for _, tc := range []struct {
		name     string
		version  uint16
		disabled bool
	}{
		{"TLS12", tls.VersionTLS12, false},
		{"TLS13", tls.VersionTLS13, false},
		{"server_without_tickets", tls.VersionTLS13, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			type handshake struct {
				Resumed bool
				Peer    string
			}
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Authorization") != "Bearer synthetic-valid" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				conn, err := coderws.Accept(w, r, nil)
				if err != nil {
					t.Errorf("accept websocket: %v", err)
					return
				}
				defer func() { _ = conn.CloseNow() }()
				if err := wsjson.Write(ctx, conn, handshake{r.TLS.DidResume, r.RemoteAddr}); err != nil {
					t.Errorf("write handshake result: %v", err)
					return
				}
				// Consume the client's close frame and finish the close handshake.
				_, _, _ = conn.Read(ctx)
			}))
			server.TLS = &tls.Config{MinVersion: tc.version, MaxVersion: tc.version, SessionTicketsDisabled: tc.disabled}
			server.Config.ErrorLog = log.New(io.Discard, "", 0)
			server.StartTLS()
			defer server.Close()
			wsURL := "wss" + strings.TrimPrefix(server.URL, "https")
			headers := http.Header{"Authorization": {"Bearer synthetic-valid"}}
			dialer := newDefaultOpenAIWSClientDialer().(*coderOpenAIWSClientDialer)
			defaultTransport := dialer.defaultHTTPClient.Transport.(*http.Transport)
			require.NotSame(t, http.DefaultTransport, defaultTransport, "must not mutate the process-wide transport")

			// Certificate verification must still reject untrusted peers.
			_, _, _, err := dialer.Dial(ctx, wsURL, headers, "")
			var unknownAuthority x509.UnknownAuthorityError
			require.ErrorAs(t, err, &unknownAuthority)
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			// The failed transport has already been used; replace it with a clone
			// before adding this test server's CA rather than mutating it in use.
			trustedTransport := defaultTransport.Clone()
			trustedTransport.TLSClientConfig.RootCAs = roots
			dialer.defaultHTTPClient.Transport = trustedTransport
			defer trustedTransport.CloseIdleConnections()

			proxyA := newWSTLSConnectProxy(t, server.Listener.Addr().String())
			proxyB := newWSTLSConnectProxy(t, server.Listener.Addr().String())
			for _, proxyURL := range []string{proxyA.URL, proxyB.URL} {
				client, err := dialer.proxyHTTPClient(proxyURL)
				require.NoError(t, err)
				transport := client.Transport.(*http.Transport)
				transport.TLSClientConfig.RootCAs = roots
				defer transport.CloseIdleConnections()
			}

			seenExits := map[string]bool{}
			seenPeers := map[string]bool{}
			for _, proxyURL := range []string{"", "", proxyA.URL, proxyA.URL, proxyB.URL, proxyB.URL, proxyA.URL, ""} {
				conn, _, _, err := dialer.Dial(ctx, wsURL, headers, proxyURL)
				require.NoError(t, err)
				defer func() { _ = conn.(openAIWSForceCloser).CloseNow() }()
				payload, err := conn.ReadMessage(ctx)
				require.NoError(t, err)
				var got handshake
				require.NoError(t, json.Unmarshal(payload, &got))
				require.Equal(t, seenExits[proxyURL] && !tc.disabled, got.Resumed,
					"only subsequent connections through the same exit may resume")
				require.False(t, seenPeers[got.Peer], "each dial must open a new socket, not reuse a WS session")
				seenExits[proxyURL] = true
				seenPeers[got.Peer] = true
				require.NoError(t, conn.Close())
			}

			// A TLS ticket is not OAuth authorization: a warm cache must not let
			// a different/revoked credential inherit the prior WS authorization.
			for _, proxyURL := range []string{"", proxyA.URL} {
				conn, status, _, err := dialer.Dial(ctx, wsURL, http.Header{"Authorization": {"Bearer synthetic-revoked"}}, proxyURL)
				require.Error(t, err)
				require.Nil(t, conn)
				require.Equal(t, http.StatusUnauthorized, status)
			}
		})
	}
}

// A CONNECT tunnel keeps TLS end-to-end so the upstream can report DidResume.
// Only the test server is reachable through this proxy.
func newWSTLSConnectProxy(t *testing.T, target string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || r.Host != target {
			http.Error(w, "unexpected proxy target", http.StatusBadRequest)
			return
		}
		upstream, err := (&net.Dialer{Timeout: time.Second}).DialContext(r.Context(), "tcp", target)
		if err != nil {
			http.Error(w, "test upstream unavailable", http.StatusBadGateway)
			return
		}
		defer upstream.Close()
		client, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack CONNECT: %v", err)
			return
		}
		defer client.Close()
		_ = client.SetDeadline(time.Now().Add(15 * time.Second))
		_ = upstream.SetDeadline(time.Now().Add(15 * time.Second))
		if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		if err := buffered.Flush(); err != nil {
			return
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = io.Copy(upstream, buffered)
			_ = upstream.Close()
		}()
		_, _ = io.Copy(client, upstream)
		_ = client.Close()
		<-done
	}))
	t.Cleanup(server.Close)
	return server
}
