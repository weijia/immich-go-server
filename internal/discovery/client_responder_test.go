package discovery

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"
)

func runResponderTest(t *testing.T, req string, check func(*testing.T, string)) {
	t.Helper()
	id := ClientIdentity{ServerID: "sid-1", ServerName: "test", ServerToken: "tok", Version: "3.0.0"}
	r, err := NewClientResponder("127.0.0.1:0", id, "http://127.0.0.1:9999")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = r.Run(ctx) }()

	addr := r.Addr().String()
	ua, _ := net.ResolveUDPAddr("udp", addr)
	conn, err := net.DialUDP("udp", nil, ua)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1500)
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	check(t, string(buf[:n]))
}

func TestClientResponderV1(t *testing.T) {
	runResponderTest(t, "DISCOVER_IMMICH_SERVER", func(t *testing.T, body string) {
		if !strings.HasPrefix(body, "IMMICH_SERVER_RESPONSE:") {
			t.Fatalf("bad prefix: %q", body)
		}
		if !strings.Contains(body, `"serverId"`) {
			t.Fatalf("missing serverId: %q", body)
		}
		if !strings.Contains(body, `/api"`) {
			t.Fatalf("serverUrl must end with /api: %q", body)
		}
	})
}

func TestClientResponderV3(t *testing.T) {
	nonce := "nonce-xyz789"
	runResponderTest(t, "DISCOVER_IMMICH_SERVER:client-1:"+nonce, func(t *testing.T, body string) {
		if !strings.Contains(body, `"challengeNonce":"`+nonce+`"`) {
			t.Fatalf("nonce mismatch: %q", body)
		}
		if !strings.Contains(body, `"signature"`) {
			t.Fatalf("missing signature: %q", body)
		}
		if !strings.Contains(body, `/api"`) {
			t.Fatalf("serverUrl must end with /api: %q", body)
		}
	})
}

func TestGenerateServerIdentityStable(t *testing.T) {
	mem := map[string]string{}
	get := func(k string) (string, bool, error) { v, ok := mem[k]; return v, ok, nil }
	set := func(k, v string) error { mem[k] = v; return nil }
	id1, err := GenerateServerIdentity(get, set, "node")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := GenerateServerIdentity(get, set, "node")
	if err != nil {
		t.Fatal(err)
	}
	if id1.ServerID != id2.ServerID || id1.ServerToken != id2.ServerToken || id1.ServerName != id2.ServerName {
		t.Fatalf("identity not stable: %+v vs %+v", id1, id2)
	}
	if id1.ServerID == "" || id1.ServerToken == "" {
		t.Fatalf("empty identity: %+v", id1)
	}
}
