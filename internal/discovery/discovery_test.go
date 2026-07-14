package discovery

import (
	"encoding/json"
	"net"
	"testing"
	"time"
)

func TestSignVerifyBeacon(t *testing.T) {
	secret := "cluster-secret"
	now := int64(1700000000)
	b := Beacon{NodeID: "node-A", Addr: "10.0.0.1:8080", Timestamp: now, Nonce: "abc123"}
	b.Sig = SignBeacon(secret, b)

	if !VerifyBeacon(secret, b, now, 300) {
		t.Error("valid beacon should verify")
	}
	// 篡改 addr
	bad := b
	bad.Addr = "10.0.0.2:8080"
	if VerifyBeacon(secret, bad, now, 300) {
		t.Error("tampered beacon should fail")
	}
	// 时钟偏差超出
	if VerifyBeacon(secret, b, now+1000, 300) {
		t.Error("clock skew beyond maxSkew should fail")
	}
}

func TestBroadcasterSendAndListenLoopback(t *testing.T) {
	secret := "cluster-secret"
	// 监听端：随机端口 UDP
	laddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lc, err := net.ListenUDP("udp", laddr)
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	listenAddr := lc.LocalAddr().(*net.UDPAddr)
	dst := &net.UDPAddr{IP: listenAddr.IP, Port: listenAddr.Port}

	reg := NewRegistry()
	lis := NewListener(lc, secret, 300, reg)

	// 发送端：绑定任意端口
	sc, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close()
	bc := NewBroadcaster(sc, secret, "node-A", "127.0.0.1:8080")

	if err := bc.Send(dst); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// 接收
	got, err := lis.RecvOnce()
	if err != nil {
		t.Fatalf("RecvOnce: %v", err)
	}
	if !got {
		t.Fatal("beacon should be accepted")
	}
	peers := reg.Peers()
	if len(peers) != 1 || peers[0].NodeID != "node-A" || peers[0].Addr != "127.0.0.1:8080" {
		t.Fatalf("unexpected peers: %+v", peers)
	}
}

func TestListenerRejectsTamperedAndReplay(t *testing.T) {
	reg := NewRegistry()
	lis := NewListener(nil, "secret", 300, reg)
	now := int64(1700000000)
	lis.now = func() int64 { return now }

	good := Beacon{NodeID: "node-B", Addr: "1.2.3.4:9", Timestamp: now, Nonce: "n1"}
	good.Sig = SignBeacon("secret", good)
	raw, _ := json.Marshal(good)

	if ok, _ := lis.HandlePacket(raw); !ok {
		t.Fatal("valid beacon rejected")
	}
	// 重放同一包：nonce 已见，应拒
	if ok, _ := lis.HandlePacket(raw); ok {
		t.Error("replayed beacon should be rejected")
	}
	// 篡改后重算签名
	bad := good
	bad.Addr = "9.9.9.9:9"
	bad.Sig = SignBeacon("secret", bad)
	badRaw, _ := json.Marshal(bad)
	if ok, _ := lis.HandlePacket(badRaw); ok {
		t.Error("tampered beacon should be rejected")
	}
}

func TestRegistryPrune(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert("a", "1.1.1.1:1", 1000)
	reg.Upsert("b", "2.2.2.2:2", 2000)
	if n := reg.Prune(1500); n != 1 {
		t.Fatalf("expected prune 1, got %d", n)
	}
	peers := reg.Peers()
	if len(peers) != 1 || peers[0].NodeID != "b" {
		t.Fatalf("unexpected survivors: %+v", peers)
	}
}

// 验证 Run 循环能持续收包（用真实回环 + 短超时控制退出）。
func TestListenerRecvLoop(t *testing.T) {
	secret := "s"
	laddr, _ := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	lc, err := net.ListenUDP("udp", laddr)
	if err != nil {
		t.Fatal(err)
	}
	defer lc.Close()
	dst := lc.LocalAddr()
	reg := NewRegistry()
	lis := NewListener(lc, secret, 300, reg)
	lc.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

	sc, _ := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 0})
	defer sc.Close()
	bc := NewBroadcaster(sc, secret, "node-X", "127.0.0.1:1")

	go func() {
		for i := 0; i < 3; i++ {
			_ = bc.Send(dst)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	received := 0
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		lc.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		ok, err := lis.RecvOnce()
		if err != nil {
			break
		}
		if ok {
			received++
		}
	}
	if received < 1 {
		t.Fatalf("expected at least 1 beacon, got %d", received)
	}
	if len(reg.Peers()) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(reg.Peers()))
	}
}
