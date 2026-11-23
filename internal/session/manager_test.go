package session

import (
	"errors"
	"net"
	"testing"
	"time"
)

func TestUpsertCreatesAndUpdatesSessionForSameClient(t *testing.T) {
	manager := NewManager()
	now := time.Unix(100, 0).UTC()
	first, err := manager.Upsert("client-1", RoleConsumer, udpAddr(t, "127.0.0.1", 9001), now)
	if err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if first.ID == "" {
		t.Fatal("session ID must not be empty")
	}

	updated, err := manager.Upsert("client-1", RoleBoth, udpAddr(t, "127.0.0.1", 9002), now.Add(time.Second))
	if err != nil {
		t.Fatalf("Upsert(update) error = %v", err)
	}
	if updated.ID != first.ID {
		t.Fatalf("session ID changed from %q to %q", first.ID, updated.ID)
	}
	if updated.Role != RoleBoth {
		t.Fatalf("updated role = %d", updated.Role)
	}
	if updated.Address.Port != 9002 {
		t.Fatalf("updated address = %v", updated.Address)
	}

	byAddress, ok := manager.GetByAddress(udpAddr(t, "127.0.0.1", 9002))
	if !ok || byAddress.ID != first.ID {
		t.Fatalf("GetByAddress() = %#v, %t", byAddress, ok)
	}
}

func TestUpsertValidation(t *testing.T) {
	manager := NewManager()
	_, err := manager.Upsert("", RoleConsumer, udpAddr(t, "127.0.0.1", 9001), time.Time{})
	if !errors.Is(err, ErrInvalidClientID) {
		t.Fatalf("empty client ID error = %v", err)
	}
	_, err = manager.Upsert("client", 0, udpAddr(t, "127.0.0.1", 9001), time.Time{})
	if !errors.Is(err, ErrInvalidRole) {
		t.Fatalf("invalid role error = %v", err)
	}
	_, err = manager.Upsert("client", RoleConsumer, nil, time.Time{})
	if !errors.Is(err, ErrNilAddress) {
		t.Fatalf("nil address error = %v", err)
	}
}

func TestSnapshotReturnsCopies(t *testing.T) {
	manager := NewManager()
	_, err := manager.Upsert("client-1", RoleConsumer, udpAddr(t, "127.0.0.1", 9001), time.Unix(100, 0))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := manager.Snapshot()
	if len(snapshot) != 1 || snapshot[0].ClientID != "client-1" {
		t.Fatalf("snapshot = %+v", snapshot)
	}
	snapshot[0].Address.Port = 9999
	again, _ := manager.GetByAddress(udpAddr(t, "127.0.0.1", 9001))
	if again.Address.Port != 9001 {
		t.Fatalf("snapshot leaked address: %+v", again.Address)
	}
}

func TestRestoreReplacesSessionState(t *testing.T) {
	manager := NewManager()
	_, _ = manager.Upsert("old", RoleConsumer, udpAddr(t, "127.0.0.1", 9001), time.Unix(100, 0))
	address := udpAddr(t, "127.0.0.1", 9002)
	manager.Restore([]Session{{ID: "session-2", ClientID: "new", Role: RoleBoth, Address: address}})
	if _, exists := manager.GetByAddress(address); !exists {
		t.Fatal("restored session address not found")
	}
	if _, exists := manager.GetByID("1"); exists {
		t.Fatal("old session survived restore")
	}
}

func udpAddr(t *testing.T, ip string, port int) *net.UDPAddr {
	t.Helper()
	parsed := net.ParseIP(ip)
	if parsed == nil {
		t.Fatalf("invalid IP: %q", ip)
	}
	return &net.UDPAddr{IP: parsed, Port: port}
}
