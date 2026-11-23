package session

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

var (
	ErrInvalidClientID = errors.New("session: invalid client ID")
	ErrInvalidRole     = errors.New("session: invalid role")
	ErrNilAddress      = errors.New("session: UDP address is nil")
)

const (
	RoleConsumer  uint8 = 1
	RolePublisher uint8 = 2
	RoleBoth      uint8 = 3
)

// Session describes one broker-visible client session.
type Session struct {
	ID        string
	ClientID  string
	Role      uint8
	Address   *net.UDPAddr
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Manager keeps all sessions for one broker process.
type Manager struct {
	mu       sync.RWMutex
	nextID   uint64
	byID     map[string]Session
	byClient map[string]string
	byAddr   map[string]string
}

func NewManager() *Manager {
	return &Manager{
		byID:     make(map[string]Session),
		byClient: make(map[string]string),
		byAddr:   make(map[string]string),
	}
}

// Upsert creates or refreshes a session for clientID.
func (m *Manager) Upsert(clientID string, role uint8, address *net.UDPAddr, now time.Time) (Session, error) {
	if clientID == "" {
		return Session{}, ErrInvalidClientID
	}
	if !validRole(role) {
		return Session{}, fmt.Errorf("%w: %d", ErrInvalidRole, role)
	}
	if address == nil {
		return Session{}, ErrNilAddress
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if sessionID, exists := m.byClient[clientID]; exists {
		session := m.byID[sessionID]
		delete(m.byAddr, session.Address.String())
		session.Role = role
		session.Address = cloneAddr(address)
		session.UpdatedAt = now
		m.byID[session.ID] = session
		m.byAddr[session.Address.String()] = session.ID
		return copySession(session), nil
	}

	m.nextID++
	session := Session{
		ID:        strconv.FormatUint(m.nextID, 10),
		ClientID:  clientID,
		Role:      role,
		Address:   cloneAddr(address),
		CreatedAt: now,
		UpdatedAt: now,
	}
	m.byID[session.ID] = session
	m.byClient[clientID] = session.ID
	m.byAddr[session.Address.String()] = session.ID
	return copySession(session), nil
}

func (m *Manager) GetByID(id string) (Session, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	session, exists := m.byID[id]
	if !exists {
		return Session{}, false
	}
	return copySession(session), true
}

// Snapshot returns a copy of all broker-local sessions for control replication.
func (m *Manager) Snapshot() []Session {
	if m == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	sessions := make([]Session, 0, len(m.byID))
	for _, session := range m.byID {
		sessions = append(sessions, copySession(session))
	}
	return sessions
}

func (m *Manager) GetByAddress(address *net.UDPAddr) (Session, bool) {
	if address == nil {
		return Session{}, false
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	sessionID, exists := m.byAddr[address.String()]
	if !exists {
		return Session{}, false
	}
	session := m.byID[sessionID]
	return copySession(session), true
}

// Restore replaces session state from a replicated control snapshot.
func (m *Manager) Restore(sessions []Session) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.byID = make(map[string]Session)
	m.byClient = make(map[string]string)
	m.byAddr = make(map[string]string)
	for _, session := range sessions {
		if session.ID == "" || session.ClientID == "" {
			continue
		}
		copy := copySession(session)
		m.byID[copy.ID] = copy
		m.byClient[copy.ClientID] = copy.ID
		if copy.Address != nil {
			m.byAddr[copy.Address.String()] = copy.ID
		}
	}
}

func validRole(role uint8) bool {
	return role == RoleConsumer || role == RolePublisher || role == RoleBoth
}

func cloneAddr(address *net.UDPAddr) *net.UDPAddr {
	if address == nil {
		return nil
	}
	copy := *address
	copy.IP = append(net.IP(nil), address.IP...)
	return &copy
}

func copySession(session Session) Session {
	session.Address = cloneAddr(session.Address)
	return session
}
