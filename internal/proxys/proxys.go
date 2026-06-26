package proxys

import (
	"math/rand"
	"sync"
)

type IProxy struct {
	lock sync.RWMutex
	ips  []string
}

func NewIProxyIP(ips []string) IProxy {
	return IProxy{
		ips: ips,
	}
}

func (p *IProxy) GetIPS() int {
	if p == nil {
		return 0
	}
	p.lock.RLock()
	defer p.lock.RUnlock()
	return len(p.ips)
}

func (p *IProxy) SetIPS(ips []string) {
	if p == nil {
		return
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	p.ips = ips
}

func (p *IProxy) RemoveIP(ip string) bool {
	if p == nil || ip == "" {
		return false
	}
	p.lock.Lock()
	defer p.lock.Unlock()
	for i, candidate := range p.ips {
		if candidate == ip {
			p.ips = append(p.ips[:i], p.ips[i+1:]...)
			return true
		}
	}
	return false
}

func (p *IProxy) GetProxyIP() string {
	if p == nil {
		return ""
	}
	p.lock.RLock()
	defer p.lock.RUnlock()
	if len(p.ips) == 0 {
		return ""
	}
	return p.ips[rand.Intn(len(p.ips))]
}
