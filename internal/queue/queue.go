package queue

import "time"

type LeaseConfig struct {
	TTL              time.Duration
	HeartbeatEvery   time.Duration
	RecoveryInterval time.Duration
}

func (c LeaseConfig) Normalize() LeaseConfig {
	if c.TTL <= 0 {
		c.TTL = 10 * time.Second
	}
	if c.HeartbeatEvery <= 0 {
		c.HeartbeatEvery = c.TTL / 3
	}
	if c.RecoveryInterval <= 0 {
		c.RecoveryInterval = 500 * time.Millisecond
	}
	return c
}
