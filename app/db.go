package main

import (
	"sync"
	"time"
)

type DB struct {
	config     Config
	db         map[string]string
	expiration map[string]time.Time
	lock       sync.Mutex
	aof        *AOF
}

func NewDB(conf Config) *DB {
	return &DB{
		config:     conf,
		db:         map[string]string{},
		expiration: map[string]time.Time{},
		lock:       sync.Mutex{},
		aof:        NewAOF(conf),
	}
}
