package main

import (
	"strconv"
	"strings"
	"sync"
	"time"
)

var db = map[string]string{}
var expiration = map[string]time.Time{}
var lock = sync.Mutex{}

var Commands = map[string]func([]Value) Value{
	"PING": commandPing,
	"ECHO": commandEcho,
	"GET":  commandGet,
	"SET":  commandSet,
}

func commandPing(args []Value) Value {
	if len(args) == 0 {
		return Value{
			Type: SimpleStrings,
			Data: "PONG",
		}
	}
	return Value{
		Type: SimpleStrings,
		Data: args[0].Data,
	}
}

func commandEcho(args []Value) Value {
	if len(args) != 1 {
		return Value{
			Type: SimpleErrors,
			Data: "ERR wrong number of arguments for 'echo' command",
		}
	}
	return Value{
		Type: SimpleStrings,
		Data: args[0].Data,
	}
}

func commandGet(args []Value) Value {
	if len(args) != 1 {
		return Value{
			Type: SimpleErrors,
			Data: "ERR wrong number of arguments for 'get' command",
		}
	}
	key := args[0].Data.(string)
	lock.Lock()
	defer lock.Unlock()
	val, ok := db[key]
	if !ok {
		return Value{
			Type: Nulls,
		}
	}
	if exp, ok := expiration[key]; ok && time.Now().After(exp) {
		delete(db, key)
		delete(expiration, key)
		return Value{
			Type: BulkStrings,
			Data: "-1",
		}
	}

	return Value{
		Type: BulkStrings,
		Data: val,
	}
}

func commandSet(args []Value) Value {
	if len(args) < 2 {
		return Value{
			Type: SimpleErrors,
			Data: "ERR wrong number of arguments for 'set' command",
		}
	}
	key := args[0].Data.(string)
	val := args[1].Data.(string)

	lock.Lock()
	db[key] = val
	defer lock.Unlock()
	if len(args) >= 4 && strings.ToUpper(args[2].Data.(string)) == "PX" {
		px, err := strconv.Atoi(args[3].Data.(string))
		if err != nil {
			return Value{
				Type: SimpleErrors,
				Data: "ERR received invalid value for px",
			}
		}
		expirationTime := time.Now().Add(time.Duration(px) * time.Millisecond)
		expiration[key] = expirationTime
	}
	return Value{
		Type: SimpleStrings,
		Data: "OK",
	}
}
