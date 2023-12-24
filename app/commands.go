package main

import (
	"strconv"
	"strings"
	"time"
)

var Commands = map[string]func(db *DB, val []Value) Value{
	"PING":   commandPing,
	"ECHO":   commandEcho,
	"GET":    commandGet,
	"SET":    commandSet,
	"CONFIG": commandConfig,
}

func commandPing(db *DB, args []Value) Value {
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

func commandEcho(db *DB, args []Value) Value {
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

func commandGet(db *DB, args []Value) Value {
	if len(args) != 1 {
		return Value{
			Type: SimpleErrors,
			Data: "ERR wrong number of arguments for 'get' command",
		}
	}
	key := args[0].Data.(string)
	db.lock.Lock()
	defer db.lock.Unlock()
	val, ok := db.db[key]
	if !ok {
		return Value{
			Type: Nulls,
		}
	}
	if exp, ok := db.expiration[key]; ok && time.Now().After(exp) {
		delete(db.db, key)
		delete(db.expiration, key)
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

func commandSet(db *DB, args []Value) Value {
	if len(args) < 2 {
		return Value{
			Type: SimpleErrors,
			Data: "ERR wrong number of arguments for 'set' command",
		}
	}
	key := args[0].Data.(string)
	val := args[1].Data.(string)

	db.lock.Lock()
	db.db[key] = val
	defer db.lock.Unlock()
	if len(args) >= 4 && strings.ToUpper(args[2].Data.(string)) == "PX" {
		px, err := strconv.Atoi(args[3].Data.(string))
		if err != nil {
			return Value{
				Type: SimpleErrors,
				Data: "ERR received invalid value for px",
			}
		}
		expirationTime := time.Now().Add(time.Duration(px) * time.Millisecond)
		db.expiration[key] = expirationTime
	}
	return Value{
		Type: SimpleStrings,
		Data: "OK",
	}
}

func commandConfig(db *DB, args []Value) Value {
	if len(args) != 2 {
		return Value{
			Type: SimpleErrors,
			Data: "ERR wrong number of arguments for 'config' command",
		}
	}
	v := Value{
		Type:  Arrays,
		Array: []Value{},
	}
	// TODO check for first args
	configKey := args[1].Data.(string)
	switch strings.ToUpper(configKey) {
	case "DIR":
		v.Array = append(v.Array, Value{
			Type: SimpleStrings,
			Data: "dir",
		})
		v.Array = append(v.Array, Value{
			Type: SimpleStrings,
			Data: db.config.AofDir,
		})
	case "DBFILENAME":
		v.Array = append(v.Array, Value{
			Type: SimpleStrings,
			Data: "dbfilename",
		})
		v.Array = append(v.Array, Value{
			Type: SimpleStrings,
			Data: db.config.AofFileName,
		})
	}
	return v
}
