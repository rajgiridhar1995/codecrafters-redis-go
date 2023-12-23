package main

import "sync"

var db = map[string]string{}
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
	val, ok := db[key]
	lock.Unlock()
	if !ok {
		return Value{
			Type: Nulls,
		}
	}
	return Value{
		Type: BulkStrings,
		Data: val,
	}
}

func commandSet(args []Value) Value {
	if len(args) != 2 {
		return Value{
			Type: SimpleErrors,
			Data: "ERR wrong number of arguments for 'set' command",
		}
	}
	key := args[0].Data.(string)
	val := args[1].Data.(string)

	lock.Lock()
	db[key] = val
	lock.Unlock()
	return Value{
		Type: SimpleStrings,
		Data: "OK",
	}
}
