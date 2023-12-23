package main

import (
	"strconv"
)

/*
  RESP Protocol description - https://redis.io/docs/reference/protocol-spec/#resp-protocol-description
*/

const (
	CR   = '\r'
	LF   = '\n'
	CRLF = "\r\n"
)

type Type byte

const (
	SimpleStrings = '+'
	SimpleErrors  = '-'
	Integers      = ':'
	BulkStrings   = '$'
	Arrays        = '*'
)

type RESP struct {
	Type  Type
	Raw   []byte
	Data  []byte
	Count int
}

func (r RESP) Bytes() []byte {
	return r.Data
}

func (r RESP) String() string {
	return string(r.Data)
}

func (r RESP) Int() int64 {
	x, _ := strconv.ParseInt(r.String(), 10, 64)
	return x
}

func (r RESP) Float() float64 {
	x, _ := strconv.ParseFloat(r.String(), 10)
	return x
}

func (r RESP) Exists() bool {
	return r.Type != 0
}

func ReadNextRESP(b []byte) (int, RESP) {
	if len(b) == 0 {
		return 0, RESP{}
	}
	resp := RESP{}
	resp.Type = Type(b[0])
	switch resp.Type {
	case SimpleStrings, SimpleErrors, Integers, BulkStrings, Arrays:
	default:
		return 0, RESP{}
	}
	// read to end of line
	i := 1
	for ; ; i++ {
		if i == len(b) {
			return 0, RESP{}
		}
		if b[i] == LF {
			if b[i] != CR {
				return 0, RESP{}
			}
			i++
			break
		}
	}
	resp.Raw = b[0:i]
	resp.Data = b[1 : i-2]
	if resp.Type == Integers {
		// Integer
		if len(resp.Data) == 0 {
			return 0, RESP{} //, invalid integer
		}
		var j int
		if resp.Data[0] == '-' {
			if len(resp.Data) == 1 {
				return 0, RESP{} //, invalid integer
			}
			j++
		}
		for ; j < len(resp.Data); j++ {
			if resp.Data[j] < '0' || resp.Data[j] > '9' {
				return 0, RESP{} // invalid integer
			}
		}
		return len(resp.Raw), resp
	}
	if resp.Type == SimpleStrings || resp.Type == SimpleErrors {
		// String, Error
		return len(resp.Raw), resp
	}
	var err error
	resp.Count, err = strconv.Atoi(string(resp.Data))
	if err != nil {
		return 0, RESP{}
	}
	var tn int
	sdata := b[i:]
	for j := 0; j < resp.Count; j++ {
		rn, rresp := ReadNextRESP(sdata)
		if rresp.Type == 0 {
			return 0, RESP{}
		}
		tn += rn
		sdata = sdata[rn:]
	}
	resp.Data = b[i : i+tn]
	resp.Raw = b[0 : i+tn]
	return len(resp.Raw), resp
}
