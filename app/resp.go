package main

import (
	"bufio"
	"fmt"
	"io"
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

const (
	SimpleStrings = '+'
	SimpleErrors  = '-'
	Integers      = ':'
	BulkStrings   = '$'
	Arrays        = '*'
	Nulls         = '_'
)

type Value struct {
	Type  rune
	Data  string
	Array []Value
}

func (v Value) Marshal() []byte {
	switch v.Type {
	case SimpleStrings:
		return v.marshalSimpleStrings()
	case SimpleErrors:
		return v.marshalSimpleErrors()
	case Integers:
		return v.marshalIntegers()
	case BulkStrings:
		return v.marshalBulkStrings()
	case Arrays:
		return v.marshalArrays()
	case Nulls:
		return v.marshalNulls()
	}
	return nil
}

func (v Value) marshalSimpleStrings() []byte {
	var bytes []byte
	bytes = append(bytes, SimpleStrings)
	bytes = append(bytes, v.Data...)
	return append(bytes, CRLF...)
}

func (v Value) marshalSimpleErrors() []byte {
	var bytes []byte
	bytes = append(bytes, SimpleErrors)
	bytes = append(bytes, v.Data...)
	return append(bytes, CRLF...)
}

func (v Value) marshalIntegers() []byte {
	var bytes []byte
	bytes = append(bytes, Integers)
	bytes = append(bytes, v.Data...)
	return append(bytes, CRLF...)
}

func (v Value) marshalBulkStrings() []byte {
	var bytes []byte
	bytes = append(bytes, BulkStrings)
	if len(v.Data) == 2 && v.Data == "-1" {
		bytes = append(bytes, v.Data...)
		return append(bytes, CRLF...)
	}
	size := strconv.FormatInt(int64(len(v.Data)), 10)
	bytes = append(bytes, size...)
	bytes = append(bytes, CRLF...)
	bytes = append(bytes, v.Data...)
	return append(bytes, CRLF...)
}

func (v Value) marshalArrays() []byte {
	var bytes []byte
	bytes = append(bytes, Arrays)
	size := len(v.Array)
	bytes = append(bytes, strconv.Itoa(size)...)
	bytes = append(bytes, CRLF...)
	for i := 0; i < size; i++ {
		bytes = append(bytes, v.Array[i].Marshal()...)
	}
	return bytes
}

func (v Value) marshalNulls() []byte {
	var bytes []byte
	bytes = append(bytes, Nulls)
	return append(bytes, CRLF...)
}

type Resp struct {
	reader *bufio.Reader
}

func NewResp(r io.Reader) *Resp {
	return &Resp{
		reader: bufio.NewReader(r),
	}
}

func (r *Resp) Read() (Value, error) {
	typ, err := r.reader.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch typ {
	case SimpleStrings:
		return r.readSimpleStrings()
	case BulkStrings:
		return r.readBulkStrings()
	case Arrays:
		return r.readArrays()
	default:
		fmt.Printf("received unknown type: %v\n", string(typ))
		return Value{}, nil
	}
}

func (r *Resp) readSimpleStrings() (Value, error) {
	v := Value{}
	v.Type = SimpleStrings
	line, _, err := r.readLine()
	if err != nil {
		return Value{}, err
	}
	v.Data = string(line)
	return v, nil
}

func (r *Resp) readArrays() (Value, error) {
	v := Value{}
	v.Type = Arrays
	len, _, err := r.readInteger()
	if err != nil {
		return Value{}, err
	}
	v.Array = make([]Value, 0)
	for i := 0; i < len; i++ {
		val, err := r.Read()
		if err != nil {
			return Value{}, err
		}
		v.Array = append(v.Array, val)
	}
	return v, nil
}

func (r *Resp) readBulkStrings() (Value, error) {
	v := Value{}
	v.Type = BulkStrings
	len, _, err := r.readInteger()
	if err != nil {
		return Value{}, err
	}
	bulk := make([]byte, len)
	_, err = r.reader.Read(bulk)
	if err != nil {
		return Value{}, err
	}
	v.Data = string(bulk)
	r.reader.ReadLine()
	return v, nil
}

func (r *Resp) readInteger() (val int, n int, err error) {
	line, n, err := r.readLine()
	if err != nil {
		return 0, 0, err
	}
	i64, err := strconv.ParseInt(string(line), 10, 64)
	if err != nil {
		return 0, n, err
	}
	return int(i64), n, nil
}

func (r *Resp) readLine() (line []byte, n int, err error) {
	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return nil, 0, err
		}
		n++
		line = append(line, b)
		if len(line) >= 2 && line[len(line)-2] == CR && line[len(line)-1] == LF {
			break
		}
	}
	return line[:len(line)-2], n, nil
}

type Writer struct {
	writer io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{writer: w}
}

func (w *Writer) Write(v Value) error {
	var bytes = v.Marshal()

	_, err := w.writer.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}
