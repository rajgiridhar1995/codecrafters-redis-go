package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

type KeyValue struct {
	Value     string
	HasExpiry bool
	Expires   time.Time
}

type DB struct {
	config Config
	db     map[string]KeyValue
	lock   sync.Mutex
	RDB    *RDB
}

func NewDB(conf Config) *DB {
	RDB := NewRDB(conf)

	return &DB{
		config: conf,
		db:     map[string]KeyValue{},
		lock:   sync.Mutex{},
		RDB:    RDB,
	}
}

func (db *DB) ReadRDB() error {
	fmt.Println("reading rdb file")
	if db.config.RDBDir == "" || db.config.RDBFileName == "" {
		fmt.Println("rdb file is empty")
		return nil
	}

	filePath := path.Join(db.config.RDBDir, db.config.RDBFileName)
	fd, err := os.Open(filePath)
	if err != nil {
		fmt.Println("failed to read RDB", err)
		return nil
	}

	// TODO: remove this >>>
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Println("failed to read RDB", err)
		os.Exit(1)
	}
	r := bufio.NewReader(f)
	b := make([]byte, 10000)
	n, err := r.Read(b)
	if err != nil {
		fmt.Println("failed to read RDB", err)
		return err
	}
	fmt.Printf("n: %d \nfile content: %v\n\n", n, b[:n])
	// TODO: <<< remove this

	if info, err := fd.Stat(); err == nil {
		fmt.Println("file size:", info.Size())
		if info.Size() == 0 {
			fmt.Println("rdb file is empty")
			return nil
		}
	} else {
		fmt.Printf("failed to get file stat, err: %v", err)
		return nil
	}

	reader := bufio.NewReader(fd)
	header := make([]byte, 5)
	_, err = reader.Read(header)
	if err != nil {
		fmt.Println("failed to read RDB", err)
		return err
	}
	if strings.ToUpper(string(header)) != MAGIC {
		fmt.Println("received invalid header", string(header))
		return fmt.Errorf("received invalid header")
	}
	fmt.Println(string(header))
	version := make([]byte, 4)
	_, err = reader.Read(version)
	if err != nil {
		fmt.Println("failed to read RDB", err)
		return err
	}
	fmt.Println("received version", version)

	for {
		opCode, err := reader.ReadByte()
		if err != nil {
			return nil
		}
		fmt.Println("opCode:", opCode)
		switch opCode {
		case OpCodeAUX:
			key, err := readStringEncoding(reader)
			if err != nil {
				fmt.Println("failed to read aux key", err)
				return err
			}
			fmt.Println("aux field key:", key)

			value, err := readStringEncoding(reader)
			if err != nil {
				fmt.Println("failed to aux value", err)
				return err
			}
			fmt.Println("aux field value:", value)
		case OpCodeSELECTDB:
			dbNum, err := reader.ReadByte()
			if err != nil {
				fmt.Println("failed to read dbnum", err)
				return err
			}
			fmt.Println("db number:", int(dbNum))

			resizedb, err := reader.ReadByte()
			if err != nil {
				fmt.Println("failed to read resizedb", err)
				return err
			}
			fmt.Println("resizedb", resizedb)

			hashTableSize, err := readLengthEncoding(reader)
			if err != nil {
				fmt.Println("failed to read hashTableSize", err)
				return err
			}
			fmt.Println("hashTableSize:", hashTableSize)

			expireHashTableSize, err := readLengthEncoding(reader)
			if err != nil {
				fmt.Println("failed to read expireHashTableSize", err)
				return err
			}
			fmt.Println("expireHashTableSize:", expireHashTableSize)
		case OpCodeEXPIRETIME:
			expiry := make([]byte, 4)
			_, err := reader.Read(expiry)
			if err != nil {
				fmt.Println("failed to read RDB", err)
				return err
			}
			expireTimestamp := int64(binary.BigEndian.Uint32(expiry))

			valueType, err := reader.ReadByte()
			if err != nil {
				fmt.Println("failed to read RDB", err)
				return err
			}
			fmt.Println("valueType:", valueType)

			key, err := readStringEncoding(reader)
			if err != nil {
				fmt.Println("failed to read RDB", err)
				return err
			}
			fmt.Println("key:", key)

			value, err := readValue(valueType, reader)
			if err != nil {
				fmt.Println("failed to read RDB", err)
				return err
			}
			fmt.Println("value:", value)

			if expireTimestamp > time.Now().Unix() {
				db.db[key] = KeyValue{
					Value:     value,
					HasExpiry: true,
					Expires:   time.Unix(expireTimestamp, 0),
				}
			}
		case OpCodeEXPIRETIMEMS:
			expiry := make([]byte, 8)
			_, err := reader.Read(expiry)
			if err != nil {
				fmt.Println("failed to read RDB", err)
				return err
			}
			expireTimestampMs := int64(binary.BigEndian.Uint64(expiry))

			valueType, err := reader.ReadByte()
			if err != nil {
				fmt.Println("failed to read RDB", err)
				return err
			}
			fmt.Println("valueType:", valueType)

			key, err := readStringEncoding(reader)
			if err != nil {
				fmt.Println("failed to read RDB", err)
				return err
			}
			fmt.Println("key:", key)

			value, err := readValue(valueType, reader)
			if err != nil {
				fmt.Println("failed to read RDB", err)
				return err
			}
			fmt.Println("value:", value)

			if expireTimestampMs > time.Now().UnixMilli() {
				db.db[key] = KeyValue{
					Value:     value,
					HasExpiry: true,
					Expires:   time.Unix(expireTimestampMs/1000, (expireTimestampMs%1000)*int64(time.Millisecond)),
				}
			}

		case 0:
			valueType := opCode
			fmt.Println("valueType:", valueType)

			key, err := readStringEncoding(reader)
			if err != nil {
				fmt.Println("failed to read key", err)
				return err
			}
			fmt.Println("key:", key)

			value, err := readValue(valueType, reader)
			if err != nil {
				fmt.Println("failed to read value", err)
				return err
			}
			fmt.Println("value:", value)

			db.db[key] = KeyValue{
				Value: value,
			}

		case OpCodeEOF:
			bytes := make([]byte, 8)
			_, err := reader.Read(bytes)
			if err != nil {
				fmt.Println("failed to read checksum", err)
				return err
			}
			fmt.Println("checksum", string(bytes))
			return nil
		}
	}
}

func readStringEncoding(reader *bufio.Reader) (string, error) {
	length, err := readLengthEncoding(reader)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	fmt.Println("string length:", length)
	bytes := make([]byte, length)
	_, err = reader.Read(bytes)
	if err != nil {
		fmt.Println(err)
		return "", err
	}
	return string(bytes), nil
}

func readLengthEncoding(reader *bufio.Reader) (int, error) {
	num, err := reader.ReadByte()
	if err != nil {
		fmt.Println(err)
		return 0, err
	}
	// fmt.Println("num:", num)
	switch {
	case num <= 0b00111111: // bits: 00xxxxxx
		return int(num & 0b00111111), nil
	case num <= 0b01111111: // bits: 01xxxxxx
		nextByte, err := reader.ReadByte()
		if err != nil {
			fmt.Println(err)
			return 0, err
		}
		// fmt.Println("nextByte:", nextByte)
		length := binary.BigEndian.Uint16([]byte{num & 0b01111111, nextByte})
		return int(length), nil
	case num <= 0b10111111: // bits: 10xxxxxx
		bytes := make([]byte, 4)
		_, err := reader.Read(bytes)
		if err != nil {
			fmt.Println(err)
			return 0, err
		}
		length := binary.BigEndian.Uint32(bytes)
		return int(length), nil
	default: // bits: 11xxxxxx
		// The next object is encoded in a special format. The remaining 6 bits indicate the format. May be used to store numbers or Strings, see String Encoding
		return int(num & 0b00111111), nil
	}
}

func readValue(valueType byte, reader *bufio.Reader) (string, error) {
	switch valueType {
	case 0:
		return readStringEncoding(reader)
	default:
		return "", fmt.Errorf("received invalid value type: %v", valueType)
	}
}
