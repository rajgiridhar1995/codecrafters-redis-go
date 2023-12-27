package main

const (
	MAGIC = "REDIS"

	OpCodeEOF          byte = 0xFF
	OpCodeSELECTDB     byte = 0xFE
	OpCodeEXPIRETIME   byte = 0xFD
	OpCodeEXPIRETIMEMS byte = 0xFC
	OpCodeRESIZEDB     byte = 0xFB
	OpCodeAUX          byte = 0xFA
)

type RDB struct {
}

func NewRDB(conf Config) *RDB {
	return &RDB{}
}
