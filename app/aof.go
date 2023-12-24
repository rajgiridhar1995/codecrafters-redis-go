package main

import (
	"bufio"
	"fmt"
	"os"
	"path"
)

type AOF struct {
	File *os.File
	rw   *bufio.ReadWriter
}

func NewAOF(conf Config) *AOF {
	filePath := path.Join(conf.AofDir, conf.AofFileName)
	f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0666)
	if err != nil {
		fmt.Println("failed to create AOF", err)
		os.Exit(1)
	}
	return &AOF{
		File: f,
		rw:   bufio.NewReadWriter(bufio.NewReader(f), bufio.NewWriter(f)),
	}
}
