package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	fmt.Println("Started server")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("failed to accept connection", err)
			os.Exit(1)
		}
		go handleNewConnection(conn)
	}
}

func handleNewConnection(conn net.Conn) {
	fmt.Println("received a new connection")
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("failed to read from connection", err)
			return
		}
		if n == 0 {
			fmt.Println("connection closed")
			return
		}
		buf = buf[:n]
		fmt.Println("received:", string(buf))
		command := string(buf)
		cmdSplit := strings.Split(command, "\r\n")
		switch strings.ToUpper(cmdSplit[2]) {
		case "PING":
			conn.Write([]byte("+PONG\r\n"))
		case "ECHO":
			conn.Write([]byte(fmt.Sprint("+", cmdSplit[len(cmdSplit)-2], "\r\n")))
		}
	}
}
