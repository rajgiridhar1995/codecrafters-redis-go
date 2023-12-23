package main

import (
	"fmt"
	"net"
	"os"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
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
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			fmt.Println("failed to read from connection", err)
			os.Exit(1)
		}
		if n == 0 {
			fmt.Println("connection closed")
			return
		}
		fmt.Println("received:", string(buf[:n]))
		conn.Write([]byte("+PONG\r\n"))
	}
}
