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
	db := map[string]string{}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("failed to accept connection", err)
			os.Exit(1)
		}
		go handleNewConnection(conn, db)
	}
}

func handleNewConnection(conn net.Conn, db map[string]string) {
	fmt.Println("received a new connection")
	defer conn.Close()
	for {
		resp := NewResp(conn)
		writer := NewWriter(conn)
		value, err := resp.Read()
		if err != nil {
			continue
		}
		if value.Type != Arrays {
			fmt.Println("Invalid request, expected array")
			continue
		}
		if len(value.Array) == 0 {
			fmt.Println("Invalid request, expected array length > 0")
			continue
		}
		command := strings.ToUpper(value.Array[0].Data.(string))
		args := value.Array[1:]
		fmt.Println("received command", command, "args:", args)
		handler, ok := Commands[command]
		if !ok {
			fmt.Println("received invalid command")
			writer.Write(Value{
				Type: SimpleErrors,
				Data: "ERR unknown command",
			})
			continue
		}
		res := handler(args)
		writer.Write(res)
	}
}
