package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	fmt.Println("Logs from your program will appear here!")

	dir := flag.String("dir", "", "The directory where RDB files are stored")
	dbfilename := flag.String("dbfilename", "", "The name of the RDB file")

	flag.Parse()
	fmt.Println("dir:", *dir)
	fmt.Println("dbfilename:", *dbfilename)

	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
	conf := Config{
		RDBDir:      *dir,
		RDBFileName: *dbfilename,
	}
	db := NewDB(conf)
	fmt.Println("Started server")

	err = db.ReadRDB()
	if err != nil {
		fmt.Println("failed to read RDB", err)
		os.Exit(1)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("failed to accept connection", err)
			os.Exit(1)
		}
		go handleNewConnection(conn, db)
	}
}

func handleNewConnection(conn net.Conn, db *DB) {
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
		command := strings.ToUpper(value.Array[0].Data)
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
		res := handler(db, args)
		writer.Write(res)
	}
}
