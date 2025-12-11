package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"
	"fmt"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "snapshot":
			if len(os.Args) < 4 {
				fmt.Println("Usage: go run . snapshot [create|load] <name>")
				os.Exit(1)
			}
			
			db, err := NewDatabase()
			if err != nil {
				log.Fatalf("DB initialization failed: %v", err)
			}

			subcommand := os.Args[2]
			name := os.Args[3]

			if subcommand == "create" {
				if err := db.CreateSnapshot(name); err != nil {
					log.Fatalf("Snapshot creation failed: %v", err)
				}
			} else if subcommand == "load" {
				if err := db.LoadSnapshot(name); err != nil {
					log.Fatalf("Snapshot loading failed: %v", err)
				}
			} else {
				fmt.Println("Invalid snapshot subcommand. Use 'create' or 'load'.")
				os.Exit(1)
			}
			os.Exit(0)
		}
	}

	db, err := NewDatabase()
	if err != nil {
		log.Fatalf("Database initialization failed: %v", err)
	}
	defer db.Close()

	server := NewServer(db)
	server.ListenAndServe(":8000")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Server shutting down...")
}
