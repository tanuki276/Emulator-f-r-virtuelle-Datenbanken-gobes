package api

import (
	"log"
	"net/http"
	"strings"

	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/core"
)

type Server struct {
	Database *core.Database
	Mux *http.ServeMux
}

func NewServer(db *core.Database) *Server {
	s := &Server{
		Database: db,
		Mux: http.NewServeMux(),
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	
	s.Mux.HandleFunc("/dynamodb", func(w http.ResponseWriter, r *http.Request) {
		target := r.Header.Get("X-Amz-Target")
		
		if target == "" {
			http.Error(w, "X-Amz-Target header missing", http.StatusBadRequest)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read request body", http.StatusInternalServerError)
			return
		}

		parts := strings.Split(target, ".")
		if len(parts) < 2 {
			http.Error(w, "Invalid X-Amz-Target format", http.StatusBadRequest)
			return
		}
		operation := parts[1]

		switch operation {
		case "CreateTable":
			s.handleCreateTable(w, body)
		case "ListTables":
			s.handleListTables(w)
		case "DeleteTable":
			s.handleDeleteTable(w, body)
		case "PutItem":
			s.handlePutItem(w, body)
		case "GetItem":
			s.handleGetItem(w, body)
		case "Query":
			s.handleQuery(w, body)
		case "Scan":
			s.handleScan(w, body)
		case "UpdateItem":
			s.handleUpdateItem(w, body)
		case "DeleteItem":
			s.handleDeleteItem(w, body)
		case "BatchWriteItem":
			s.handleBatchWriteItem(w, body)
		case "TransactWriteItems":
			s.handleTransactWriteItems(w, body)
		case "CreateSnapshot":
			s.handleCreateSnapshot(w, body)
		case "LoadSnapshot":
			s.handleLoadSnapshot(w, body)
		case "DeleteAllData":
			s.handleDeleteAllData(w)
		default:
			s.writeDynamoDBError(w, "UnknownOperationException", "The requested operation is not supported.", http.StatusBadRequest)
		}
	})
}

func (s *Server) writeDynamoDBError(w http.ResponseWriter, code, message string, status int) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(status)
	response := fmt.Sprintf(`{"__type": "com.amazonaws.dynamodb.%s", "message": "%s"}`, code, message)
	w.Write([]byte(response))
}

func (s *Server) Start(addr string) {
	log.Printf("DynamoDB Emulator running on http://%s/dynamodb", addr)
	if err := http.ListenAndServe(addr, s.Mux); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
