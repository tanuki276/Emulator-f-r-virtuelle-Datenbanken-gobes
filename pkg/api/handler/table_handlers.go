// pkg/api/handler/table_handlers.go
package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
    "Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

type CreateTableInput struct {
	TableName string `json:"TableName"`
	KeySchema []struct {
		AttributeName string `json:"AttributeName"`
		KeyType string `json:"KeyType"`
	} `json:"KeySchema"`
}

func (s *Server) handleCreateTable(w http.ResponseWriter, body []byte) {
	var input CreateTableInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	if input.TableName == "" {
		s.writeDynamoDBError(w, "ValidationException", "TableName must be specified", http.StatusBadRequest)
		return
	}

	schema := model.TableSchema{TableName: input.TableName, GSIs: make(map[string]model.GsiSchema)}
	for _, k := range input.KeySchema {
		if k.KeyType == "HASH" {
			schema.PartitionKey = k.AttributeName
		} else if k.KeyType == "RANGE" {
			schema.SortKey = k.AttributeName
		}
	}

	s.DB.mu.Lock()
	defer s.DB.mu.Unlock()

	if _, exists := s.DB.Tables[input.TableName]; exists {
		s.writeDynamoDBError(w, "ResourceInUseException", "Table already exists", http.StatusBadRequest)
		return
	}

	s.DB.Tables[input.TableName] = schema

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf(`{"TableDescription": {"TableName": "%s", "TableStatus": "ACTIVE"}}`, input.TableName)))
}

type DescribeTableInput struct {
    TableName string `json:"TableName"`
}

func (s *Server) handleDescribeTable(w http.ResponseWriter, body []byte) {
    var input DescribeTableInput
    if err := json.Unmarshal(body, &input); err != nil {
        s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
        return
    }

    s.DB.mu.RLock()
    schema, ok := s.DB.Tables[input.TableName]
    s.DB.mu.RUnlock()

    if !ok {
        s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
        return
    }

    keySchema := []map[string]string{
        {"AttributeName": schema.PartitionKey, "KeyType": "HASH"},
    }
    if schema.SortKey != "" {
        keySchema = append(keySchema, map[string]string{"AttributeName": schema.SortKey, "KeyType": "RANGE"})
    }

    response := map[string]interface{}{
        "TableDescription": map[string]interface{}{
            "TableName": input.TableName,
            "TableStatus": "ACTIVE",
            "KeySchema": keySchema,
            "ItemCount": 0,
        },
    }

    responseBody, _ := json.Marshal(response)
    w.WriteHeader(http.StatusOK)
    w.Write(responseBody)
}

type ListTablesOutput struct {
    TableNames []string `json:"TableNames"`
}

func (s *Server) handleListTables(w http.ResponseWriter, body []byte) {
    s.DB.mu.RLock()
    defer s.DB.mu.RUnlock()

    tableNames := make([]string, 0, len(s.DB.Tables))
    for name := range s.DB.Tables {
        tableNames = append(tableNames, name)
    }

    output := ListTablesOutput{TableNames: tableNames}
    responseBody, _ := json.Marshal(output)

    w.WriteHeader(http.StatusOK)
    w.Write(responseBody)
}
