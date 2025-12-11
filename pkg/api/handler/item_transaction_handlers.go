package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
    
	"github.com/syndtr/goleveldb/leveldb"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/core"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

type TransactWriteItem struct {
	ConditionCheck *struct {
		TableName string `json:"TableName"`
		Key model.Record `json:"Key"`
		ConditionExpression string `json:"ConditionExpression"`
		ExpressionAttributeValues map[string]model.AttributeValue `json:"ExpressionAttributeValues"`
	} `json:"ConditionCheck,omitempty"`
	Put *struct {
		Item model.Record `json:"Item"`
		TableName string `json:"TableName"`
	} `json:"Put,omitempty"`
	Delete *struct {
		Key model.Record `json:"Key"`
		TableName string `json:"TableName"`
	} `json:"Delete,omitempty"`
}

type TransactWriteItemsInput struct {
	TransactItems []TransactWriteItem `json:"TransactItems"`
}

func (s *Server) handleTransactWriteItems(w http.ResponseWriter, body []byte) {
	var input TransactWriteItemsInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	batch := new(leveldb.Batch)

	s.Database.Lock()
	defer s.Database.Unlock()

	for _, item := range input.TransactItems {
		if item.ConditionCheck != nil {
			cc := item.ConditionCheck
			
			schema, ok := s.Database.Tables[cc.TableName]
			if !ok {
				s.writeDynamoDBError(w, "ResourceNotFoundException", fmt.Sprintf("Table %s not found", cc.TableName), http.StatusBadRequest)
				return
			}
			
			pkAV, _ := cc.Key[schema.PartitionKey]
			pkVal, _ := model.GetAttributeValueString(pkAV)

			var skVal string
			if schema.SortKey != "" {
				skAV, _ := cc.Key[schema.SortKey]
				skVal, _ = model.GetAttributeValueString(skAV)
			}

			levelDBKey := model.BuildLevelDBKey(cc.TableName, pkVal, skVal)

			oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
			var recordForEvaluation model.Record
			
			if err != nil && err != leveldb.ErrNotFound {
				http.Error(w, "Internal DB error", http.StatusInternalServerError)
				return
			}

			if err == nil {
				recordForEvaluation, _ = model.UnmarshalRecord(oldValue)
			} else {
				recordForEvaluation = nil 
			}
			
			conditionInput := model.ConditionInput{
				ConditionExpression:       cc.ConditionExpression,
				ExpressionAttributeNames:  map[string]string{},
				ExpressionAttributeValues: cc.ExpressionAttributeValues,
			}
			
			ok, condErr := core.EvaluateConditionExpression(recordForEvaluation, conditionInput)
			
			if condErr != nil {
				s.writeDynamoDBError(w, "ValidationException", condErr.Error(), http.StatusBadRequest)
				return
			}
			if !ok {
				s.writeDynamoDBError(w, "ConditionCheckFailedException", "Transaction condition check failed.", http.StatusBadRequest)
				return
			}
		}
	}

	for _, item := range input.TransactItems {
		var tableName string
		var key model.Record
		var itemData model.Record
		var opType string

		if item.Put != nil {
			tableName = item.Put.TableName
			itemData = item.Put.Item
			opType = "PUT"
		} else if item.Delete != nil {
			tableName = item.Delete.TableName
			key = item.Delete.Key
			opType = "DELETE"
		} else if item.ConditionCheck != nil {
			continue
		} else {
			s.writeDynamoDBError(w, "ValidationException", "Invalid TransactItem structure.", http.StatusBadRequest)
			return
		}

		schema, ok := s.Database.Tables[tableName]
		if !ok {
			s.writeDynamoDBError(w, "ResourceNotFoundException", fmt.Sprintf("Table %s not found", tableName), http.StatusBadRequest)
			return
		}

		pkAV, _ := key[schema.PartitionKey]
		if opType == "PUT" { pkAV = itemData[schema.PartitionKey] }

		pkVal, _ := model.GetAttributeValueString(pkAV)

		var skVal string
		if schema.SortKey != "" {
			skAV, _ := key[schema.SortKey]
			if opType == "PUT" { skAV = itemData[schema.SortKey] }
			skVal, _ = model.GetAttributeValueString(skAV)
		}

		levelDBKey := model.BuildLevelDBKey(tableName, pkVal, skVal)

		oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
		var oldRecord model.Record
		if err != leveldb.ErrNotFound && err != nil {
			http.Error(w, "Internal DB error", http.StatusInternalServerError)
			return
		}
		if err == nil {
			oldRecord, _ = model.UnmarshalRecord(oldValue)
		}


		if opType == "PUT" {
			core.UpdateGSI(batch, schema, oldRecord, itemData)

			value, _ := model.MarshalRecord(itemData)
			batch.Put([]byte(levelDBKey), value)
		} else if opType == "DELETE" {
			core.UpdateGSI(batch, schema, oldRecord, nil) 

			batch.Delete([]byte(levelDBKey))
		}
	}

	if err := s.Database.DB.Write(batch, nil); err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Internal DB error during transaction write.", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
}
