package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
    
	"github.com/syndtr/goleveldb/leveldb"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/core"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
)

func (s *Server) handlePutItem(w http.ResponseWriter, body []byte) {
	var input model.PutItemInput
    if err := json.Unmarshal(body, &input); err != nil {
        s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
        return
    }
    
    s.Database.RLock()
    schema, ok := s.Database.Tables[input.TableName]
    s.Database.RUnlock()
    if !ok {
        s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
        return
    }

    pkAV, ok := input.Item[schema.PartitionKey]
    if !ok {
        s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key '%s' value missing", schema.PartitionKey), http.StatusBadRequest)
        return
    }
    pkVal, _ := model.GetAttributeValueString(pkAV)

    var skVal string
    if schema.SortKey != "" {
        skAV, ok := input.Item[schema.SortKey]
        if ok {
            skVal, _ = model.GetAttributeValueString(skAV)
        }
    }

    levelDBKey := model.BuildLevelDBKey(input.TableName, pkVal, skVal)

	batch := new(leveldb.Batch)
	s.Database.Lock()
	defer s.Database.Unlock()

	oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
	var oldRecord model.Record
	recordExists := err == nil
	if err == nil {
		oldRecord, _ = model.UnmarshalRecord(oldValue)
	}

	if input.ConditionExpression != "" {
		conditionInput := model.ConditionInput{
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
		}
		
		recordForEvaluation := oldRecord
		if !recordExists { recordForEvaluation = nil }
		
		ok, condErr := core.EvaluateConditionExpression(recordForEvaluation, conditionInput)
		
		if condErr != nil {
			s.writeDynamoDBError(w, "ValidationException", condErr.Error(), http.StatusBadRequest)
			return
		}
		if !ok {
			s.writeDynamoDBError(w, "ConditionCheckFailedException", "The conditional request failed.", http.StatusBadRequest)
			return
		}
	}

	core.UpdateGSI(batch, schema, oldRecord, input.Item)

	value, err := model.MarshalRecord(input.Item)
	if err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Failed to marshal item", http.StatusInternalServerError)
		return
	}
	batch.Put([]byte(levelDBKey), value)

	if err := s.Database.DB.Write(batch, nil); err != nil {
		http.Error(w, "Internal DB error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
}

type DeleteItemInput struct {
	TableName string `json:"TableName"`
	Key map[string]model.AttributeValue `json:"Key"`
	ReturnValues string `json:"ReturnValues,omitempty"`
    ConditionExpression string `json:"ConditionExpression,omitempty"`
    ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
    ExpressionAttributeValues map[string]AttributeValue `json:"ExpressionAttributeValues,omitempty"`
}

func (s *Server) handleDeleteItem(w http.ResponseWriter, body []byte) {
	var input DeleteItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input for DeleteItem", http.StatusBadRequest)
		return
	}

	s.Database.RLock()
	schema, ok := s.Database.Tables[input.TableName]
	s.Database.RUnlock()
	if !ok {
		s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
		return
	}

	pkAV, ok := input.Key[schema.PartitionKey]
	if !ok {
		s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key '%s' value missing in Key", schema.PartitionKey), http.StatusBadRequest)
		return
	}
	pkVal, _ := model.GetAttributeValueString(pkAV)

	var skVal string
	if schema.SortKey != "" {
		if skAV, ok := input.Key[schema.SortKey]; ok {
			skVal, _ = model.GetAttributeValueString(skAV)
		}
	}
	levelDBKey := model.BuildLevelDBKey(input.TableName, pkVal, skVal)

	s.Database.Lock()
	defer s.Database.Unlock()

	oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
	recordExists := err == nil
	var oldRecord model.Record
	
	if err == leveldb.ErrNotFound {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{}`))
		return
	}
	if err != nil {
		http.Error(w, "Internal DB error on retrieve", http.StatusInternalServerError)
		return
	}
	
	if err := model.UnmarshalRecord(oldValue, &oldRecord); err != nil {
		http.Error(w, "Failed to unmarshal existing item", http.StatusInternalServerError)
		return
	}

    if input.ConditionExpression != "" {
		conditionInput := model.ConditionInput{
			ConditionExpression:       input.ConditionExpression,
			ExpressionAttributeNames:  input.ExpressionAttributeNames,
			ExpressionAttributeValues: input.ExpressionAttributeValues,
		}
		
		ok, condErr := core.EvaluateConditionExpression(oldRecord, conditionInput)
		
		if condErr != nil {
			s.writeDynamoDBError(w, "ValidationException", condErr.Error(), http.StatusBadRequest)
			return
		}
		if !ok {
			s.writeDynamoDBError(w, "ConditionCheckFailedException", "The conditional request failed.", http.StatusBadRequest)
			return
		}
	}

	batch := new(leveldb.Batch)
	core.UpdateGSI(batch, schema, oldRecord, nil) 

	batch.Delete([]byte(levelDBKey))

	if err := s.Database.DB.Write(batch, nil); err != nil {
		http.Error(w, "Internal DB error on write", http.StatusInternalServerError)
		return
	}

    responseBody := []byte(`{}`)
    if input.ReturnValues == "ALL_OLD" {
        marshaledOldRecord, _ := json.Marshal(oldRecord)
        responseBody = []byte(fmt.Sprintf(`{"Attributes": %s}`, marshaledOldRecord))
    }

	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

func (s *Server) handleUpdateItem(w http.ResponseWriter, body []byte) {
	var input model.UpdateItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input for UpdateItem", http.StatusBadRequest)
		return
	}

	s.Database.RLock()
	schema, ok := s.Database.Tables[input.TableName]
	s.Database.RUnlock()
	if !ok {
		s.writeDynamoDBError(w, "ResourceNotFoundException", "Table not found", http.StatusBadRequest)
		return
	}

	pkAV, ok := input.Key[schema.PartitionKey]
	if !ok {
		s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key '%s' value missing in Key", schema.PartitionKey), http.StatusBadRequest)
		return
	}
	pkVal, _ := model.GetAttributeValueString(pkAV)

	var skVal string
	if schema.SortKey != "" {
		if skAV, ok := input.Key[schema.SortKey]; ok {
			skVal, _ = model.GetAttributeValueString(skAV)
		}
	}
	levelDBKey := model.BuildLevelDBKey(input.TableName, pkVal, skVal)

	s.Database.Lock()
	defer s.Database.Unlock()

	oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
	oldRecord := make(model.Record)
    recordExists := err == nil
	if err != leveldb.ErrNotFound && err != nil {
		http.Error(w, "Internal DB error on retrieve", http.StatusInternalServerError)
		return
	}
	if err == nil {
		oldRecord, _ = model.UnmarshalRecord(oldValue)
	}

    if input.ConditionExpression != "" {
        conditionInput := model.ConditionInput{
            ConditionExpression:       input.ConditionExpression,
            ExpressionAttributeNames:  input.ExpressionAttributeNames,
            ExpressionAttributeValues: input.ExpressionAttributeValues,
        }
        
        recordForEvaluation := oldRecord
        if !recordExists { recordForEvaluation = nil }
        
        ok, condErr := core.EvaluateConditionExpression(recordForEvaluation, conditionInput)
        
        if condErr != nil {
            s.writeDynamoDBError(w, "ValidationException", condErr.Error(), http.StatusBadRequest)
            return
        }
        if !ok {
            s.writeDynamoDBError(w, "ConditionCheckFailedException", "The conditional request failed.", http.StatusBadRequest)
            return
        }
    }
    
    actions, err := core.ParseUpdateExpression(&input) 
    if err != nil {
        s.writeDynamoDBError(w, "ValidationException", err.Error(), http.StatusBadRequest)
        return
    }

    newRecord, err := core.ApplyUpdateActions(oldRecord, actions)
    if err != nil {
        s.writeDynamoDBError(w, "ValidationException", err.Error(), http.StatusBadRequest)
        return
    }
    
    if _, ok := actions.Set[schema.PartitionKey]; ok || len(actions.Add[schema.PartitionKey]) > 0 {
        s.writeDynamoDBError(w, "ValidationException", "Cannot update Partition Key", http.StatusBadRequest)
        return
    }
    if schema.SortKey != "" {
        if _, ok := actions.Set[schema.SortKey]; ok || len(actions.Add[schema.SortKey]) > 0 {
             s.writeDynamoDBError(w, "ValidationException", "Cannot update Sort Key", http.StatusBadRequest)
            return
        }
    }

	batch := new(leveldb.Batch)
	core.UpdateGSI(batch, schema, oldRecord, newRecord)

	value, err := model.MarshalRecord(newRecord)
	if err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Failed to marshal updated item", http.StatusInternalServerError)
		return
	}
	batch.Put([]byte(levelDBKey), value)

	if err := s.Database.DB.Write(batch, nil); err != nil {
		http.Error(w, "Internal DB error on write", http.StatusInternalServerError)
		return
	}

    responseBody := []byte(`{}`)
    if input.ReturnValues == "ALL_NEW" {
        marshaledNewRecord, _ := json.Marshal(newRecord)
        responseBody = []byte(fmt.Sprintf(`{"Attributes": %s}`, marshaledNewRecord))
    }

	w.WriteHeader(http.StatusOK)
	w.Write(responseBody)
}

type WriteRequest struct {
	PutRequest *struct {
		Item model.Record `json:"Item"`
	} `json:"PutRequest,omitempty"`
	DeleteRequest *struct {
		Key model.Record `json:"Key"`
	} `json:"DeleteRequest,omitempty"`
}

type BatchWriteItemInput struct {
	RequestItems map[string][]WriteRequest `json:"RequestItems"`
}

type BatchWriteItemOutput struct {
	UnprocessedItems map[string][]WriteRequest `json:"UnprocessedItems,omitempty"`
}

func (s *Server) handleBatchWriteItem(w http.ResponseWriter, body []byte) {
	var input BatchWriteItemInput
	if err := json.Unmarshal(body, &input); err != nil {
		s.writeDynamoDBError(w, "ValidationException", "Invalid JSON input", http.StatusBadRequest)
		return
	}

	totalBatch := new(leveldb.Batch)
	
	s.Database.Lock()
	defer s.Database.Unlock()

	for tableName, requests := range input.RequestItems {
		
		schema, ok := s.Database.Tables[tableName]
		if !ok {
			s.writeDynamoDBError(w, "ResourceNotFoundException", fmt.Sprintf("Table %s not found", tableName), http.StatusBadRequest)
			return
		}

		for _, req := range requests {
			var pkAV model.AttributeValue
			var itemData model.Record
			var isDelete bool = false

			if req.PutRequest != nil {
				itemData = req.PutRequest.Item
				pkAV = itemData[schema.PartitionKey]
			} else if req.DeleteRequest != nil {
				itemData = req.DeleteRequest.Key 
				pkAV = itemData[schema.PartitionKey]
				isDelete = true
			} else {
				continue
			}

			pkVal, _ := model.GetAttributeValueString(pkAV)

			var skVal string
			if schema.SortKey != "" {
				skAV, ok := itemData[schema.SortKey]
				if ok {
					skVal, _ = model.GetAttributeValueString(skAV)
				}
			}

			levelDBKey := model.BuildLevelDBKey(tableName, pkVal, skVal)

			oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
			var oldRecord model.Record
			
			if err != nil && err != leveldb.ErrNotFound {
				http.Error(w, "Internal DB error", http.StatusInternalServerError)
				return
			}
			if err == nil {
				oldRecord, _ = model.UnmarshalRecord(oldValue)
			}
			
			if isDelete {
				core.UpdateGSI(totalBatch, schema, oldRecord, nil)
				totalBatch.Delete([]byte(levelDBKey))
			} else {
				core.UpdateGSI(totalBatch, schema, oldRecord, itemData)
				value, _ := model.MarshalRecord(itemData)
				totalBatch.Put([]byte(levelDBKey), value)
			}
		}
	}
	
	if err := s.Database.DB.Write(totalBatch, nil); err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Internal DB error during batch write.", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
}

// --- TransactWriteItems handler ---

type TransactWriteItem struct {
	Put *model.PutItemInput `json:"Put,omitempty"`
	Update *model.UpdateItemInput `json:"Update,omitempty"`
	Delete *DeleteItemInput `json:"Delete,omitempty"`
	ConditionCheck *struct {
		TableName string `json:"TableName"`
		Key map[string]model.AttributeValue `json:"Key"`
		ConditionExpression string `json:"ConditionExpression"`
		ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
		ExpressionAttributeValues map[string]model.AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	} `json:"ConditionCheck,omitempty"`
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

	totalBatch := new(leveldb.Batch)

	s.Database.Lock()
	defer s.Database.Unlock()

	for i, item := range input.TransactItems {
		var tableName string
		var key map[string]model.AttributeValue
		var currentInput interface{}

		if item.Put != nil {
			tableName = item.Put.TableName
			key = core.GetItemKey(item.Put.Item, s.Database.Tables[tableName])
			currentInput = item.Put
		} else if item.Update != nil {
			tableName = item.Update.TableName
			key = item.Update.Key
			currentInput = item.Update
		} else if item.Delete != nil {
			tableName = item.Delete.TableName
			key = item.Delete.Key
			currentInput = item.Delete
		} else if item.ConditionCheck != nil {
			tableName = item.ConditionCheck.TableName
			key = item.ConditionCheck.Key
			currentInput = item.ConditionCheck
		} else {
			continue 
		}

		schema, ok := s.Database.Tables[tableName]
		if !ok {
			s.writeDynamoDBError(w, "ResourceNotFoundException", fmt.Sprintf("Table %s not found in item %d", tableName, i), http.StatusBadRequest)
			return
		}

		pkAV, ok := key[schema.PartitionKey]
		if !ok {
			s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Partition Key missing for item %d in table %s", i, tableName), http.StatusBadRequest)
			return
		}
		pkVal, _ := model.GetAttributeValueString(pkAV)

		var skVal string
		if schema.SortKey != "" {
			if skAV, ok := key[schema.SortKey]; ok {
				skVal, _ = model.GetAttributeValueString(skAV)
			}
		}
		levelDBKey := model.BuildLevelDBKey(tableName, pkVal, skVal)

		oldValue, err := s.Database.DB.Get([]byte(levelDBKey), nil)
		var oldRecord model.Record
		recordExists := err == nil
		if err != nil && err != leveldb.ErrNotFound {
			s.writeDynamoDBError(w, "InternalServerError", "Internal DB error during read for transaction.", http.StatusInternalServerError)
			return
		}
		if err == nil {
			oldRecord, _ = model.UnmarshalRecord(oldValue)
		}

		var newRecord model.Record
		var condition model.ConditionInput
		var isWriteOp bool = false

		switch req := currentInput.(type) {
		case *model.PutItemInput:
			isWriteOp = true
			newRecord = req.Item
			condition = model.ConditionInput{
				ConditionExpression:       req.ConditionExpression,
				ExpressionAttributeNames:  req.ExpressionAttributeNames,
				ExpressionAttributeValues: req.ExpressionAttributeValues,
			}
		case *model.UpdateItemInput:
			isWriteOp = true
			condition = model.ConditionInput{
				ConditionExpression:       req.ConditionExpression,
				ExpressionAttributeNames:  req.ExpressionAttributeNames,
				ExpressionAttributeValues: req.ExpressionAttributeValues,
			}
			actions, err := core.ParseUpdateExpression(req)
			if err != nil {
				s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Invalid UpdateExpression for item %d: %v", i, err), http.StatusBadRequest)
				return
			}
			newRecord, err = core.ApplyUpdateActions(oldRecord, actions)
			if err != nil {
				s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Failed to apply update for item %d: %v", i, err), http.StatusBadRequest)
				return
			}

		case *DeleteItemInput:
			isWriteOp = true
			newRecord = nil 
			condition = model.ConditionInput{
				ConditionExpression:       req.ConditionExpression,
				ExpressionAttributeNames:  req.ExpressionAttributeNames,
				ExpressionAttributeValues: req.ExpressionAttributeValues,
			}

		case *struct { TableName string; Key map[string]model.AttributeValue; ConditionExpression string; ExpressionAttributeNames map[string]string; ExpressionAttributeValues map[string]model.AttributeValue }:
			condition = model.ConditionInput{
				ConditionExpression:       req.ConditionExpression,
				ExpressionAttributeNames:  req.ExpressionAttributeNames,
				ExpressionAttributeValues: req.ExpressionAttributeValues,
			}
			
		default:
			continue
		}

		if condition.ConditionExpression != "" {
			recordForEvaluation := oldRecord
			if !recordExists && item.ConditionCheck == nil { recordForEvaluation = nil }

			ok, condErr := core.EvaluateConditionExpression(recordForEvaluation, condition)
			if condErr != nil {
				s.writeDynamoDBError(w, "ValidationException", fmt.Sprintf("Condition evaluation error for item %d: %v", i, condErr), http.StatusBadRequest)
				return
			}
			if !ok {
				s.writeDynamoDBError(w, "TransactionCanceledException", fmt.Sprintf("Transaction canceled, condition check failed for item %d.", i), http.StatusConflict)
				return
			}
		}

		if isWriteOp {
			core.UpdateGSI(totalBatch, schema, oldRecord, newRecord)

			if newRecord != nil {
				value, _ := model.MarshalRecord(newRecord)
				totalBatch.Put([]byte(levelDBKey), value)
			} else {
				totalBatch.Delete([]byte(levelDBKey))
			}
		}
	}

	if err := s.Database.DB.Write(totalBatch, nil); err != nil {
		s.writeDynamoDBError(w, "InternalServerError", "Internal DB error during transaction write.", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{}`))
}
