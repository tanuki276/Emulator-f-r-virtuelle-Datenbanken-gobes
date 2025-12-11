package model

type AttributeValue map[string]interface{}

type Record map[string]AttributeValue

type Key map[string]AttributeValue

type GsiSchema struct {
	IndexName string
	PartitionKey string
	SortKey string
}

type TableSchema struct {
	TableName string
	PartitionKey string
	SortKey string
	GSIs map[string]GsiSchema
	TTLAttribute string 
}

type PutItemInput struct {
	TableName string `json:"TableName"`
	Item Record `json:"Item"`
	ConditionExpression string `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	ReturnValues string `json:"ReturnValues,omitempty"`
}

type QueryInput struct {
	TableName string `json:"TableName"`
	IndexName string `json:"IndexName,omitempty"`
	KeyConditionExpression string `json:"KeyConditionExpression"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]AttributeValue `json:"ExpressionAttributeValues"`
	Limit int64 `json:"Limit"`
	ScanIndexForward bool `json:"ScanIndexForward"`
	ExclusiveStartKey Record `json:"ExclusiveStartKey,omitempty"`
}

type UpdateItemInput struct {
	TableName string `json:"TableName"`
	Key Key `json:"Key"`
	UpdateExpression string `json:"UpdateExpression"`
	ConditionExpression string `json:"ConditionExpression,omitempty"`
	ExpressionAttributeNames map[string]string `json:"ExpressionAttributeNames,omitempty"`
	ExpressionAttributeValues map[string]AttributeValue `json:"ExpressionAttributeValues,omitempty"`
	ReturnValues string `json:"ReturnValues,omitempty"`
}

type ConditionInput struct {
	ConditionExpression string
	ExpressionAttributeNames map[string]string
	ExpressionAttributeValues map[string]AttributeValue
}
