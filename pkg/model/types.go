package model

type AttributeValue map[string]interface{}
type Record map[string]AttributeValue

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
}

type QueryInput struct {
	TableName string `json:"TableName"`
	IndexName string `json:"IndexName,omitempty"`
	KeyConditionExpression string `json:"KeyConditionExpression"`
	ExpressionAttributeValues map[string]AttributeValue `json:"ExpressionAttributeValues"`
	Limit int64 `json:"Limit"`
	ScanIndexForward bool `json:"ScanIndexForward"`
	ExclusiveStartKey Record `json:"ExclusiveStartKey"`
}
