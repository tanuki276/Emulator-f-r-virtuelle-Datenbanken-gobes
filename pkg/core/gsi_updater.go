package core

import (
	"fmt"
	"Emulator-fr-virtuelle-Datenbanken-gobes/pkg/model"
	"github.com/syndtr/goleveldb/leveldb"
)

func UpdateGSI(batch *leveldb.Batch, schema model.TableSchema, oldRecord model.Record, newRecord model.Record) {
	if len(schema.GSIs) == 0 {
		return
	}

	for _, gsiSchema := range schema.GSIs {
		oldPKVal, oldSKVal, oldGSIExists := getGSIKeyValues(oldRecord, gsiSchema)
		newPKVal, newSKVal, newGSIExists := getGSIKeyValues(newRecord, gsiSchema)
		
		var basePKVal string
		
		
		
		if newRecord != nil {
			if basePKAV, ok := newRecord[schema.PartitionKey]; ok {
				basePKVal, _ = model.GetAttributeValueString(basePKAV)
			}
		} else if oldRecord != nil {
			if basePKAV, ok := oldRecord[schema.PartitionKey]; ok {
				basePKVal, _ = model.GetAttributeValueString(basePKAV)
			}
		} else {
			continue 
		}

		
		if oldGSIExists {
			
			if !newGSIExists || oldPKVal != newPKVal || oldSKVal != newSKVal {
				oldGSIKey := model.BuildGSILevelDBKey(gsiSchema.IndexName, oldPKVal, oldSKVal, basePKVal)
				batch.Delete([]byte(oldGSIKey))
			}
		}

		
		if newGSIExists {
			newGSIKey := model.BuildGSILevelDBKey(gsiSchema.IndexName, newPKVal, newSKVal, basePKVal)
			
			
			gsiValue, err := model.MarshalRecord(newRecord)
			if err != nil {
				
				
				continue 
			}

			batch.Put([]byte(newGSIKey), gsiValue)
		}
	}
}

func getGSIKeyValues(record model.Record, gsiSchema model.GsiSchema) (string, string, bool) {
	if record == nil {
		return "", "", false
	}

	pkAV, pkExists := record[gsiSchema.PartitionKey]
	if !pkExists {
		return "", "", false
	}
	pkVal, _ := model.GetAttributeValueString(pkAV)

	var skVal string
	skExists := true
	if gsiSchema.SortKey != "" {
		skAV, ok := record[gsiSchema.SortKey]
		if ok {
			skVal, _ = model.GetAttributeValueString(skAV)
		} else {
			
			
			return "", "", false
		}
	}

	
	return pkVal, skVal, true
}
