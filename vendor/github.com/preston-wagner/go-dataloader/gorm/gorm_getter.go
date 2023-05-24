package gormLoader

import (
	"github.com/preston-wagner/go-dataloader"
	"github.com/preston-wagner/unicycle"
	"gorm.io/gorm"
)

// GormGetter trims out boilerplate for the common task of fetching individual rows by a single key
func GormGetter[KEY_TYPE comparable, VALUE_TYPE any](db *gorm.DB, columnName string, keyGetter func(VALUE_TYPE) KEY_TYPE) dataloader.Getter[KEY_TYPE, VALUE_TYPE] {
	return func(keys []KEY_TYPE) (map[KEY_TYPE]VALUE_TYPE, map[KEY_TYPE]error) {
		values := []VALUE_TYPE{}
		result := db.Where(columnName+" IN ?", keys).Find(&values)
		if result.Error != nil {
			return nil, dataloader.ErrForAll(keys, result.Error)
		}
		return unicycle.KeyBy(values, keyGetter), nil
	}
}

// like GormGetter, except instead of fetching individual rows for each key, selects a slice of matches
func GormListGetter[KEY_TYPE comparable, VALUE_TYPE any](db *gorm.DB, columnName string, keyGetter func(VALUE_TYPE) KEY_TYPE) dataloader.Getter[KEY_TYPE, []VALUE_TYPE] {
	return func(keys []KEY_TYPE) (map[KEY_TYPE][]VALUE_TYPE, map[KEY_TYPE]error) {
		values := []VALUE_TYPE{}
		result := db.Where(columnName+" IN ?", keys).Find(&values)
		if result.Error != nil {
			return nil, dataloader.ErrForAll(keys, result.Error)
		}
		return dataloader.FillEmpty(keys, unicycle.GroupBy(values, keyGetter)), nil
	}
}
