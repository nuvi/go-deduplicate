package deduplicate

import (
	"encoding/json"
	"reflect"
	"runtime"
)

func (tp *TaskPool[KEY_TYPE, VALUE_TYPE]) getKeyStr(key KEY_TYPE) (string, error) {
	bytes, err := json.Marshal(key)
	if err != nil {
		return "", err
	}
	return tp.getterName + "-" + string(bytes), nil
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
