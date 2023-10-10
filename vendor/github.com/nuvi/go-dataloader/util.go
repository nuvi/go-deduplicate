package dataloader

// reduces boilerplate for getters that return slices, where an empty slice is an expected possibility
func FillEmpty[KEY_TYPE comparable, VALUE_TYPE any](keys []KEY_TYPE, current map[KEY_TYPE][]VALUE_TYPE) map[KEY_TYPE][]VALUE_TYPE {
	for _, key := range keys {
		_, ok := current[key]
		if !ok {
			current[key] = []VALUE_TYPE{}
		}
	}
	return current
}
