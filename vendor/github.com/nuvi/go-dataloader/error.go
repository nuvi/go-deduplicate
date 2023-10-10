package dataloader

// reduces boilerplate for getters that want to respond to all keys with a shared error (such as database failure)
func ErrForAll[KEY_TYPE comparable](keys []KEY_TYPE, err error) map[KEY_TYPE]error {
	errs := map[KEY_TYPE]error{}
	for _, key := range keys {
		errs[key] = err
	}
	return errs
}
