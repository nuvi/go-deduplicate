package cache

type Getter[KEY_TYPE comparable, VALUE_TYPE any] func(KEY_TYPE) VALUE_TYPE

func WrapWithCache[KEY_TYPE comparable, VALUE_TYPE any](getter Getter[KEY_TYPE, VALUE_TYPE], cache Cache[KEY_TYPE, VALUE_TYPE]) Getter[KEY_TYPE, VALUE_TYPE] {
	return func(key KEY_TYPE) VALUE_TYPE {
		value, ok := cache.Get(key)
		if ok {
			return value
		}
		value = getter(key)
		cache.Set(key, value)
		return value
	}
}

type GetterWithError[KEY_TYPE comparable, VALUE_TYPE any] func(KEY_TYPE) (VALUE_TYPE, error)

func WrapWithCacheWithUncachedError[KEY_TYPE comparable, VALUE_TYPE any](getter GetterWithError[KEY_TYPE, VALUE_TYPE], cache Cache[KEY_TYPE, VALUE_TYPE]) GetterWithError[KEY_TYPE, VALUE_TYPE] {
	return func(key KEY_TYPE) (VALUE_TYPE, error) {
		value, ok := cache.Get(key)
		if ok {
			return value, nil
		}
		value, err := getter(key)
		if err == nil {
			cache.Set(key, value)
		}
		return value, err
	}
}
