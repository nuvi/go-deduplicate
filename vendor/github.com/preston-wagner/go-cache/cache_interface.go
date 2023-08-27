package cache

type Cache[KEY_TYPE comparable, VALUE_TYPE any] interface {
	Set(key KEY_TYPE, value VALUE_TYPE)
	Get(key KEY_TYPE) (VALUE_TYPE, bool)
	Remove(key KEY_TYPE)
}

type ReapedCache[KEY_TYPE comparable, VALUE_TYPE any] interface {
	Set(key KEY_TYPE, value VALUE_TYPE)
	Get(key KEY_TYPE) (VALUE_TYPE, bool)
	Remove(key KEY_TYPE)
	StartReaping()
	StopReaping()
	Reap()
}
