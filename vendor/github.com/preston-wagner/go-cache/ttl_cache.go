package cache

import (
	"sync"
	"time"

	"github.com/preston-wagner/unicycle"
)

type ttlCacheValue[VALUE_TYPE any] struct {
	createdAt time.Time
	value     VALUE_TYPE
}

func (cached *ttlCacheValue[VALUE_TYPE]) isExpired(ttl time.Duration) bool {
	return cached.createdAt.Before(time.Now().Add(-ttl))
}

// a cache that stores values for a fixed amount of time
type TTLCache[KEY_TYPE comparable, VALUE_TYPE any] struct {
	cache         map[KEY_TYPE]ttlCacheValue[VALUE_TYPE]
	ttl           time.Duration
	reapFrequency time.Duration
	lock          *sync.RWMutex
	canceller     func()
}

func NewTTLCache[KEY_TYPE comparable, VALUE_TYPE any](ttl, reapFrequency time.Duration) *TTLCache[KEY_TYPE, VALUE_TYPE] {
	cache := &TTLCache[KEY_TYPE, VALUE_TYPE]{
		cache:         map[KEY_TYPE]ttlCacheValue[VALUE_TYPE]{},
		ttl:           ttl,
		reapFrequency: reapFrequency,
		lock:          &sync.RWMutex{},
	}

	cache.StartReaping()

	return cache
}

func (ttlCache *TTLCache[KEY_TYPE, VALUE_TYPE]) Set(key KEY_TYPE, value VALUE_TYPE) {
	ttlCache.lock.Lock()
	defer ttlCache.lock.Unlock()
	ttlCache.cache[key] = ttlCacheValue[VALUE_TYPE]{
		value:     value,
		createdAt: time.Now(),
	}
}

func (ttlCache *TTLCache[KEY_TYPE, VALUE_TYPE]) rawGet(key KEY_TYPE) (ttlCacheValue[VALUE_TYPE], bool) {
	ttlCache.lock.RLock()
	defer ttlCache.lock.RUnlock()
	cached, ok := ttlCache.cache[key]
	return cached, ok
}

func (ttlCache *TTLCache[KEY_TYPE, VALUE_TYPE]) Get(key KEY_TYPE) (VALUE_TYPE, bool) {
	cached, ok := ttlCache.rawGet(key)
	if ok {
		if cached.isExpired(ttlCache.ttl) {
			ok = false
			go ttlCache.Remove(key)
		}
	}
	return cached.value, ok
}

func (ttlCache *TTLCache[KEY_TYPE, VALUE_TYPE]) Remove(key KEY_TYPE) {
	ttlCache.lock.Lock()
	defer ttlCache.lock.Unlock()
	delete(ttlCache.cache, key)
}

func (ttlCache *TTLCache[KEY_TYPE, VALUE_TYPE]) StartReaping() {
	ttlCache.canceller = unicycle.Repeat(ttlCache.Reap, ttlCache.reapFrequency, false)
}

func (ttlCache *TTLCache[KEY_TYPE, VALUE_TYPE]) StopReaping() {
	ttlCache.canceller()
}

func (ttlCache *TTLCache[KEY_TYPE, VALUE_TYPE]) Reap() {
	ttlCache.lock.Lock()
	defer ttlCache.lock.Unlock()
	for key, cached := range ttlCache.cache {
		if cached.isExpired(ttlCache.ttl) {
			// Note: deleting from a map doesn't shrink the underlying array, so while memory can be re-used it is not freed
			delete(ttlCache.cache, key)
		}
	}
}
