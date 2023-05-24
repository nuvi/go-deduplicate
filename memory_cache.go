package deduplicate

import (
	"sync"
	"time"

	"github.com/preston-wagner/unicycle"
)

type cacheEntry[VALUE_TYPE any] struct {
	createdAt time.Time
	value     VALUE_TYPE
}

func (ce *cacheEntry[VALUE_TYPE]) isExpired(ttl time.Duration) bool {
	return ce.createdAt.Before(time.Now().Add(-ttl))
}

type expiringMemoryCache[KEY_TYPE comparable, VALUE_TYPE any] struct {
	cache     map[KEY_TYPE]cacheEntry[VALUE_TYPE]
	lock      *sync.RWMutex
	ttl       time.Duration
	canceller func()
}

func newMemoryCache[KEY_TYPE comparable, VALUE_TYPE any](ttl time.Duration) *expiringMemoryCache[KEY_TYPE, VALUE_TYPE] {
	toReturn := expiringMemoryCache[KEY_TYPE, VALUE_TYPE]{
		cache: map[KEY_TYPE]cacheEntry[VALUE_TYPE]{},
		lock:  &sync.RWMutex{},
		ttl:   ttl,
	}

	toReturn.canceller = unicycle.Repeat(toReturn.reap, ttl/4, false)

	return &toReturn
}

func (emc *expiringMemoryCache[KEY_TYPE, VALUE_TYPE]) Add(key KEY_TYPE, value VALUE_TYPE) {
	emc.lock.Lock()
	defer emc.lock.Unlock()
	emc.cache[key] = cacheEntry[VALUE_TYPE]{
		value:     value,
		createdAt: time.Now(),
	}
}

func (emc *expiringMemoryCache[KEY_TYPE, VALUE_TYPE]) rawGet(key KEY_TYPE) (cacheEntry[VALUE_TYPE], bool) {
	emc.lock.RLock()
	defer emc.lock.RUnlock()
	cached, ok := emc.cache[key]
	return cached, ok
}

func (emc *expiringMemoryCache[KEY_TYPE, VALUE_TYPE]) Get(key KEY_TYPE) (VALUE_TYPE, bool) {
	cached, ok := emc.rawGet(key)
	if ok {
		if cached.isExpired(emc.ttl) {
			ok = false
			go emc.remove(key)
		}
	}
	return cached.value, ok
}

func (emc *expiringMemoryCache[KEY_TYPE, VALUE_TYPE]) remove(key KEY_TYPE) {
	emc.lock.Lock()
	defer emc.lock.Unlock()
	delete(emc.cache, key)
}

func (emc *expiringMemoryCache[KEY_TYPE, VALUE_TYPE]) getAllKeys() []KEY_TYPE {
	emc.lock.RLock()
	defer emc.lock.RUnlock()
	return unicycle.Keys(emc.cache)
}

func (emc *expiringMemoryCache[KEY_TYPE, VALUE_TYPE]) reap() {
	for _, key := range emc.getAllKeys() {
		cached, ok := emc.rawGet(key)
		if ok {
			if cached.isExpired(emc.ttl) {
				emc.remove(key)
			}
		}
	}
}

func (emc *expiringMemoryCache[KEY_TYPE, VALUE_TYPE]) Close() {
	emc.canceller()
}
