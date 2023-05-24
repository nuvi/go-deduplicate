# deduplicate
Sometimes, when building large distributed systems, you want an efficient way to ensure that expensive requests (such as to 3rd-party services) are only made once for a given piece of data. By coordinating work via a shared database and in-memory caching, this library allows the developer to only worry about implementing the actual request. 

## usage
```go
func getMediaAnalytics(url string) (Analytics, error){
  ...
}

const pendingTTL = time.Minute
const valueTTL = time.Hour * 24 * 7
const maxConcurrentBatches = 3
const maxBatchSize = 9999

db, _ := gorm.Open(postgres.Open(connectURL))

pool := NewTaskPool(db, getMediaAnalytics, pendingTTL, valueTTL, maxConcurrentBatches, maxBatchSize)

analytics, err := pool.Load("http://foo.bar/img.png")
```

## caveats
If you change the function signature of the "getter" function (including editing fields on the key/value types), you should also change the name of the function so that deduplicate doesn't try to load data using the old signature when starting the new pod.
