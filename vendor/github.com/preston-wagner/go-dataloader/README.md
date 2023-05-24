# dataloader
Inspired by https://www.npmjs.com/package/dataloader, this is a generic utility to be used as part of your application's data fetching layer to allow naive calls to databases and other services without sacrificing performance via caching and batching.

## QueryBatcher usage
```go
func getUsers(userIds []string) (map[string]User, map[string]error){
  ...
}

const maxConcurrentBatches = 3
const maxBatchSize = 9999

batcher := NewQueryBatcher(getUsers, maxConcurrentBatches, maxBatchSize)

user, err := batcher.Load("user-id-0001")
```

QueryBatcher's name says it all: it represents a pool that limits the number and size of simultaneous requests a service can make to a resource like a database. When more requests come in at once than are allowed by `maxConcurrentBatches`, these excess requests will be added to a batch (with a size capped at `maxBatchSize`) which will all be queried at once as soon as a current request finishes.

## DataLoader usage
DataLoader is functionally the same as QueryBatcher, but with an added cache to prevent repeating calls after they've already been made.
