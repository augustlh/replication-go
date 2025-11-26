---
> Some are born without a purpose.
> Other than prolonged demise in ornate ways.
> Most of the time it's pathetic, quick and useless.
> And good neighbours don't bother digging up the graves.

— How this project made us feel

Must start leader before replica to avoid undefined behaviour because of reasons.

Start leader like so
```
go run server/serverReplicator.go leader
```

Start replica like so
```
go run server/serverReplicator.go
```

Make a bid like so
```
go run client/client.go [USERNAME] [POSITIVE INTEGER]
```

Example
```
go run client/client.go bob 23
```

See current auction result like so
```
go run client/client.go result
```

