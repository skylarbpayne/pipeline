#pipeline
This is a library for quickly creating pipelines in Go. Often, your entire computation can be broken down into discrete steps that can operate concurrently. Furthermore, many of these steps can be executed
as multiple concurrent processes themselves to improve the speed of the computation. Pipelines are a great computational element to introduce in such a case. The pipeline library makes it easy to make a pipeline
and execute it!

```go
//Create a pipeline with 3 stages
pl := NewPipeline(3)

//Add a stage named split with 2 concurrent processes for the function 'Split'
pl.AddStage("split", 2, Split)

//If you want to abort from inside a stage, just close the done channel!
...
close(done)
...

//Get the resulting channel
res, err := pl.Execute(input)
```

You can look at the test file to see some examples of how to define your own pipeline.
I will probably update the readme with a map-reduce example after I fix a few interface
issues.
