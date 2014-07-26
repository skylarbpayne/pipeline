#pipeline
This is a library for quickly creating pipelines in Go. Often, your entire computation can be broken down into discrete steps that can operate concurrently. Furthermore, many of these steps can be executed
as multiple concurrent processes themselves to improve the speed of the computation. Pipelines are a great computational element to introduce in such a case. The pipeline library makes it easy to make a pipeline
and execute it!

```go
//Create a pipeline with 3 stages
pl := NewPipeline(3)

//Add a stage named split with 2 concurrent processes for the function 'Split'
pl.AddStage("split", 2, Split)

//Get the resulting channel
res, err := pl.Execute(input)
```

You can look at the test file to see some examples of how to define your own pipeline.

Note: This is slightly unstable because if some type of error occurs downstream in the pipeline, there's no guarantee that the upstream resources won't deadlock. After changing the interface, this will be the number one
priority. It isn't the number one priority right now because the change to the interface will change how I have to implement this.
