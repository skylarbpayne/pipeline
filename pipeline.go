package pipeline

/*
* TODO
*	2) Add a done channel to free resources should an error of sorts occur!
 */

import (
	"errors"
	"sync"
)

type Pipeline struct {
	NumStages   int
	stages      []*Stage
	nextStage   int
	numChannels int
}

type Stage struct {
	Name           string
	chanIndexBegin int
	numRoutines    int
	StageFunc      func(instream <-chan interface{}, outstream chan<- interface{})
}

func NewPipeline(numStages int) *Pipeline {
	if numStages < 1 {
		return nil
	}

	return &Pipeline{numStages, make([]*Stage, numStages), 0, 1}
}

func (pl *Pipeline) AddStage(name string, numRoutines int, fnc func(instream <-chan interface{}, outstream chan<- interface{})) error {
	if pl.nextStage >= pl.NumStages {
		return errors.New("AddStage: No Pipeline stages left!")
	} else if fnc == nil {
		return errors.New("AddStage: StageFunc is nil")
	} else if numRoutines < 1 {
		return errors.New("AddStage: numRoutines is less than 1!")
	}
	pl.stages[pl.nextStage] = &Stage{Name: name, chanIndexBegin: pl.numChannels, numRoutines: numRoutines, StageFunc: fnc}
	pl.nextStage++
	pl.numChannels += 2*numRoutines + 1
	return nil
}

func fanIn(inputs []chan interface{}, output chan<- interface{}) {
	var wg sync.WaitGroup

	out := func(c <-chan interface{}) {
		for n := range c {
			output <- n
		}
		wg.Done()
	}
	wg.Add(len(inputs))
	for _, c := range inputs {
		go out(c)
	}

	go func() {
		wg.Wait()
		close(output)
	}()
}

func fanOut(input <-chan interface{}, outputs []chan interface{}) {
	for i := range input {
		valSent := false
		for !valSent {
			for _, o := range outputs {
				select {
				case o <- i:
					valSent = true
				default:
				}
				if valSent {
					break
				}
			}
		}
	}
	for _, o := range outputs {
		close(o)
	}
}

func (pl *Pipeline) Execute(input interface{}) (<-chan interface{}, error) {
	pipes := make([]chan interface{}, pl.numChannels)
	for i := range pipes {
		pipes[i] = make(chan interface{})
	}

	wrapper := func(instream <-chan interface{}, outstream chan<- interface{}, stageFunc func(instream <-chan interface{}, outstream chan<- interface{})) {
		defer close(outstream)
		stageFunc(instream, outstream)
	}

	for _, stage := range pl.stages {
		//fan out
		begin := stage.chanIndexBegin
		numRouts := stage.numRoutines

		go fanOut(pipes[begin-1], pipes[begin:begin+numRouts])
		//run all routines of stage
		for i := begin; i < begin+numRouts; i++ {
			go wrapper(pipes[i], pipes[i+numRouts], stage.StageFunc)
		}
		//fan in
		go fanIn(pipes[begin+numRouts:begin+2*numRouts], pipes[begin+2*numRouts])
	}
	pipes[0] <- input
	close(pipes[0])
	return pipes[pl.numChannels-1], nil
}
