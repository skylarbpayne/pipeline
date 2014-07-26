package pipeline

import (
	"errors"
	"sync"
)

type Pipeline struct {
	NumStages   int
	stages      []*Stage
	nextStage   int
	numChannels int
	done        chan int
}

type Stage struct {
	Name           string
	chanIndexBegin int
	numRoutines    int
	StageFunc      func(instream <-chan interface{}, outstream chan<- interface{}, done chan int)
}

func NewPipeline(numStages int) *Pipeline {
	if numStages < 1 {
		return nil
	}

	return &Pipeline{numStages, make([]*Stage, numStages), 0, 1, make(chan int)}
}

func (pl *Pipeline) Cleanup() {
	opened := true
	select {
	case _, opened = <-pl.done:
	default:
	}
	if opened {
		close(pl.done)
	}
}

func (pl *Pipeline) AddStage(name string, numRoutines int, fnc func(instream <-chan interface{}, outstream chan<- interface{}, done chan int)) error {
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

func fanIn(inputs []chan interface{}, output chan<- interface{}, done <-chan int) {
	var wg sync.WaitGroup

	out := func(c <-chan interface{}) {
		defer wg.Done()
		for n := range c {
			select {
			case output <- n:
			case <-done:
				return
			}
		}
	}

	wg.Add(len(inputs))
	for _, c := range inputs {
		go out(c)
	}

	go func() {
		defer close(output)
		wg.Wait()
	}()
}

func fanOut(input <-chan interface{}, outputs []chan interface{}, done <-chan int) {
	for i := range input {
		valSent := false
		for !valSent {
			for _, o := range outputs {
				select {
				case o <- i:
					valSent = true
				case <-done:
					return
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

func (pl *Pipeline) Execute(input ...interface{}) (<-chan interface{}, error) {
	pipes := make([]chan interface{}, pl.numChannels)
	for i := range pipes {
		pipes[i] = make(chan interface{})
	}

	wrapper := func(instream <-chan interface{}, outstream chan<- interface{}, stageFunc func(instream <-chan interface{}, outstream chan<- interface{}, done chan int)) {
		defer close(outstream)
		stageFunc(instream, outstream, pl.done)
	}

	for _, stage := range pl.stages {
		//fan out
		begin := stage.chanIndexBegin
		numRouts := stage.numRoutines

		go fanOut(pipes[begin-1], pipes[begin:begin+numRouts], pl.done)
		//run all routines of stage
		for i := begin; i < begin+numRouts; i++ {
			go wrapper(pipes[i], pipes[i+numRouts], stage.StageFunc)
		}
		//fan in
		go fanIn(pipes[begin+numRouts:begin+2*numRouts], pipes[begin+2*numRouts], pl.done)
	}
	for _, i := range input {
		select {
		case pipes[0] <- i:
		case <-pl.done:
			return nil, errors.New("Done channel closed. Exiting.")
		}
	}
	close(pipes[0])
	return pipes[pl.numChannels-1], nil
}
