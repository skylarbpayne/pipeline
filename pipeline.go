package pipeline

import "errors"

type Pipeline struct {
	NumStages int
	stages    []*Stage
	nextStage int
}

type Stage struct {
	Name      string
	StageFunc func(instream <-chan interface{}, outstream chan<- interface{})
}

func NewPipeline(numStages int) *Pipeline {
	if numStages < 1 {
		return nil
	}

	return &Pipeline{numStages, make([]*Stage, numStages), 0}
}

func (pl *Pipeline) AddStage(name string, fnc func(instream <-chan interface{}, outstream chan<- interface{})) error {
	if pl.nextStage >= pl.NumStages {
		return errors.New("AddStage: No Pipeline stages left!")
	} else if fnc == nil {
		return errors.New("AddStage: StageFunc is nil")
	}
	pl.stages[pl.nextStage] = &Stage{Name: name, StageFunc: fnc}
	pl.nextStage++
	return nil
}

func (pl *Pipeline) Execute(input interface{}) (<-chan interface{}, error) {
	pipes := make([]chan interface{}, pl.NumStages+1)
	for i := range pipes {
		pipes[i] = make(chan interface{})
	}
	for i, stage := range pl.stages {
		go stage.StageFunc(pipes[i], pipes[i+1])
	}
	pipes[0] <- input
	close(pipes[0])
	return pipes[pl.NumStages], nil
}
