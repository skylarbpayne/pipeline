package pipeline

import (
	"testing"
)

func TestMakePipeline(t *testing.T) {
	pl := NewPipeline(1)

	if pl == nil {
		t.Error("Expected non-nil, got nil")
	}
}

func TestMakePipelineWithZeroStages(t *testing.T) {
	pl := NewPipeline(0)

	if pl != nil {
		t.Error("Expected nil, got ", pl)
	}
}

func TestAddStageToPipeline(t *testing.T) {
	pl := NewPipeline(1)
	err := pl.AddStage("test", func(instream <-chan interface{}, outstream chan<- interface{}) {
		for elem := range instream {
			arr := elem.([]int)
			for _, i := range arr {
				outstream <- i
			}
		}
		close(outstream)
	})

	if err != nil {
		t.Error("Error: ", err)
	}
}

func TestAddNilStage(t *testing.T) {
	pl := NewPipeline(1)
	if err := pl.AddStage("test", nil); err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestAddStageWithoutName(t *testing.T) {
	pl := NewPipeline(1)
	if err := pl.AddStage("", func(instream <-chan interface{}, outstream chan<- interface{}) {}); err != nil {
		t.Error("Expected nil, got ", err)
	}
}

func TestAddOneTooManyStages(t *testing.T) {
	pl := NewPipeline(1)
	if err := pl.AddStage("", func(instream <-chan interface{}, outstream chan<- interface{}) {}); err != nil {
		t.Error("Error: ", err)
	}

	if err := pl.AddStage("", func(instream <-chan interface{}, outstream chan<- interface{}) {}); err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestSimpleArraySplitPipeline(t *testing.T) {
	pl := NewPipeline(1)
	err := pl.AddStage("begin", func(instream <-chan interface{}, outstream chan<- interface{}) {
		defer close(outstream)
		for elem := range instream {
			arr := elem.([]int)
			for _, num := range arr {
				outstream <- num
			}
		}
	})
	if err != nil {
		t.Error("Error: ", err)
	}

	input := []int{1, 2, 3}
	res, err := pl.Execute(input)
	if err != nil {
		t.Error("Error: ", err)
	}
	var output []int
	for elem := range res {
		output = append(output, elem.(int))
	}
	for i := range input {
		if input[i] != output[i] {
			t.Errorf("Expected ", input, " got ", output)
		}
	}
}

func TestArraySplitAndSumPipeline(t *testing.T) {
	pl := NewPipeline(2)
	err := pl.AddStage("split", func(instream <-chan interface{}, outstream chan<- interface{}) {
		defer close(outstream)
		for elem := range instream {
			arr := elem.([]int)
			for _, num := range arr {
				outstream <- num
			}
		}
	})
	if err != nil {
		t.Error("Error: ", err)
	}

	err = pl.AddStage("sum", func(instream <-chan interface{}, outstream chan<- interface{}) {
		defer close(outstream)
		sum := 0
		for elem := range instream {
			sum += elem.(int)
		}
		outstream <- sum
	})
	if err != nil {
		t.Error("Error: ", err)
	}
	input := []int{1, 2, 3, 4}
	exp := 10
	res, err := pl.Execute(input)
	if err != nil {
		t.Error("Error: ", err)
	}
	output := <-res
	if output.(int) != exp {
		t.Error("Expected ", exp, " got ", output.(int))
	}
}

//Test concurrent stage(s)
//Fan in / Fan out
//How to make prettier (i.e. refactor phase?)
