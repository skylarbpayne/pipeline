package pipeline

import "testing"

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
	err := pl.AddStage("test", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {
		for elem := range instream {
			arr := elem.([]int)
			for _, i := range arr {
				outstream <- i
			}
		}
	})

	if err != nil {
		t.Error("Error: ", err)
	}
}

func TestAddNilStage(t *testing.T) {
	pl := NewPipeline(1)
	if err := pl.AddStage("test", 1, nil); err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestAddStageWithoutName(t *testing.T) {
	pl := NewPipeline(1)
	if err := pl.AddStage("", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {}); err != nil {
		t.Error("Expected nil, got ", err)
	}
}

func TestAddOneTooManyStages(t *testing.T) {
	pl := NewPipeline(1)
	if err := pl.AddStage("", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {}); err != nil {
		t.Error("Error: ", err)
	}

	if err := pl.AddStage("", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {}); err == nil {
		t.Error("Expected error, got nil")
	}
}

func TestSimpleArraySplitPipeline(t *testing.T) {
	pl := NewPipeline(1)
	err := pl.AddStage("begin", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {
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
	if len(input) != len(output) {
		t.Error("Input: ", input, ", Output: ", output)
	}
	for i := range input {
		if input[i] != output[i] {
			t.Errorf("Expected ", input, " got ", output)
		}
	}
}

func TestArraySplitAndSumPipeline(t *testing.T) {
	pl := NewPipeline(2)
	err := pl.AddStage("split", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {
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

	err = pl.AddStage("sum", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {
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

func TestConcurrentArraySplitAndSumPipeline(t *testing.T) {
	pl := NewPipeline(3)
	err := pl.AddStage("split", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {
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

	err = pl.AddStage("sum", 2, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {
		sum := 0
		for elem := range instream {
			sum += elem.(int)
		}
		outstream <- sum
	})
	if err != nil {
		t.Error("Error: ", err)
	}

	err = pl.AddStage("sum2", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {
		sum := 0
		for elem := range instream {
			sum += elem.(int)
		}
		outstream <- sum
	})
	if err != nil {
		t.Error("Error: ", err)
	}

	input := []int{1, 2, 3, 4, 5, 6, 7, 8}
	exp := 36
	res, err := pl.Execute(input)
	if err != nil {
		t.Error("Error: ", err)
	}
	output := <-res
	if output.(int) != exp {
		t.Error("Expected ", exp, " got ", output.(int))
	}
}

func TestCancelledStage(t *testing.T) {
	pl := NewPipeline(2)
	defer pl.Cleanup()

	pl.AddStage("test1", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {
		close(done)
		for n := range instream {
			select {
			case outstream <- n:
			case <-done:
				return
			}
		}
	})

	pl.AddStage("test2", 1, func(instream <-chan interface{}, outstream chan<- interface{}, done chan int) {
		for n := range instream {
			select {
			case outstream <- n:
			case <-done:
				return
			}
		}
	})

	res, _ := pl.Execute(0, 1, 2, 3, 4, 5, 6, 7, 8)

	if res != nil {
		t.Error("Expected nil result!")
	}
}
