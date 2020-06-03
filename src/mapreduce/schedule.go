package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// 1. Retrieve worker from channel
	// 2. Signal worker to DoTask through RPC, if fail then put back into channel
	//

	var goroutineWaitGroup sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		goroutineWaitGroup.Add(1)
		go func(phase jobPhase, TaskNumber int, NumOtherPhase int) {
			// keep executing worker until succeed
			var worker string
			for {
				// retrieve worker from channel
				worker = <-registerChan
				var arg DoTaskArgs
				arg.Phase = phase
				arg.TaskNumber = TaskNumber
				arg.NumOtherPhase = NumOtherPhase
				arg.JobName = jobName
				arg.File = mapFiles[TaskNumber]
				reply := new(struct{})
				// use call() to pass RPC info
				ok := call(worker, "Worker.DoTask", &arg, &reply)
				// handle failure，if RPC fails，master assigns task to another worker
				// obtain another worker in the next iteration and assign task through RPC
				if ok {
					// Task succeeded, retrieve worker
					goroutineWaitGroup.Done()
					registerChan <- worker
					// Done() must be called before placing worker back into channel
					// channel could block when no goroutine is available
					return
				}
				// task failed
			}
		}(phase, i, n_other)
	}

	goroutineWaitGroup.Wait()

	fmt.Printf("Schedule: %v done\n", phase)
}
