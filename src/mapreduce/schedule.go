package mapreduce

import (
	"fmt"
	// "log"
	"sync"
	// "time"
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
	//
	// Your code here (Part 2, 2B).
	//

	var wg sync.WaitGroup

	var tasks []DoTaskArgs

	for i := 0; i < ntasks; i++ {
		workerFile := mapFiles[i]
		todo := DoTaskArgs{jobName, workerFile, phase, i, n_other}
		tasks = append(tasks, todo)
	}

	wg.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		go func(ind int){
			for{
				wrpc := <- registerChan
				done := call(wrpc, "Worker.DoTask", tasks[ind], nil)
				if done {
					wg.Done()
					registerChan <- wrpc
					break
				}
			}
		}(i)
	}


	wg.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}
