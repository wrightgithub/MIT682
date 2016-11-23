package mapreduce

import (
	"fmt"

	"sync"
)


// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	taskChan:=make(chan int,ntasks)
	go func() {
		for i := 0;i<ntasks ; i++ {
			taskChan<-i
		}
	}()

	for {
		taskNumber,more:=<-taskChan
		if !more{
			break
		}
		worker:=<-mr.registerChannel
		// 注意这些变量都是信道共享的
		go func(w string) {
			var mutex sync.Mutex
			var args =new(DoTaskArgs)
			args.JobName = mr.jobName;
			args.Phase = phase
			args.NumOtherPhase = nios
			args.File = mr.files[taskNumber]
			args.TaskNumber = taskNumber

			ok := call(w, "Worker.DoTask", args, new(struct{}))
			if !ok {
				taskChan<-args.TaskNumber;
			}else {
				mutex.Lock()
				ntasks--
				mutex.Unlock()
				if ntasks==0 {
					close(taskChan)
				}
			}
			mr.registerChannel<-w
		}(worker)

	}


	fmt.Printf("Schedule: %v phase done\n", phase)
}

