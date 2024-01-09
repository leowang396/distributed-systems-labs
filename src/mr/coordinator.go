package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	inputFiles          []string
	nReduce             int
	mapTaskToHandOut    map[int]struct{}
	reduceTaskToHandOut map[int]struct{}
	ongoingMapTasks     map[int]struct{}
	ongoingReduceTasks  map[int]struct{}
	reduceToMapTaskMap  map[int]map[int]struct{}
	mu                  *sync.Mutex
	mapperCanProceed    *sync.Cond
	reducerCanProceed   *sync.Cond
}

const (
	timeToWait = 10
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}

// The RPC handler that allocates a map task to a worker.
func (c *Coordinator) AllocateTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If there are remaining map tasks, keep handing them out.
	//fmt.Printf("AllocateTask called. map tasks remaining: %v, reduce tasks remaining: %v\n", len(c.mapTaskToHandOut), len(c.reduceTaskToHandOut))

	// If all maps handed out, proceed if some tasks failed or if all have completed.
	for len(c.mapTaskToHandOut) == 0 && len(c.ongoingMapTasks) != 0 {
		c.mapperCanProceed.Wait()
	}

	if len(c.mapTaskToHandOut) > 0 {
		reply.IsMap = true
		for mTaskNum := range c.mapTaskToHandOut {
			reply.TaskNumber = mTaskNum
			delete(c.mapTaskToHandOut, mTaskNum)
			break
		}
		reply.FileNames = []string{c.inputFiles[reply.TaskNumber]}
		reply.NReduce = c.nReduce

		// Keep track of the active map workers.
		c.ongoingMapTasks[reply.TaskNumber] = struct{}{}
		go c.monitorTaskProgress(reply.TaskNumber, true)

		return nil
	}

	// If all reduces handed out, proceed only if some tasks failed or if all have completed.
	for len(c.reduceTaskToHandOut) == 0 && len(c.ongoingReduceTasks) != 0 {
		c.reducerCanProceed.Wait()
	}

	// Handles reduce task coordination
	if len(c.reduceTaskToHandOut) > 0 {
		reply.IsMap = false
		for rTaskNum := range c.reduceTaskToHandOut {
			reply.TaskNumber = rTaskNum
			delete(c.reduceTaskToHandOut, rTaskNum)
			break
		}
		reply.FileNames = make([]string, 0)
		for correspondingMTaskNum := range c.reduceToMapTaskMap[reply.TaskNumber] {
			iFileName := fmt.Sprintf("mr-%v-%v", correspondingMTaskNum, reply.TaskNumber)
			reply.FileNames = append(reply.FileNames, iFileName)
		}
		reply.NReduce = c.nReduce
		//fmt.Printf("Reduce task handed out for files %v\n", reply.FileNames)

		// Keeps track of the active reduce workers.
		c.ongoingReduceTasks[reply.TaskNumber] = struct{}{}
		go c.monitorTaskProgress(reply.TaskNumber, false)
	} else {
		// If all reduce tasks have also completed, tell workers to exit.
		reply.NReduce = -1
	}

	return nil
}

func (c *Coordinator) monitorTaskProgress(taskNumber int, isMap bool) {
	time.Sleep(timeToWait * time.Second)

	c.mu.Lock()
	defer c.mu.Unlock()
	//fmt.Printf("10 seconds up\n")
	if _, isOngoing := c.ongoingMapTasks[taskNumber]; isMap && isOngoing {
		//fmt.Printf("Putting map task %v back into the set\n", taskNumber)
		c.mapTaskToHandOut[taskNumber] = struct{}{}
		c.mapperCanProceed.Broadcast()
	} else if _, isOngoing = c.ongoingReduceTasks[taskNumber]; !isMap && isOngoing {
		//fmt.Printf("Putting reduce task %v back into the set\n", taskNumber)
		c.reduceTaskToHandOut[taskNumber] = struct{}{}
		c.reducerCanProceed.Broadcast()
	}
}

func (c *Coordinator) ReceiveIntermediate(args *PostIntermediateArgs, reply *PostIntermediateReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Case 1: Coordinator is not expecting further responses for this map task.
	_, isOngoing := c.ongoingMapTasks[args.MapTaskNumber]
	if !isOngoing {
		return nil
	}

	// Case 2: Coordinator is expecting further responses for this map task.
	// Keeps track of intermediate file names to reducer mapping; received data looks like map 3 -> reduce 1, 2, 5
	for reduceTaskNum := range args.ReduceTaskNumbers {
		if _, temp := c.reduceToMapTaskMap[reduceTaskNum]; !temp {
			c.reduceToMapTaskMap[reduceTaskNum] = make(map[int]struct{})
		}

		// Avoids duplicates of mapping records.
		if _, temp := c.reduceToMapTaskMap[reduceTaskNum][args.MapTaskNumber]; !temp {
			c.reduceToMapTaskMap[reduceTaskNum][args.MapTaskNumber] = struct{}{}
		}
	}

	// Removes the completed map task from the trackers.
	//fmt.Printf("Removing map task %v from the set\n", args.MapTaskNumber)
	delete(c.ongoingMapTasks, args.MapTaskNumber)
	delete(c.mapTaskToHandOut, args.MapTaskNumber)
	if len(c.ongoingMapTasks) == 0 {
		c.mapperCanProceed.Broadcast()
	}

	return nil
}

func (c *Coordinator) ReceiveFinal(args *PostReduceArgs, reply *PostReduceReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Case 1: Coordinator is not expecting further responses for this reduce task.
	if _, isOngoing := c.ongoingReduceTasks[args.ReduceTaskNumber]; !isOngoing {
		return nil
	}

	// Case 2: Coordinator is expecting further responses for this map task.
	// Removes the completed map task from the tracker.
	//fmt.Printf("Removing reduce task %v back into the set\n", args.ReduceTaskNumber)
	delete(c.ongoingReduceTasks, args.ReduceTaskNumber)
	delete(c.reduceTaskToHandOut, args.ReduceTaskNumber)
	if len(c.ongoingReduceTasks) == 0 {
		c.reducerCanProceed.Broadcast()
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	// Your code here.
	if len(c.reduceTaskToHandOut) == 0 && len(c.ongoingReduceTasks) == 0 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	mapTaskToHandOut := make(map[int]struct{})
	reduceTaskToHandOut := make(map[int]struct{})
	for i := 0; i < len(files); i++ {
		mapTaskToHandOut[i] = struct{}{}
	}
	for i := 0; i < nReduce; i++ {
		reduceTaskToHandOut[i] = struct{}{}
	}
	var mu sync.Mutex
	c := Coordinator{inputFiles: files, nReduce: nReduce, mu: &mu,
		mapperCanProceed: sync.NewCond(&mu), reducerCanProceed: sync.NewCond(&mu),
		mapTaskToHandOut: mapTaskToHandOut, reduceTaskToHandOut: reduceTaskToHandOut,
		ongoingMapTasks: make(map[int]struct{}), ongoingReduceTasks: make(map[int]struct{}),
		reduceToMapTaskMap: make(map[int]map[int]struct{})}

	c.server()
	return &c
}
