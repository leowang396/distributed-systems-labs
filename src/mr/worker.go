package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		reply := CallGetTask()

		// NReduce == -1 is the "please exit" pseudo-task.
		if reply.NReduce == -1 {
			break
		}

		if reply.IsMap {
			// Case 1: Map task.

			// Gets the input file data.
			// Expects only 1 input file for map tasks.
			content := retrieveFileContent(reply.FileNames[0])

			kva := mapf(reply.FileNames[0], content)

			// Stores intermediate kv pairs to local files.
			reduceToKVMap := make(map[int][]KeyValue)
			reduceTaskNums := make(map[int]struct{})
			for _, kv := range kva {
				reduceTaskNumber := ihash(kv.Key) % reply.NReduce
				if _, isPresent := reduceToKVMap[reduceTaskNumber]; !isPresent {
					reduceToKVMap[reduceTaskNumber] = make([]KeyValue, 0)
				}
				reduceToKVMap[reduceTaskNumber] = append(reduceToKVMap[reduceTaskNumber], kv)
				if _, isPresent := reduceTaskNums[reduceTaskNumber]; !isPresent {
					reduceTaskNums[reduceTaskNumber] = struct{}{}
				}
			}

			for reduceTaskNum, kv := range reduceToKVMap {
				iFileName := fmt.Sprintf("mr-%d-%d", reply.TaskNumber, reduceTaskNum)
				produceIntermediateKv(kv, iFileName)
			}

			// Sends all intermediate files to coordinator in a batch to reduce RPC calls.
			CallPostIntermediate(reply.TaskNumber, reduceTaskNums)
		} else {
			// Case 2: Reduce task.

			// Gets and sorts the intermediate file data.
			var kva []KeyValue
			for _, fileName := range reply.FileNames {
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
			}

			sort.Sort(ByKey(kva))

			oname := fmt.Sprintf("mr-out-%v", reply.TaskNumber)
			produceFinalOutput(oname, kva, reducef)

			// Lets coordinator know that a reduce task is done.
			CallPostReduce(reply.TaskNumber)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
}

func CallPostReduce(reduceTaskNumber int) {
	args := PostReduceArgs{}
	args.ReduceTaskNumber = reduceTaskNumber
	reply := PostReduceReply{}
	ok := call("Coordinator.ReceiveFinal", &args, &reply)
	if ok {
		//fmt.Printf("Calling to signal that reduce task %v is completed\n", reduceTaskNumber)
	} else {
		fmt.Printf("CallPostReduce failed!\n")
	}
}

func produceFinalOutput(oname string, kva []KeyValue, reducef func(string, []string) string) {
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot get working directory")
	}
	file, err := os.CreateTemp(dir, "example.*.txt")
	if err != nil {
		log.Fatalf("cannot create temp file for %v", oname)
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			log.Fatalf("cannot close %v", oname)
		}
		//if err := os.Remove(file.Name()); err != nil {
		//	log.Fatalf("cannot remove %v %v", file.Name(), err.Error())
		//}
	}(file)

	// call Reduce on each distinct key in kva
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(file, "%v %v\n", kva[i].Key, output)
		if err != nil {
			log.Fatalf("cannot write to %v", file)
		}

		i = j
	}

	err = os.Rename(file.Name(), oname)
	if err != nil {
		log.Fatalf("cannot rename %v", oname)
	}
}

func retrieveFileContent(fileName string) string {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("cannot close %v", fileName)
		}
	}(file)

	return string(content)
}

// produceIntermediateKv - Stores Map function output as a local file.
func produceIntermediateKv(kva []KeyValue, iFileName string) {
	//file, err := os.OpenFile(iFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	dir, err := os.Getwd()
	if err != nil {
		log.Fatalf("cannot get working directory")
	}

	file, err := os.CreateTemp(dir, "example.*.txt")
	if err != nil {
		log.Fatalf("cannot create temp file for %v", iFileName)
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			log.Fatalf("cannot close %v", iFileName)
		}
		//if err := os.Remove(file.Name()); err != nil {
		//	log.Fatalf("cannot remove %v %v", file.Name(), err.Error())
		//}
	}(file)

	enc := json.NewEncoder(file)
	for _, kv := range kva {
		err = enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %s: %s", kv.Key, kv.Value)
		}
	}

	err = os.Rename(file.Name(), iFileName)
	if err != nil {
		log.Fatalf("cannot rename %v", iFileName)
	}
}

func CallPostIntermediate(mapTaskNumber int, reduceTaskNumbers map[int]struct{}) {
	args := PostIntermediateArgs{}
	args.MapTaskNumber = mapTaskNumber
	args.ReduceTaskNumbers = reduceTaskNumbers
	reply := PostIntermediateReply{}
	ok := call("Coordinator.ReceiveIntermediate", &args, &reply)
	if ok {
		//fmt.Printf("posted the intermediate file from map task %v to reduce task %v\n", mapTaskNumber, reduceTaskNumbers)
	} else {
		fmt.Printf("CallPostIntermediate failed!\n")
	}
}

// CallGetTask - RPC function asking for a task from the coordinator, which decides whether this is a map/reduce task.
func CallGetTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	ok := call("Coordinator.AllocateTask", &args, &reply)
	if ok {
		//fmt.Printf("got a task for files %v, a map task? %v\n", reply.FileNames, reply.IsMap)
	} else {
		fmt.Printf("CallGetTask failed!\n")
	}
	return reply
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	// the "Coordinator.Example" tells the
//	// receiving server that we'd like to call
//	// the Example() method of struct Coordinator.
//	ok := call("Coordinator.Example", &args, &reply)
//	if ok {
//		// reply.Y should be 100.
//		fmt.Printf("reply.Y %v\n", reply.Y)
//	} else {
//		fmt.Printf("call failed!\n")
//	}
//}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
