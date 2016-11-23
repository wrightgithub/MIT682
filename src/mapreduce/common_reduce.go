package mapreduce

import (
	"os"
	"log"
	"encoding/json"
	"sort"
	"fmt"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
jobName string, // the name of the whole MapReduce job
reduceTaskNumber int, // which reduce task this is
nMap int, // the number of map tasks that were run ("M" in the paper)
reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()


	/**
	1.读intermediate files
	2.排序
	    为什么要排序? 为了方便有些任务需要排序后的数据，因此提供了排序功能。
	3.调用reduceF
	4.writes the output to disk.
	 */

	fmt.Println("doReduce")
	keys := []string{}
	var line KeyValue
	reduceMap := map[string][]string{}
	for i := 0; i < nMap; i++ {
		reduceName := reduceName(jobName, i, reduceTaskNumber);
		f, err := os.Open(reduceName);
		if err != nil {
			log.Fatal("open file error")
		}
		dec := json.NewDecoder(f)
		for {
			if err := dec.Decode(&line); err == nil {
				reduceMap[line.Key] = append(reduceMap[line.Key], line.Value)
				continue
			}
			break
		}
		f.Close()
	}

	// fmt.Println("reduceMap=", reduceMap)
	for key := range reduceMap {
		keys = append(keys, key)
	}

	//sort
	sort.Strings(keys)

	mergeFile := mergeName(jobName, reduceTaskNumber)
	f := openFile(mergeFile)
	enc := json.NewEncoder(f)

	for _, key := range keys {
		enc.Encode(KeyValue{Key:key, Value:reduceF(key, reduceMap[key])})
	}

	f.Close()

}
