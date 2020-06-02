package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//

	// Overall flow:
	// 1. Open intermediate files created by map tasks
	// 2. Decode files with JSON format and merge data
	// 3. Generate new files with merged data
	// 4. Invoke reduceF on the merged results and write the sorted output into a file.

	keyvalue := make(map[string][]string)
	// store all keyvalue
	for i := 0; i < nMap; i++ {
		filename := reduceName(jobName, i, reduceTask)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Print(err)
			fmt.Printf("error opening file %s\n", filename)
			continue
		}
		fjson := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := fjson.Decode(&kv)
			if err != nil {
				break
				// All data decoded, break
			}
			if _, ok := keyvalue[kv.Key]; ok {
				// key already exists
				keyvalue[kv.Key] = append(keyvalue[kv.Key], kv.Value)
			} else {
				// key not in map yet
				newvalue := []string{kv.Value}
				keyvalue[kv.Key] = newvalue

			}
		}
		file.Close()
	}

	file, err := os.Create(mergeName(jobName, reduceTask))
	defer file.Close()
	if err != nil {
		fmt.Print(err)
		fmt.Printf("Failed to open merge file %s!\n", mergeName(jobName, reduceTask))
		return
	}

	keys := make([]string, 0, len(keyvalue))
	for k, _ := range keyvalue {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	enc := json.NewEncoder(file)
	for _, k := range keys {
		enc.Encode(KeyValue{k, reduceF(k, keyvalue[k])})
	}
}
