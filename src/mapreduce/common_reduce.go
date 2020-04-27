package mapreduce

import (
	//"hash/fnv"
	"os"
	"encoding/json"
	"sort"
	"fmt"
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
	// Your code here (Part I).
	//
	kvm := make(map[string][]string)

	for m := 0; m < nMap; m++ {
		infile_name := reduceName(jobName, m, reduceTask) // generate the file name appropriate for the specific file
		infile, err := os.Open(infile_name) // read it in. not sure if this is the right parser to use here
		defer infile.Close()
		if err != nil {
			panic(err)
			fmt.Println("opening issue")
		}

		dec := json.NewDecoder(infile)
		var keyv KeyValue

		for {

			err := dec.Decode(&keyv)
			if err != nil {
				break
			}

			if _, good := kvm[keyv.Key]; good {
				kvm[keyv.Key] = append(kvm[keyv.Key], keyv.Value)
			} else {
					kvm[keyv.Key] = []string{keyv.Value}
			}
		}
	}
	keys := make([]string, 0, len(kvm))
	for k := range kvm {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {return keys[i] < keys[j]})

	outFileHandle, err := os.Create(outFile)
	if err != nil {
		panic(err)
		fmt.Println("opening output issue")
	}
	defer outFileHandle.Close()
	enc := json.NewEncoder(outFileHandle)

	for _, key := range keys {
		enc.Encode(KeyValue{key, reduceF(key, kvm[key])})
	}
}
	//variable := string(infile)
	//fmt.Println(variable) // print the readin file in string format
	//lm := len(infile)
//
// 	var vals [nmap]string
// 	for f:= 0; f < lm; f++ {
//
// 		}
//
// 		type Message struct {
// 			Key, Value
// 		}
//
// 		for {
// 			var m Message
// 			if err := dec.Decode(&m); err == io.EOF {
// 				break
// 			} else if err != nil {
// 				log.Fatal(err)
// 			}
// 			fmt.Printf("%v: %v\n", m.Key, m.Value)
// 		}
// 	}
//
// 	 	// need to sort files by key
//
// 		//result := reduceF(inFile, variable)
//
// 		//dec := json.NewDecoder(infile) // taking read data and decoding it from JSON
// 		//dencerr := dec.Decode(&kv) // ^^.Decode(&kv)
//
// }
