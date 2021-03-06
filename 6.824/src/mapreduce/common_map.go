package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"log"
	"os"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// version 1 2的输出类型
type MapOutPutType struct {
	Key   string
	Value []string
}

func doMap(
	jobName string, // the name of the MapReduce job
	mapTask int,    // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(filename string, contents string) []KeyValue,
) {
	//
	// doMap manages one map task: it should read one of the input files
	// (inFile), call the user-defined map function (mapF) for that file's
	// contents, and partition mapF's output into nReduce intermediate files.
	//
	// There is one intermediate file per reduce task. The file name
	// includes both the map task number and the reduce task number. Use
	// the filename generated by reduceName(jobName, mapTask, r)
	// as the intermediate file for reduce task r. Call ihash() (see
	// below) on each key, mod nReduce, to pick r for a key/value pair.
	//
	// mapF() is the map function provided by the application. The first
	// argument should be the input file name, though the map function
	// typically ignores it. The second argument should be the entire
	// input file contents. mapF() returns a slice containing the
	// key/value pairs for reduce; see common.go for the definition of
	// KeyValue.
	//
	// Look at Go's ioutil and os packages for functions to read
	// and write files.
	//
	// Coming up with a scheme for how to format the key/value pairs on
	// disk can be tricky, especially when taking into account that both
	// keys and values could contain newlines, quotes, and any other
	// character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	//
	// Your code here (Part I).
	//

	//读取输入文件，执行map函数
	fileStream, err := os.Open(inFile)
	if err != nil {
		log.Fatal("open file error in doMap")
		return
	}
	defer fileStream.Close()
	fileContent, err := ioutil.ReadAll(fileStream)
	if err != nil {
		log.Fatal("read file error in doMap")
		return
	}
	mapOutput := mapF(inFile, string((fileContent)))
	// 生成nReduce个输入文件流
	files := make([]*os.File, 0, nReduce)
	enc := make([]*json.Encoder, 0, nReduce)
	for r := 0; r < nReduce; r++ {
		filename := reduceName(jobName, mapTask, r)
		mapOutputFileStream, err := os.Create(filename)
		if err != nil {
			log.Fatal("doMap Create: ", err)
			return
		}
		files = append(files, mapOutputFileStream)
		enc = append(enc, json.NewEncoder(mapOutputFileStream))
	}
	/*
		// version1: 使用sort后进行聚集
		// 将map阶段产生的输出按key进行排序并合并key值相同的value，然后写入文件
		sort.Sort(ByKey(mapOutput))
		outputLength := len(mapOutput)
		i := 0
		for i < outputLength {
			j := i + 1
			for j < outputLength && mapOutput[j].Key == mapOutput[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, mapOutput[k].Value)
			}
			reduceID := ihash(mapOutput[i].Key) % nReduce
			enc[reduceID].Encode(MapOutPutType{mapOutput[i].Key, values})
			i = j
		}

		// version2: 使用map数据结构进行聚集
		mapData := make(map[string][] string)
		for _, kv := range mapOutput {
			mapData[kv.Key] = append(mapData[kv.Key], kv.Value)
		}
		for k, v := range mapData {
			reduceID := ihash(k) % nReduce
			enc[reduceID].Encode(MapOutPutType{k, v})
		}
	*/
	// version3:不进行聚集，直接写入文件
	for _,kv := range mapOutput{
		reduceID := ihash(kv.Key) % nReduce
		enc[reduceID].Encode(kv)
	}
	for _, f := range files {
		f.Close()
	}


}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}
