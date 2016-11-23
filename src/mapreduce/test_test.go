package mapreduce

import (
	"fmt"
	"testing"
	"time"

	"bufio"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"encoding/json"
)

const (
	nNumber = 100000
	nMap = 100
	nReduce = 50
)

// Create input file with N numbers
// Check if we have N numbers in output file

// Split in words  切分文件产生乱序的媒介key / values
func MapFunc(file string, value string) (res []KeyValue) {
	debug("Map %v\n", value)
	words := strings.Fields(value)
	for _, w := range words {
		kv := KeyValue{w, ""}
		res = append(res, kv)
	}
	return
}


// 应该是把shuffle好的文件拿来用
// Just return key
func ReduceFunc(key string, values []string) string {
	for _, e := range values {
		debug("Reduce %s %v\n", key, e)
	}
	return ""
}

// Checks input file agaist output file: each input number should show up
// in the output file in string sorted order
func check(t *testing.T, files []string) {
	output, err := os.Open("mrtmp.test")
	if err != nil {
		log.Fatal("check: ", err)
	}
	defer output.Close()

	var lines []string
	for _, f := range files {
		input, err := os.Open(f)
		if err != nil {
			log.Fatal("check: ", err)
		}
		defer input.Close()
		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i++
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// Workers report back how many RPCs they have processed in the Shutdown reply.
// Check that they processed at least 1 RPC.
func checkWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		if tasks == 0 {
			t.Fatalf("Some worker didn't do any work\n")
		}
	}
}

// Make input file
func makeInputs(num int) []string {
	var names []string
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("824-mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			log.Fatal("mkInput: ", err)
		}
		w := bufio.NewWriter(file)
		for i < (f + 1) * (nNumber / num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return names
}
// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp. can't use current directory since
// AFS doesn't support UNIX-domain sockets.
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}

func setup() *Master {
	files := makeInputs(nMap)
	master := port("master")
	mr := Distributed("test", files, nReduce, master)
	return mr
}

func cleanup(mr *Master) {
	mr.CleanupFiles()
	for _, f := range mr.files {
		removeFile(f)
	}
}

func TestSequentialSingle(t *testing.T) {
	mr := Sequential("test", makeInputs(1), 1, MapFunc, ReduceFunc)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestSequentialMany(t *testing.T) {
	mr := Sequential("test", makeInputs(5), 3, MapFunc, ReduceFunc)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestBasic(t *testing.T) {
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, port("worker" + strconv.Itoa(i)),
			MapFunc, ReduceFunc, -1)
	}
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestOneFailure(t *testing.T) {
	mr := setup()
	// Start 2 workers that fail after 10 tasks
	go RunWorker(mr.address, port("worker" + strconv.Itoa(0)),
		MapFunc, ReduceFunc, 10)
	go RunWorker(mr.address, port("worker" + strconv.Itoa(1)),
		MapFunc, ReduceFunc, -1)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestManyFailures(t *testing.T) {
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.doneChannel:
			check(t, mr.files)
			cleanup(mr)
			break
		default:
		// Start 2 workers each sec. The workers fail after 10 tasks
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 10)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFunc, ReduceFunc, 10)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}


// after this is my test
func TestGgg(t *testing.T) {
	fmt.Println("dd")
}

func TestHash(t *testing.T) {
	print(ihash("ddd"))
}

func TestKv1(t *testing.T) {
	_, err := os.Open("jsontest")
	if err != nil {
		log.Fatal("error")
	}
}
func TestKv2(t *testing.T) {
	values1 := "a"
	values2 := "b"

	kvs := []KeyValue{
		{"1",values1},
		{"2",values2},
	}

	f := openFile("jsontest")

	enc := json.NewEncoder(f)
//	enc.Encode(&kvs)
	for _,kv := range kvs {
		fmt.Println(kv)
		enc.Encode(&kv)
	}

	f.Close();
	/////////////////////////////////////
	f, err := os.Open("jsontest")
	if err != nil {
		log.Fatal("open file error")
	}
	dec := json.NewDecoder(f)
	var v map[string]string
	//for
	{
		if err := dec.Decode(&v); err != nil {
			log.Println("done")
			return
		}

		for k,v:=range v{

			fmt.Printf("key=%s,value=%s\n", k, v)
		}
	}

}

func TestKv(t *testing.T) {
	values1 := []string{"a", "b", "c"}
	values2 := []string{"e", "f", "g"}

	values1=append(values1,"ee")
	kvs := map[string][]string{
		"1":values1,
		"2":values2,
		"5":values1,
	}

	kvs["5"]=append(kvs["5"],"rr")
	if kvs["5"]!=nil{
		print(kvs["5"])
	}

	f := openFile("jsontest")

	enc := json.NewEncoder(f)
	enc.Encode(&kvs)
	//for k,v := range kvs {
	//	fmt.Println(kv)
	//	enc.Encode(&kv)
	//}

	f.Close();
	/////////////////////////////////////
	f, err := os.Open("jsontest")
	if err != nil {
		log.Fatal("open file error")
	}
	dec := json.NewDecoder(f)
	var v map[string][]string
	//for
	{
		if err := dec.Decode(&v); err != nil {
			log.Println("done")
			return
		}

		for k,v:=range v{

			fmt.Printf("key=%s,value=%s\n", k, v)
		}
	}

}
func TestDaily(t *testing.T)  {

	strs:=[]string{"1","2","3"}
	fmt.Println(strconv.Itoa(len(strs)))
}
