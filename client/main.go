package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"time"

	"bytes"
	"os/exec"
	"strings"
	"strconv"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/DistSystem-Project/RAFT/pb"
)

var endpoint string
var serverMap map[string]string
var kvc pb.KvStoreClient
var conn *grpc.ClientConn
var CmdMap map[int]*pb.Command
var CorrectResMap map[int]pb.KeyValue
var BatchSize int
var CommandNum int
var AVG_Latency time.Duration

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no endpoint fail
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	BatchSize = 10	// default batch size
	CommandNum = 1000	// default test command num
	endpoint = flag.Args()[0]
	if flag.NArg() >1 {
		bs,err := strconv.Atoi(flag.Args()[1])
		if err==nil{
			BatchSize = bs
		}
	}
	if flag.NArg() == 3 {
		cn,err := strconv.Atoi(flag.Args()[2])
		if err == nil{
			CommandNum = cn
		}
	}
	log.Printf("Connecting to %v", endpoint)
	log.Printf("Test %v commands with batch size %v", CommandNum, BatchSize)

	cmd := exec.Command("../launch-tool/launch.py", "list")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	//log.Printf("list of peers: %v\n", strings.Split(out.String(), "\n"))
	serverMap = make(map[string]string)
	peerList := strings.Split(out.String(), "\n")
	for i := 0; i < len(peerList); i++ {
		if strings.Contains(peerList[i], "peer") {
			peerN := strings.Trim(peerList[i], "peer")
			cmd := exec.Command("../launch-tool/launch.py", "client-url", peerN)
			out.Reset()
			cmd.Stdout = &out
			err = cmd.Run()
			if err != nil {
				log.Fatal(err)
			}
			//log.Printf("peer address: %v, %v", strings.Trim(out.String(),"\n"), len(out.String()))
			serverMap[peerList[i]] = strings.Trim(out.String(), "\n")
		}
	}
	//fmt.Println(serverMap)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}

	buildCommandMap()
	buildCorrectResMap()

	// Create a KvStore client
	kvc = pb.NewKvStoreClient(conn)
	start := time.Now()
	for i:=0; i<CommandNum; {
		i = BatchTest(i)
		fmt.Printf("\033[15DOn %d/%v", i, CommandNum)
	}
	t := time.Now()
	elapsed := t.Sub(start)
	AVG_Latency = AVG_Latency/time.Duration(CommandNum)
	fmt.Println()
	log.Printf("Finished test with %v runtime, %v AVG latency", elapsed, AVG_Latency)
}
func buildCommandMap() {
	CmdMap = make(map[int]*pb.Command)
	clear_c := pb.Command{Operation: pb.Op_CLEAR, Arg: &pb.Command_Clear{Clear: &pb.Empty{}}}
	set_c := pb.Command{Operation: pb.Op_SET, Arg: &pb.Command_Set{Set: &pb.KeyValue{Key: "hello", Value: "1"}}}
	get_c := pb.Command{Operation: pb.Op_GET, Arg: &pb.Command_Get{Get: &pb.Key{Key: "hello"}}}
	cas_c1 := pb.Command{Operation: pb.Op_CAS, Arg: &pb.Command_Cas{Cas: &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}}}
	cas_c2 := pb.Command{Operation: pb.Op_CAS, Arg: &pb.Command_Cas{Cas: &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}}}
	cas_c3 := pb.Command{Operation: pb.Op_CAS, Arg: &pb.Command_Cas{Cas: &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}}}
	CmdMap[0] = &clear_c
	CmdMap[1] = &set_c
	CmdMap[2] = &get_c
	CmdMap[3] = &cas_c1
	CmdMap[4] = &cas_c2
	CmdMap[5] = &cas_c3
}
func buildCorrectResMap() {
	CorrectResMap = make(map[int]pb.KeyValue)
	CorrectResMap[0] = pb.KeyValue{}
	CorrectResMap[1] = pb.KeyValue{Key: "hello", Value: "1"}
	CorrectResMap[2] = pb.KeyValue{Key: "hello", Value: "1"}
	CorrectResMap[3] = pb.KeyValue{Key: "hello", Value: "2"}
	CorrectResMap[4] = pb.KeyValue{Key: "hello", Value: "2"}
	CorrectResMap[5] = pb.KeyValue{Key: "hellooo", Value: ""}
}
func BatchTest(counter int) (int){
	Cmd_Batch := make([]*pb.Command,0)
	start_num := counter
	for i:=0; i<BatchSize; i++{
		if counter < CommandNum{
			Cmd_Batch = append(Cmd_Batch, CmdMap[counter%6])
			counter += 1
		}else{
			break
		}
	}
	end_num := counter
	start := time.Now()	// start time of this batch
	r := pb.CommandBatch{CmdB: Cmd_Batch}	// request of this batch
	res, err := kvc.Batching(context.Background(), &r)
	arg := res.ResB[0].GetRedirect()
	if arg != nil{
		log.Printf("Redirecting!")
	}
	for arg != nil {
		peerN := strings.Split(arg.Server, ":")[0]
		endpoint = serverMap[peerN]
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.Batching(context.Background(), &r)
		arg = res.ResB[0].GetRedirect()
	}
	t := time.Now()	// finish time of this batch
	Latency := t.Sub(start)
	AVG_Latency += Latency * time.Duration(len(Cmd_Batch))
	for i:=start_num; i<end_num; i++{	// check res is correct or not
		//log.Printf("Key: %v, Value %v\n", res.ResB[i].GetKv().Key, res.ResB[i].GetKv().Value)
		if i%6!=0 && (res.ResB[i-start_num].GetKv().Key != CorrectResMap[i%6].Key || res.ResB[i-start_num].GetKv().Value != CorrectResMap[i%6].Value){
			log.Printf("Wrong Res!\n")
		}
	}
	return counter
}
func test() {
	// Clear KVC
	r := pb.Command{Operation: pb.Op_CLEAR, Arg: &pb.Command_Clear{Clear: &pb.Empty{}}}
	res, err := kvc.Do(context.Background(), &r)
	arg := res.GetRedirect()
	if arg != nil {
		log.Printf("Redirecting")
	}
	for arg != nil {
		peerN := strings.Split(arg.Server, ":")[0]
		endpoint = serverMap[peerN]
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.Clear(context.Background(), &pb.Empty{})
		arg = res.GetRedirect()
	}

	// Put setting hello -> 1
	putReq := &pb.KeyValue{Key: "hello", Value: "1"}
	res, err = kvc.Set(context.Background(), putReq)
	//if err != nil{
	//	log.Fatalf("Put error")
	//}
	arg = res.GetRedirect()
	if arg != nil {
		log.Printf("Redirecting")
	}

	for arg != nil {
		peerN := strings.Split(arg.Server, ":")[0]
		endpoint = serverMap[peerN]
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.Set(context.Background(), putReq)
		arg = res.GetRedirect()
	}
	//log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Put returned the wrong response")
	}

	//time.Sleep(1000 * time.Millisecond)
	// Request value for hello
	req := &pb.Key{Key: "hello"}
	res, err = kvc.Get(context.Background(), req)
	//if err != nil {
	//	log.Fatalf("Request error %v", err)
	//}
	arg = res.GetRedirect()

	if arg != nil {
		log.Printf("Redirecting")
	}

	for arg != nil {
		peerN := strings.Split(arg.Server, ":")[0]
		endpoint = serverMap[peerN]
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.Get(context.Background(), req)
		arg = res.GetRedirect()
	}

	//log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Get returned the wrong response")
	}

	//time.Sleep(1000 * time.Millisecond)
	// Successfully CAS changing hello -> 2
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	//if err != nil {
	//	log.Fatalf("Request error %v", err)
	//}
	arg = res.GetRedirect()

	if arg != nil {
		log.Printf("Redirecting")
	}

	for arg != nil {
		peerN := strings.Split(arg.Server, ":")[0]
		endpoint = serverMap[peerN]
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.CAS(context.Background(), casReq)
		arg = res.GetRedirect()
	}

	//log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "2" {
		log.Fatalf("Get returned the wrong response")
	}

	// Unsuccessfully CAS
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}
	res, err = kvc.CAS(context.Background(), casReq)
	//if err != nil {
	//	log.Fatalf("Request error %v", err)
	//}
	arg = res.GetRedirect()

	if arg != nil {
		log.Printf("Redirecting")
	}

	for arg != nil {
		peerN := strings.Split(arg.Server, ":")[0]
		endpoint = serverMap[peerN]
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.CAS(context.Background(), casReq)
		arg = res.GetRedirect()
	}

	//log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value == "3" {
		log.Fatalf("Get returned the wrong response")
	}

	// CAS should fail for uninitialized variables
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	//if err != nil {
	//	log.Fatalf("Request error %v", err)
	//}
	arg = res.GetRedirect()

	if arg != nil {
		log.Printf("Redirecting")
	}

	for arg != nil {
		peerN := strings.Split(arg.Server, ":")[0]
		endpoint = serverMap[peerN]
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.CAS(context.Background(), casReq)
		arg = res.GetRedirect()
	}

	//log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
		log.Fatalf("Get returned the wrong response")
	}

}
