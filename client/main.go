package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	//"time"

	"bytes"
	"os/exec"
	"strings"
	//"strconv"

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
	endpoint = flag.Args()[0]
	log.Printf("Connecting to %v", endpoint)

	cmd := exec.Command("../launch-tool/launch.py", "list")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("list of peers: %v\n", strings.Split(out.String(), "\n"))
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
	fmt.Println(serverMap)
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
	for i:=0; i<200; i++{
		BatchTest()
		fmt.Printf("\033[15DOn %d/1000", (i+1)*5)
	}
	log.Printf("\nFinished test")
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
func BatchTest() {
	Cmd_Batch := make([]*pb.Command,0)
	for i:=0; i<6; i++{
		Cmd_Batch = append(Cmd_Batch, CmdMap[i])
	}
	r := pb.CommandBatch{CmdB: Cmd_Batch}
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
	for i:=1; i<6; i++{
		//log.Printf("Key: %v, Value %v\n", res.ResB[i].GetKv().Key, res.ResB[i].GetKv().Value)
		if res.ResB[i].GetKv().Key != CorrectResMap[i].Key || res.ResB[i].GetKv().Value != CorrectResMap[i].Value{
			log.Printf("Wrong Res!\n")
		}
	}
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
