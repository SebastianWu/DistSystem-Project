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

	// Create a KvStore client
	kvc = pb.NewKvStoreClient(conn)
	log.Printf("start to test 1000 requests!\n")
	for i := 0; i < 200; i++ {
		test()
		fmt.Printf("\033[15DOn %d/1000", (i+1)*5)
	}
	fmt.Println()
	log.Printf("Finished test")
}
func test() {
	// Clear KVC
	res, err := kvc.Clear(context.Background(), &pb.Empty{})
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
