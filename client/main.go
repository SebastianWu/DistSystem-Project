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
	//"strconv"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/nyu-distributed-systems-fa18/lab-2-raft-SebastianWu/pb"
)

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
	endpoint := flag.Args()[0]
	log.Printf("Connecting to %v", endpoint)

	cmd := exec.Command("../launch-tool/launch.py", "list")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("list of peers: %v\n", strings.Split(out.String(), "\n"))
	peerList := strings.Split(out.String(), "\n")
	serverMap := make(map[string]string)
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
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")
	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)
	// Clear KVC
	res, err := kvc.Clear(context.Background(), &pb.Empty{})
	arg := res.GetRedirect()
	var connServer string
	reSend := false
	if arg != nil {
		reSend = true
		log.Printf("Redirecting!")
	}
	if err != nil {
		reSend = true
		log.Printf("Reconnecting!")
	}
	for arg != nil || err != nil {
		if err != nil {
			for peer := range serverMap {
				if strings.Compare(serverMap[peer], endpoint) != 0 {
					//log.Printf("%v, %v", serverMap[peer], endpoint)
					endpoint = serverMap[peer]
					//log.Printf("Could not clear, reconnect to %v", peer)
					connServer = peer
					break
				}
			}
			err = nil
		} else if arg != nil {
			//log.Printf("Need to redirect to %v", arg.Server)
			peerN := strings.Split(arg.Server, ":")[0]
			connServer = peerN
			endpoint = serverMap[peerN]
			arg = nil
		}
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.Clear(context.Background(), &pb.Empty{})
		arg = res.GetRedirect()
		//log.Printf("%v, %v", arg, err)
	}
	if reSend {
		log.Printf("Reconnect to %v", connServer)
	}

	// Put setting hello -> 1
	putReq := &pb.KeyValue{Key: "hello", Value: "1"}
	res, err = kvc.Set(context.Background(), putReq)
	//if err != nil{
	//	log.Fatalf("Put error")
	//}
	reSend = false
	if arg != nil {
		reSend = true
		log.Printf("Redirecting!")
	}
	if err != nil {
		reSend = true
		log.Printf("Reconnecting!")
	}
	arg = res.GetRedirect()
	for arg != nil || err != nil {
		if err != nil {
			for peer := range serverMap {
				if strings.Compare(serverMap[peer], endpoint) != 0 {
					//log.Printf("%v, %v",serverMap[peer], endpoint)
					endpoint = serverMap[peer]
					//log.Printf("Put error, reconnect to %v", peer)
					connServer = peer
					break
				}
			}
			err = nil
		} else if arg != nil {
			//log.Printf("Need to redirect to %v", arg.Server)
			peerN := strings.Split(arg.Server, ":")[0]
			connServer = peerN
			endpoint = serverMap[peerN]
			arg = nil
		}
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.Set(context.Background(), putReq)
		arg = res.GetRedirect()
		//log.Printf("%v, %v", arg, err)
	}
	if reSend {
		log.Printf("Reconnect to %v", connServer)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Put returned the wrong response")
	}

	time.Sleep(1000 * time.Millisecond)
	// Request value for hello
	req := &pb.Key{Key: "hello"}
	res, err = kvc.Get(context.Background(), req)
	//if err != nil {
	//	log.Fatalf("Request error %v", err)
	//}
	reSend = false
	if arg != nil {
		reSend = true
		log.Printf("Redirecting!")
	}
	if err != nil {
		reSend = true
		log.Printf("Reconnecting!")
	}
	for arg != nil || err != nil {
		if err != nil {
			for peer := range serverMap {
				if strings.Compare(serverMap[peer], endpoint) != 0 {
					//log.Printf("%v, %v", serverMap[peer], endpoint)
					endpoint = serverMap[peer]
					//log.Printf("Request error, reconnect to %v", peer)
					connServer = peer
					break
				}
			}
			err = nil
		} else if arg != nil {
			//log.Printf("Need to redirect to %v", arg.Server)
			peerN := strings.Split(arg.Server, ":")[0]
			connServer = peerN
			endpoint = serverMap[peerN]
			arg = nil
		}
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.Get(context.Background(), req)
		arg = res.GetRedirect()
		//log.Printf("%v, %v", arg, err)
	}
	if reSend {
		log.Printf("Reconnect to %v", connServer)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Get returned the wrong response")
	}

	time.Sleep(1000 * time.Millisecond)
	// Successfully CAS changing hello -> 2
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	//if err != nil {
	//	log.Fatalf("Request error %v", err)
	//}
	reSend = false
	if arg != nil {
		reSend = true
		log.Printf("Redirecting!")
	}
	if err != nil {
		reSend = true
		log.Printf("Reconnecting!")
	}
	arg = res.GetRedirect()
	for arg != nil || err != nil {
		if err != nil {
			for peer := range serverMap {
				if strings.Compare(serverMap[peer], endpoint) != 0 {
					//log.Printf("%v, %v", serverMap[peer], endpoint)
					endpoint = serverMap[peer]
					//log.Printf("Request error, reconnect to %v", peer)
					connServer = peer
					break
				}
			}
			err = nil
		} else if arg != nil {
			//log.Printf("Need to redirect to %v", arg.Server)
			peerN := strings.Split(arg.Server, ":")[0]
			connServer = peerN
			endpoint = serverMap[peerN]
			arg = nil
		}
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.CAS(context.Background(), casReq)
		arg = res.GetRedirect()
		//log.Printf("%v, %v", arg, err)
	}
	if reSend {
		log.Printf("Reconnect to %v", connServer)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "2" {
		log.Fatalf("Get returned the wrong response")
	}

	// Unsuccessfully CAS
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}
	res, err = kvc.CAS(context.Background(), casReq)
	//if err != nil {
	//	log.Fatalf("Request error %v", err)
	//}
	reSend = false
	if arg != nil {
		reSend = true
		log.Printf("Redirecting!")
	}
	if err != nil {
		reSend = true
		log.Printf("Reconnecting!")
	}
	arg = res.GetRedirect()
	for arg != nil || err != nil {
		if err != nil {
			for peer := range serverMap {
				if strings.Compare(serverMap[peer], endpoint) != 0 {
					//log.Printf("%v, %v", serverMap[peer], endpoint)
					endpoint = serverMap[peer]
					//log.Printf("Request error, reconnect to %v",peer)
					connServer = peer
					break
				}
			}
			err = nil
		} else if arg != nil {
			//log.Printf("Need to redirect to %v", arg.Server)
			peerN := strings.Split(arg.Server, ":")[0]
			connServer = peerN
			endpoint = serverMap[peerN]
			arg = nil
		}
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.CAS(context.Background(), casReq)
		arg = res.GetRedirect()
		//log.Printf("%v, %v", arg, err)
	}
	if reSend {
		log.Printf("Reconnect to %v", connServer)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value == "3" {
		log.Fatalf("Get returned the wrong response")
	}

	// CAS should fail for uninitialized variables
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	//if err != nil {
	//	log.Fatalf("Request error %v", err)
	//}
	reSend = false
	if arg != nil {
		reSend = true
		log.Printf("Redirecting!")
	}
	if err != nil {
		reSend = true
		log.Printf("Reconnecting!")
	}
	arg = res.GetRedirect()
	for arg != nil || err != nil {
		if err != nil {
			for peer := range serverMap {
				if strings.Compare(serverMap[peer], endpoint) != 0 {
					//log.Printf("%v, %v", serverMap[peer], endpoint)
					endpoint = serverMap[peer]
					//log.Printf("Request error, reconnect to %v", peer)
					connServer = peer
					break
				}
			}
			err = nil
		} else if arg != nil {
			//log.Printf("Need to redirect to %v", arg.Server)
			peerN := strings.Split(arg.Server, ":")[0]
			connServer = peerN
			endpoint = serverMap[peerN]
			arg = nil
		}
		conn.Close()
		conn, err = grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to dial GRPC server %v", err)
		}
		//log.Printf("Connected")
		kvc = pb.NewKvStoreClient(conn)
		res, err = kvc.CAS(context.Background(), casReq)
		arg = res.GetRedirect()
		//log.Printf("%v, %v", arg, err)
	}
	if reSend {
		log.Printf("Reconnect to %v", connServer)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
		log.Fatalf("Get returned the wrong response")
	}
}
