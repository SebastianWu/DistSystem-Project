package main

import (
	"fmt"
	"log"
	rand "math/rand"
	"net"
	"time"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/DistSystem-Project/RAFT/pb"
)

// Messages that can be passed from the Raft RPC server to the main loop for AppendEntries
type AppendEntriesInput struct {
	arg      *pb.AppendEntriesArgs
	response chan pb.AppendEntriesRet
}

// Messages that can be passed from the Raft RPC server to the main loop for VoteInput
type VoteInput struct {
	arg      *pb.RequestVoteArgs
	response chan pb.RequestVoteRet
}

// Struct off of which we shall hang the Raft service
type Raft struct {
	AppendChan chan AppendEntriesInput
	VoteChan   chan VoteInput
}

// server role type
type Role int

const (
	FOLLOWER  Role = 0
	CANDIDATE Role = 1
	LEADER    Role = 2
)

func (r *Raft) AppendEntries(ctx context.Context, arg *pb.AppendEntriesArgs) (*pb.AppendEntriesRet, error) {
	c := make(chan pb.AppendEntriesRet)
	r.AppendChan <- AppendEntriesInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

func (r *Raft) RequestVote(ctx context.Context, arg *pb.RequestVoteArgs) (*pb.RequestVoteRet, error) {
	c := make(chan pb.RequestVoteRet)
	r.VoteChan <- VoteInput{arg: arg, response: c}
	result := <-c
	return &result, nil
}

// Compute a random duration in milliseconds
func randomDuration(r *rand.Rand) time.Duration {
	// Constant
	const DurationMax = 4000
	const DurationMin = 1000
	return time.Duration(r.Intn(DurationMax-DurationMin)+DurationMin) * time.Millisecond
}

// Restart the supplied timer using a random timeout based on function above
func restartTimer(timer *time.Timer, r *rand.Rand) {
	stopped := timer.Stop()
	// If stopped is false that means someone stopped before us, which could be due to the timer going off before this,
	// in which case we just drain notifications.
	if !stopped {
		// Loop for any queued notifications
		for len(timer.C) > 0 {
			<-timer.C
		}

	}
	timer.Reset(randomDuration(r))
}

// Launch a GRPC service for this Raft peer.
func RunRaftServer(r *Raft, port int) {
	// Convert port to a string form
	portString := fmt.Sprintf(":%d", port)
	// Create socket that listens on the supplied port
	c, err := net.Listen("tcp", portString)
	if err != nil {
		// Note the use of Fatalf which will exit the program after reporting the error.
		log.Fatalf("Could not create listening socket %v", err)
	}
	// Create a new GRPC server
	s := grpc.NewServer()

	pb.RegisterRaftServer(s, r)
	log.Printf("Going to listen on port %v", port)

	// Start serving, this will block this function and only return when done.
	if err := s.Serve(c); err != nil {
		log.Fatalf("Failed to serve %v", err)
	}
}

func connectToPeer(peer string) (pb.RaftClient, error) {
	backoffConfig := grpc.DefaultBackoffConfig
	// Choose an aggressive backoff strategy here.
	backoffConfig.MaxDelay = 500 * time.Millisecond
	conn, err := grpc.Dial(peer, grpc.WithInsecure(), grpc.WithBackoffConfig(backoffConfig))
	// Ensure connection did not fail, which should not happen since this happens in the background
	if err != nil {
		return pb.NewRaftClient(nil), err
	}
	return pb.NewRaftClient(conn), nil
}

// My own Min function
func Min(x int64, y int64) int64 {
	if x > y {
		return y
	}
	return x
}

// The main service loop. All modifications to the KV store are run through here.
func serve(s *KVStore, r *rand.Rand, peers *arrayPeers, id string, port int) {
	r.Seed(200)
	raft := Raft{AppendChan: make(chan AppendEntriesInput), VoteChan: make(chan VoteInput)}
	// Start in a Go routine so it doesn't affect us.
	go RunRaftServer(&raft, port)

	peerClients := make(map[string]pb.RaftClient)

	for _, peer := range *peers {
		client, err := connectToPeer(peer)
		if err != nil {
			log.Fatalf("Failed to connect to GRPC server %v", err)
		}

		peerClients[peer] = client
		log.Printf("Connected to %v", peer)
	}

	type AppendResponse struct {
		ret          *pb.AppendEntriesRet
		prevLogIndex int64
		//lastLogIndex int64 // the index of last log
		lenEntries   int64
		err          error
		peer         string
	}

	type VoteResponse struct {
		ret  *pb.RequestVoteRet
		err  error
		peer string
	}
	appendResponseChan := make(chan AppendResponse)
	voteResponseChan := make(chan VoteResponse)

	// Create a timer and start running it
	timer := time.NewTimer(randomDuration(r))

	// Persistent state on all server
	currentTerm := int64(0)
	votedFor := ""
	var LOG []*pb.Entry

	// Volatile state on all server
	commitIndex := int64(0)
	lastApplied := int64(0)

	// Volatile state on leaders	// Reinitialized after election
	nextIndex := make(map[string]int64)
	matchIndex := make(map[string]int64)

	// my own variable
	role := FOLLOWER
	num_votes_received := 0
	currLeader := id
	responseChan := make(map[int64]chan pb.ResultBatch) // only for leader to store the response channel for each operation

	// initialize LOG with a empty entry with index 0
	LOG = append(LOG, &pb.Entry{Term: 0, Index: 0, CmdB: nil})

	// Run forever handling inputs from various channels
	for {
		//time.Sleep(10 * time.Millisecond)
		//log.Println(LOG)
		//All Servers: If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine

		// if role is leader then send append entry RPCs each round
		if role == LEADER {
			// If there exists an N such that N > commitIndex, a majority of matchIndex[i]>=N, and log[N].Term == currentTerm: set commitIndex = N
			for i := commitIndex + 1; i < int64(len(LOG)); i++ {
				if LOG[i].Term == currentTerm {
					count := 1
					for p := range peerClients {
						if matchIndex[p] >= i {
							count += 1
						}
					}
					if count > (len(peerClients)+1)/2 {
						commitIndex = i
					} else {
						break
					}
				}
			}
			if commitIndex > lastApplied { // Response after entry applied to state machine if this server is LEADER
				lastApplied += 1
				s.HandleBatchCommand(BatchInputChannelType{command_batch: *LOG[lastApplied].CmdB, response_batch: responseChan[lastApplied]})
			}

			// get lastLogIndex for the use of Append Response
			lastLogIndex := LOG[len(LOG)-1].Index

			for p, c := range peerClients {
				prevLogIndex := LOG[nextIndex[p]-1].Index
				prevLogTerm := LOG[nextIndex[p]-1].Term
				// If last log index >= nextIndex for a follower: send Append Entries RPC with log entries starting at nextIndex
				var entries []*pb.Entry = nil
				if nextIndex[p] < int64(len(LOG)) {
					entries = LOG[nextIndex[p]:]
				}
				go func(c pb.RaftClient, p string) {
					ret, err := c.AppendEntries(context.Background(), &pb.AppendEntriesArgs{Term: currentTerm, LeaderID: id, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm, LeaderCommit: commitIndex, Entries: entries})
					appendResponseChan <- AppendResponse{ret: ret, prevLogIndex: prevLogIndex, lenEntries: int64(len(entries)), err: err, peer: p}
				}(c, p)
				// implement pipeline
				nextIndex[p] = lastLogIndex + 1
			}
			restartTimer(timer, r)
		} else if commitIndex > lastApplied { // Update state machine if this server is not leader
			lastApplied += 1
			s.UpdateStateMachineWBC(*LOG[lastApplied].CmdB)
		}
		select {
		case <-timer.C:
			// The timer went off.
			log.Printf("Time out!")
			// On conversion to candidate, start election:
			role = CANDIDATE
			// 	Increment current term
			currentTerm += 1
			// 	Vote for self
			votedFor = id
			num_votes_received = 1
			// 	Send request vote RPCs to all other servers
			for p, c := range peerClients {
				// Send in parallel so we don't wait for each client.
				// get lastLogTerm
				lastLogTerm := LOG[len(LOG)-1].Term
				lastLogIndex := LOG[len(LOG)-1].Index

				go func(c pb.RaftClient, p string) {
					ret, err := c.RequestVote(context.Background(), &pb.RequestVoteArgs{Term: currentTerm, CandidateID: id, LastLogIndex: lastLogIndex, LasLogTerm: lastLogTerm})
					voteResponseChan <- VoteResponse{ret: ret, err: err, peer: p}
				}(c, p)
			}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r)
		/*
		case op := <-s.C:
			// We received an operation from a client
			// Figure out if you can actually handle the request here. If not use the Redirect result to send the
			// client elsewhere.
			if role != LEADER { // send pb.Redirect{currLeader} back to client
				op.response <- pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: currLeader}}}
			} else { // if role is LEADER
				log.Printf("received operation %v", op.command)
				// append entry to local log
				lastLogIndex := LOG[len(LOG)-1].Index

				lastLogIndex += 1
				//log.Printf(" new log index: %v", lastLogIndex)
				LOG = append(LOG, &pb.Entry{Term: currentTerm, Index: lastLogIndex, Cmd: &op.command})
				responseChan[lastLogIndex] = op.response
			}
			// Use Raft to make sure it is safe to actually run the command.
		*/
		case op_b := <-s.BC:	// after implement batching
			// We received an operation from a client
			// Figure out if you can actually handle the request here. If not use the Redirect result to send the
			// client elsewhere.
			if role != LEADER { // send pb.Redirect{currLeader} back to client
				redirect_m := pb.Result{Result: &pb.Result_Redirect{Redirect: &pb.Redirect{Server: currLeader}}}
				res_b := make([]*pb.Result,0)
				res_b = append(res_b, &redirect_m)
				op_b.response_batch <- pb.ResultBatch{ResB: res_b}
			} else { // if role is LEADER
				log.Printf("received operation batch: %v", len(op_b.command_batch.CmdB))
				// append entry to local log
				lastLogIndex := LOG[len(LOG)-1].Index

				lastLogIndex += 1
				//log.Printf(" new log index: %v", lastLogIndex)
				LOG = append(LOG, &pb.Entry{Term: currentTerm, Index: lastLogIndex, CmdB: &op_b.command_batch})
				responseChan[lastLogIndex] = op_b.response_batch
			}
			// Use Raft to make sure it is safe to actually run the command.

		case ae := <-raft.AppendChan:
			// We received an AppendEntries request from a Raft peer

			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			if ae.arg.Term > currentTerm {
				currentTerm = ae.arg.Term
				role = FOLLOWER
				votedFor = ""
				num_votes_received = 0
			}
			// If AppendEntries RPC received from new leader: convert to follower
			if role != LEADER && ae.arg.LeaderID != currLeader {
				currLeader = ae.arg.LeaderID
				role = FOLLOWER
				votedFor = ""
				num_votes_received = 0
			}
			//	 Reply false if term < currentTerm
			if ae.arg.Term < currentTerm {
				ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
			} else if int64(len(LOG)) <= ae.arg.PrevLogIndex || LOG[ae.arg.PrevLogIndex].Term != ae.arg.PrevLogTerm {
				// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
				//log.Printf("LOG doesn't contain an entry at prevLogIndex's term match prevLogTerm")
				ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: false}
			} else {
				if ae.arg.Entries != nil {
					log.Printf("Received append entry from %v at term %v", ae.arg.LeaderID, ae.arg.Term)
				} else {
					//log.Printf("Received heartbeats from %v at term %v", ae.arg.LeaderID, ae.arg.Term)
				}
				// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
				start_to_delete_index := int64(len(LOG))
				for i := ae.arg.PrevLogIndex + 1; i < int64(len(LOG)) && i-ae.arg.PrevLogIndex-1 < int64(len(ae.arg.Entries)); i++ {
					if LOG[i].Term != ae.arg.Entries[i-ae.arg.PrevLogIndex-1].Term {
						start_to_delete_index = i
						break
					}
				}
				//log.Printf("start to delete index: %v", start_to_delete_index)
				LOG = LOG[:start_to_delete_index]
				// Append any new entries not already in the log
				for i := start_to_delete_index - ae.arg.PrevLogIndex - 1; i < int64(len(ae.arg.Entries)); i++ {
					LOG = append(LOG, ae.arg.Entries[i])
				}
				// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
				if ae.arg.LeaderCommit > commitIndex {
					lastNewEntryIndex := int64(len(LOG)) - 1
					commitIndex = Min(ae.arg.LeaderCommit, lastNewEntryIndex)
				}
				ae.response <- pb.AppendEntriesRet{Term: currentTerm, Success: true}
			}
			// This will also take care of any pesky timeouts that happened while processing the operation.
			restartTimer(timer, r)
		case vr := <-raft.VoteChan:
			// We received a RequestVote RPC from a raft peer
			log.Printf("Received vote request from %v", vr.arg.CandidateID)
			// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			if vr.arg.Term > currentTerm {
				currentTerm = vr.arg.Term
				role = FOLLOWER
				num_votes_received = 0
				votedFor = ""
			}

			// get lastLogTerm and lastLogIndex for second condition below
			lastLogTerm := LOG[len(LOG)-1].Term
			lastLogIndex := LOG[len(LOG)-1].Index

			if vr.arg.Term < currentTerm {
				// Reply false if term < currentTerm
				//log.Printf("Candidate %v 's term: %v is smaller than current term: %v", vr.arg.CandidateID, vr.arg.Term, currentTerm)
				vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
			} else if (votedFor == "" || votedFor == vr.arg.CandidateID) && vr.arg.LastLogIndex >= lastLogIndex && vr.arg.LasLogTerm >= lastLogTerm {
				// If votedFor is null or candidateId, and candidate's log is at least up-to-date as receiver's log, grant vote
				votedFor = vr.arg.CandidateID
				vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: true}
				restartTimer(timer, r)
			} else {
				vr.response <- pb.RequestVoteRet{Term: currentTerm, VoteGranted: false}
			}
		case vr := <-voteResponseChan:
			// We received a response to a previous vote request.
			if vr.err != nil {
				// Do not do Fatalf here since the peer might be gone but we should survive.
				//log.Printf("Error calling RPC %v", vr.err)
			} else {
				log.Printf("Got response to vote request from %v", vr.peer)
				//log.Printf("Peers %s granted %v term %v", vr.peer, vr.ret.VoteGranted, vr.ret.Term)
				// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
				if vr.ret.Term > currentTerm {
					currentTerm = vr.ret.Term
					role = FOLLOWER
					votedFor = ""
					num_votes_received = 0
				}
				if vr.ret.Term == currentTerm && vr.ret.VoteGranted && role == CANDIDATE {
					num_votes_received += 1
				}
				// if vr.ret.Term < currentTerm
				//	do nothing, ignore previous vote request's response
				if num_votes_received > (len(peerClients)+1)/2 && role == CANDIDATE {
					log.Printf("Received %v votes from peers, current term: %v, I become leader!!!", num_votes_received, currentTerm)
					role = LEADER
					currLeader = id
					// initialize volatile state on leaders
					// 	get lastLogIndex
					lastLogIndex := int64(0)
					lastLogIndex = LOG[len(LOG)-1].Index
					for _, peer := range *peers {
						nextIndex[peer] = lastLogIndex + 1
						matchIndex[peer] = 0
					}
				}
			}
		case ar := <-appendResponseChan:
			// We received a response to a previous AppendEntries RPC call
			if ar.err != nil {
				//log.Printf("Error calling RPC %c", ar.err)
			} else {
				// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
				if ar.ret.Term > currentTerm {
					currentTerm = ar.ret.Term
					role = FOLLOWER
					votedFor = ""
					num_votes_received = 0
				}
				if role == LEADER && ar.ret.Term == currentTerm {
					log.Printf("Got append response from %v at term %v %v", ar.peer, currentTerm, ar.prevLogIndex + ar.lenEntries)
					// If successful: update nextIndex and matchIndex for follower
					if ar.ret.Success == true {
						matchIndex[ar.peer] = ar.prevLogIndex + ar.lenEntries
						nextIndex[ar.peer] = ar.prevLogIndex + ar.lenEntries + 1
					} else {
						// If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
						//if nextIndex[ar.peer] > 1 {
						//	nextIndex[ar.peer] -= 1
						//}
						nextIndex[ar.peer] = ar.prevLogIndex
					}
				}
			}
		}
	}
	log.Printf("Strange to arrive here")
}
