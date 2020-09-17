// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/cmu440/p0partA/kvstore"
)

type keyValueServer struct {
	ln                  net.Listener         //server listener
	countActive         int                  //count of Active clients
	countDropped        int                  //count of Dropped clients
	clients             []clientInfo         //store clients info
	countActiveUpdate   chan bool            //true if countActive needs to be updated
	countDroppedUpdate  chan bool            //true if countDropped needs to be updated
	countActiveRequest  chan bool            //true if countActive is requested
	countDroppedRequest chan bool            //true if countDropped is requested
	countResult         chan int             //sends count result
	request             chan databaseRequest //sends database request
	closeServer         chan bool            //true is server needs to be closed
	closeMain           chan bool            //true if main routine needs to be closed
	store               kvstore.KVStore      //database
}

type clientInfo struct {
	conn       net.Conn    //client connection descriptor
	data       chan string //data channel for the client
	closeRead  chan bool   //true if read routine for this clients needs to be closed
	closeWrite chan bool   //true if write routine for this clients needs to be closed
}

type databaseRequest struct {
	request []byte     //database request
	client  clientInfo //associated client
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	return &keyValueServer{
		store:               store,
		countActive:         0,
		countDropped:        0,
		countActiveUpdate:   make(chan bool),
		countDroppedUpdate:  make(chan bool),
		countActiveRequest:  make(chan bool),
		countDroppedRequest: make(chan bool),
		countResult:         make(chan int),
		request:             make(chan databaseRequest),
		closeServer:         make(chan bool, 1),
		closeMain:           make(chan bool, 1),
	}
}

func (kvs *keyValueServer) Start(port int) error {

	//server starts listening
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
		return err
	}
	kvs.ln = ln

	//starts the accept and main routines
	go kvs.acceptRoutine()
	go kvs.mainRoutine()

	//This routine is used to close the server
	go func() {
		<-kvs.closeServer
		kvs.ln.Close()
	}()
	return nil
}

func (kvs *keyValueServer) Close() {

	//close the read and write routines for all clients
	for _, client := range kvs.clients {
		client.closeRead <- true
		client.closeWrite <- true
	}
	//close main routine
	kvs.closeMain <- true
	//close the server
	kvs.closeServer <- true
}

func (kvs *keyValueServer) CountActive() int {
	kvs.countActiveRequest <- true
	count := <-kvs.countResult
	return count
}

func (kvs *keyValueServer) CountDropped() int {
	kvs.countDroppedRequest <- true
	count := <-kvs.countResult
	return count
}

func (kvs *keyValueServer) acceptRoutine() error {
	for {
		//Waits for client
		conn, err := kvs.ln.Accept()

		if err != nil {
			return err
		}

		//client info
		client := &clientInfo{
			conn:       conn,
			data:       make(chan string, 500), //client queue can store 500 messages
			closeRead:  make(chan bool, 1),
			closeWrite: make(chan bool, 1),
		}

		//stores clients info
		kvs.clients = append(kvs.clients, *client)

		//update active clients
		kvs.countActiveUpdate <- true

		//starts read and write routine for the client
		go kvs.readRoutine(client)
		go kvs.writeRoutine(client)
	}
}

func (kvs *keyValueServer) mainRoutine() {
	for {
		select {
		//countActive update is requested
		case <-kvs.countActiveUpdate:
			kvs.countActive += 1
		//countDropped update is requested
		case <-kvs.countDroppedUpdate:
			kvs.countActive -= 1
			kvs.countDropped += 1
		//number of active is requested
		case <-kvs.countActiveRequest:
			kvs.countResult <- kvs.countActive
		//number of dropped clients is requested
		case <-kvs.countDroppedRequest:
			kvs.countResult <- kvs.countDropped
		//database request received
		case databaseRequest := <-kvs.request:
			s := bytes.Split(databaseRequest.request, []byte(":"))
			f := string(s[0])                //Request : Get , Put, Delete
			key := string(s[1])              //Key
			client := databaseRequest.client //associated client
			switch f {
			case "Put":
				value := s[2]
				kvs.store.Put(key, value)
			case "Get":
				value := kvs.store.Get(key)
				//if no value in database
				if len(value) == 0 {
					s := key + ":" + string("\n")
					if len(client.data) != cap(client.data) {
						client.data <- s
					}
				} else {
					for i := range value {
						s := key + ":" + string(value[i]) + "\n"
						if len(client.data) != cap(client.data) {
							client.data <- s
						}
					}
				}
			case "Delete":
				kvs.store.Clear(key)
			}
		//close main request received
		case <-kvs.closeMain:
			return
		}
	}
}

func (kvs *keyValueServer) readRoutine(client *clientInfo) {
	reader := bufio.NewReader(client.conn)
	for {
		select {
		//close read request received
		case <-client.closeRead:
			client.conn.Close()
			return
		//reads from the clients
		default:
			netData, err := reader.ReadString('\n')
			if err == io.EOF {
				kvs.countDroppedUpdate <- true
				return
			}
			if err != nil {
				fmt.Println(err)
				return
			}
			request := bytes.TrimSpace([]byte(netData))
			requestStruct := &databaseRequest{
				request: request,
				client:  *client,
			}
			kvs.request <- *requestStruct
		}
	}
}

func (kvs *keyValueServer) writeRoutine(client *clientInfo) {
	writer := bufio.NewWriter(client.conn)
	for {
		select {
		case message := <-client.data:
			writer.WriteString(message)
			writer.Flush()
		case <-client.closeWrite:
			return
		}
	}
}
