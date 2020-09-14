// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"github.com/cmu440/p0partA/kvstore"
	"fmt"
	"net"
	"strconv"
	"bufio"
	"bytes"
	"io"
)

type keyValueServer struct {
	ln net.Listener
	countActive int
	countDropped int
	clients []clientInfo
	countActiveUpdate chan bool
	countDroppedUpdate chan bool
	countActiveRequest chan bool
	countDroppedRequest chan bool
	countResult chan int
	request chan databaseRequest
	closeAccept chan bool
	closeMain chan bool
	store kvstore.KVStore
}

type clientInfo struct {
	conn net.Conn
	data chan string
	closeRead chan bool
	closeWrite chan bool
}

type databaseRequest struct {
	request []byte
	client clientInfo
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	return &keyValueServer{
		store: store,
		countActive: 0,
		countDropped: 0,
		countActiveUpdate: make(chan bool),
		countDroppedUpdate: make(chan bool),
		countActiveRequest: make(chan bool),
		countDroppedRequest: make(chan bool),
		countResult: make(chan int),
		request: make(chan databaseRequest),
		closeAccept: make(chan bool, 1),
		closeMain: make(chan bool, 1),
		
	}
}

func (kvs *keyValueServer) Start(port int) error {
	ln, err := net.Listen("tcp", ":" + strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
		return err
	}
	kvs.ln = ln
	go kvs.acceptRoutine()
	go kvs.mainRoutine()
	return nil
}

func (kvs *keyValueServer) Close() {
	// fmt.Println("CLOSE1")
	for _, client := range kvs.clients {
		client.closeRead <- true
		// fmt.Println("CLOSE11")
		client.closeWrite <- true
	}
	kvs.closeMain <- true
	kvs.closeAccept <- true
	// fmt.Println("CLOSE2")
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

// TODO: add additional methods/functions below!
func (kvs *keyValueServer) acceptRoutine() error {
	for {
		select {
			case <-kvs.closeAccept:
				fmt.Println("CLOSE Accept")
				kvs.ln.Close()
				return nil
			default:
				conn, err := kvs.ln.Accept()
				
				if err != nil {
					kvs.ln.Close()
					return err
				}

				client := &clientInfo{
					conn: conn,
					data: make(chan string,500),
					closeRead: make(chan bool, 1),
					closeWrite: make(chan bool, 1),
				}

				kvs.clients = append(kvs.clients, *client)

				kvs.countActiveUpdate <- true
				go kvs.readRoutine(client)
				go kvs.writeRoutine(client)		
		}	
	}
}

func (kvs *keyValueServer) mainRoutine() {
	for {
		select {
			case <-kvs.countActiveUpdate:
				kvs.countActive += 1
			case <-kvs.countDroppedUpdate:
				kvs.countActive -= 1
				kvs.countDropped += 1
			case <-kvs.countActiveRequest:
				kvs.countResult <- kvs.countActive	
			case <-kvs.countDroppedRequest:
				kvs.countResult <- kvs.countDropped
			case databaseRequest := <-kvs.request:
				// fmt.Println("New Request")
				s := bytes.Split(databaseRequest.request, []byte(":"))
				f := string(s[0])
				key := string(s[1])
				client := databaseRequest.client
				switch f {
					case "Put":
						value := s[2]
						kvs.store.Put(key,value)
					case "Get":
						value := kvs.store.Get(key)
						if len(value) == 0 {
							s := key + ":" + string("\n")
							if len(client.data) != cap(client.data) {
								client.data <- s
							}
						} else {
							for  i:= range value {
								s := key + ":" + string(value[i]) + "\n"
								if len(client.data) != cap(client.data) {
									client.data <- s
								}
							}
						}	
					case "Delete":
						kvs.store.Clear(key)		
				}
			case <-kvs.closeMain:
				return
		}
	}
}

func (kvs *keyValueServer) readRoutine(client *clientInfo) {
	reader := bufio.NewReader(client.conn)
	for {
		select {
			case <-client.closeRead:
				client.conn.Close()
				return
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
					client: *client,
				}
				kvs.request <- *requestStruct
		}	
	}
}

func (kvs *keyValueServer) writeRoutine(client *clientInfo) {
	// buffer := make(chan string, 500)
	writer := bufio.NewWriter(client.conn)
	for {
		select {
			case message := <- client.data:
				writer.WriteString(message)
				writer.Flush()
			case <-client.closeWrite:
				return
		}				
	}
}
