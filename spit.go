package main

import (
  "fmt"
  "os"
  "net"
  "time"
  "sync"

  bw "github.com/immesys/bw2bind"
)
func bail(v interface{}) {
   fmt.Println(v)
   os.Exit(1)
}

func main() {
  // Connect to the local bosswave router
  cl, err := bw.Connect("localhost:28589")
	if err != nil {
		bail(err)
	}
  // Tell the local router who we are. Remember that in BW,
  // your identity is key
  us, err := cl.SetEntityFile("balingwire.key")
	if err != nil {
		bail(err)
	}

  // As we are going to do a bunch of publishes on the same
  // URI, it is faster to calculate the permissions once, and reuse
  // them instead of doing it on every message (using AutoChain)
	uri := "castle.bw2.io/hamilton/+/accel"
  // This says "find me any chain that gives me Publish and Consume on
  // the given url"
	pac, e := cl.BuildAnyChain(uri, "PC", us)
	if pac == nil {
		fmt.Println("Could not get permissions: ", e)
		os.Exit(1)
	}
  // Note that if permissions exist, but your local router does not
  // know about them, the above might fail. Simply let your local
  // router know about them by executing
  // bw2 bc --uri <uri> --to <our vk> --permissions "PC" --router <RTR>
  // where RTR is a router that has the missing dots to build a chain
  // for example castle.bw2.io
  // You can refer to a router by its key or by DNS name that will be
  // resolved to a key.
	fmt.Println("using pac: ", pac.Hash)

  msgchan := listenForHamiltonPackets()
  go genDemoPackets(msgchan)
  for {
    m := <- msgchan
    // m is a compliant hamilton accelerometer PO
    err = cl.Publish(&bw.PublishParams{
      // The URI we want to publish to
      URI:  "castle.bw2.io/hamilton/"+m.serial+"/accel",
      // The access chain that gives us permissions
      PrimaryAccessChain: pac.Hash,
      // This says include the full PAC in the message
      // It might soon become the default
      ElaboratePAC: bw.ElaborateFull,
      PayloadObjects: m.POs,
    })
    if err != nil {
      fmt.Println("Error publishing: ", err.Error())
    } else {
      fmt.Println("Published ok")
    }
  }
}

type HamiltonMessage struct {
  serial string
  POs []bw.PayloadObject
}
var eidTable map[string]uint32
var tablock sync.Mutex
var EPOCH uint64 = 1451606400000
func listenForHamiltonPackets() chan HamiltonMessage {
  eidTable = make(map[string]uint32)

  ch := make (chan HamiltonMessage, 10)
  go func () {
    addr, err := net.ResolveUDPAddr("udp6",":4041")
    if err != nil {
      panic(err)
    }
    sock, err := net.ListenUDP("udp6", addr)
    if err != nil {
      panic(err)
    }
    for {
      buf := make([]byte, 2048)
      ln, addr, err := sock.ReadFromUDP(buf)
      fmt.Printf("Got packet ADDR %+v\n", addr)
      if err != nil {
        fmt.Printf("Got error: %v\n", err)
        continue
      }
      go handlePacket(addr, buf[:ln], ch)
      //Timestamp is in 0.1 seconds since 2016
      ts := (uint64(time.Now().UnixNano()/1000000) - EPOCH) / 100
    	pkt := []byte{buf[0],buf[1],buf[2],buf[3],
    		uint8(ts), uint8(ts >> 8), uint8(ts >> 16), uint8(ts >> 24)}
    	_, err = sock.WriteToUDP(pkt, addr)
    	if err != nil {
    		panic(err)
    	}
    }
  } ()
  return ch
}

func handlePacket(addr *net.UDPAddr, msg []byte, ch chan HamiltonMessage) {
  serial := (uint16(addr.IP[14]) << 8) + uint16(addr.IP[15])
  sstr := fmt.Sprintf("%04x", serial)
  //First four bytes are echo ID
  eid := uint32(msg[0]) + (uint32(msg[1]) << 8) + (uint32(msg[2]) << 16) + (uint32(msg[3]) << 24)
  tablock.Lock()
  lastid, ok := eidTable[addr.String()]
  if !ok || lastid < eid {
    eidTable[addr.String()] = eid
    tablock.Unlock()
  } else {
    tablock.Unlock()
    return
  }
  //Then
  vx := int8(msg[4])
  vy := int8(msg[5])
  vz := int8(msg[6])
  vax := int8(msg[7])
  vay := int8(msg[8])
  vaz := int8(msg[9])
  temp := int32( uint32(msg[10]) + (uint32(msg[11]<<8)) + (uint32(msg[12]<<16)) + (uint32(msg[13]<<24)) )
  tF := (float64(temp)/10000.0) * 1.8 + 32
  tmSince2016 := uint32( uint32(msg[14]) + (uint32(msg[15]<<8)) + (uint32(msg[16]<<16)) + (uint32(msg[17]<<24)) )
  tm := uint64( (uint64(tmSince2016 * 100) + EPOCH) * 1000000 )
  obj := map[string]interface{} {
      "#": serial,
      "A": []int8{vx, vy, vz},
      "MA": []int8{vax, vay, vaz},
      "T" : temp,
      "F" : tF,
      "W" : tm,
    }
  po, err := bw.CreateMsgPackPayloadObject(bw.PONumHamiltonTelemetry, obj)
  if err != nil {
    panic(err)
  }
  ch <- HamiltonMessage{
    sstr,
    []bw.PayloadObject{po},
  }
}

func genDemoPackets(ch chan HamiltonMessage) {
  for {
    time.Sleep(2*time.Second)
    vx := int8(10)
    vy := int8(20)
    vz := int8(30)
    sstr := "FFFF"
    obj := map[string]interface{} {
        "#": 0xFFFF,
        "A": []int8{vx, vy, vz},
      }
    po, err := bw.CreateMsgPackPayloadObject(bw.PONumHamiltonTelemetry, obj)
    if err != nil {
      panic(err)
    }
    ch <- HamiltonMessage{
      sstr,
      []bw.PayloadObject{po},
    }
  }
}
