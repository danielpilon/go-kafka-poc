package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	m := flag.String("m", "", "indicates whether a producer (p) or a consumer (c) should be started")
	a := flag.String("a", "localhost:9092", "the kafka host to connect to")
	t := flag.String("t", "foo", "the topic to produce/consume")
	p := flag.Int("p", 0, "the partition to produce/consume")

	flag.Parse()

	switch *m {
	case "p":
		produce(*a, *t, *p)
	case "c":
		consume(*a, *t, *p)
	default:
		log.Fatalf("'%v' is not a valid mode.", *m)
	}
}

func produce(address string, topic string, partition int) {
	log.Println("Starting producer...")

	conn, _ := kafka.DialLeader(context.Background(), "tcp", address, topic, partition)

	mesRdr := make(chan string)

	go readStdin(mesRdr)

	for {
		mes := <-mesRdr
		if mes == "\\q" {
			break
		}
		_, err := conn.WriteMessages(
			kafka.Message{Value: []byte(mes)},
		)

		if err != nil {
			log.Println(err)
		}
	}

	log.Println("Stopping producer...")

	conn.Close()
}

func readStdin(input chan<- string) {
	for {
		var u string
		_, err := fmt.Scanf("%v\n", &u)
		if err != nil {
			panic(err)
		}
		input <- u
	}
}

func consume(address string, topic string, partition int) {

}
