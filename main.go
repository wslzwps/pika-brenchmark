// Copyright 2019 XNG-gopher. All Rights Reserved.
// Licensed will be under the XNG (XNG-LICENSE.txt) license.

package main

import (
	"flag"
	"fmt"
	"github.com/gomodule/redigo/redis"
	"sync"
	"time"
)

var (
	//Waiting for all tasks to be completed
	wg sync.WaitGroup

	//Record the number of slow requests with response time greater than 2 seconds
	slowRequestCount = 0

	//Count the time-cost results of each request.The array index by time.
	//Only response time less than 2 seconds are recorded.
	spendCounter     = make([]int, 2000)
	spendCounterLock sync.Mutex
)

var (
	host = flag.String("h", "127.0.0.1", "-h hostname")
	port = flag.Int("p", 6379, "-p 6379")

	//Single operation, no combination.
	//Only one command can be executed at a time. for example -t zdd  or -t zrank .
	//zdd should be executed before zrank, otherwise zrank will be executed empty.
	opType = flag.String("t", "zadd", "-t [zadd/zrem/zrank...]")

	requestNum      = flag.Int("n", 200000, "-n xxx")
	poolSize        = flag.Int("z", 20, "-z [size]")
	concurrentCount = flag.Int("c", 20, "-p xxx")

	//You can self-define Key for zset and set.by -k
	key = flag.String("k", "pikatest", "-k pikatest")
)

func main() {
	flag.CommandLine.Usage = func() { usage() }
	flag.Parse()

	connPool := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", fmt.Sprintf("%s:%d", *host, *port))
		if err != nil {
			panic(err.Error())
			return nil, err
		}
		return c, nil
	}, *poolSize)

	offset := 0
	partSize := *requestNum / (*concurrentCount)

	//totalSpendSeconds: record the total costs of time
	totalSpendSeconds := time.Now().UnixNano()
	for i := 0; i < *concurrentCount; i++ {
		wg.Add(1)
		go stressSet(connPool.Get(), offset, partSize)
		offset += partSize
	}
	wg.Wait()
	totalSpendSeconds = (time.Now().UnixNano() - totalSpendSeconds) / 1e9
	reportResult(totalSpendSeconds)
}

/*
The index of bucket represents time. It's like counting sort.

Example:
	request A takes 2 ms. make the index[index=2] of bucket increase 1.

If a slow request is greater than 2 seconds[2000ms]. count it directly into slowRequestCount
*/
func spendCounterInc(index int) {
	spendCounterLock.Lock()
	defer spendCounterLock.Unlock()

	if index >= len(spendCounter) {
		slowRequestCount++
		return
	}
	spendCounter[index]++
}

/*
Wrap for cmd exec function. To make it easy to see
the diff in parameters of different commands[sadd,sismember,zrank,zscore and so on]
*/
func action(c redis.Conn, args ...interface{}) {
	if _, err := c.Do(*opType, args...); err != nil {
		panic(err.Error())
	}
}

func reportResult(totalSpendSecond int64) {
	qps := int64(0)
	if totalSpendSecond > 0 {
		qps = int64(*requestNum) / totalSpendSecond
	} else {
		qps = int64(*requestNum)
	}
	titles := []string{
		fmt.Sprintf("================== %s =================", *opType),
		fmt.Sprintf("%d requests completed in %d seconds", *requestNum, totalSpendSecond),
		fmt.Sprintf("%d parallel clients", *concurrentCount),
		fmt.Sprintf("%d query pre seconds", qps),
	}

	for _, line := range titles {
		fmt.Println(line)
	}
	fmt.Println("")

	sum := 0
	for i := 0; i < len(spendCounter); i++ {
		if sum >= *requestNum {
			break
		}

		if spendCounter[i] == 0 {
			continue
		}
		sum += spendCounter[i]

		//format: [0.03% <= 1 milliseconds]
		fmt.Println(fmt.Sprintf("%.2f%s <= %d milliseconds", 100.0*(float64(sum)/float64(*requestNum)), "%", i))
	}
}

func usage() {
	helpWords := []string{"usage: redis-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests>] [-k <boolean>]",
		"-h <hostname>      Server hostname (default 127.0.0.1)",
		"-p <port>          Server port (default 6379)",
		"-s <socket>        Server socket (overrides host and port)",
		"-a <password>      Password for Redis Auth",
		"-c <clients>       Number of parallel connections (default 50)",
		"-n <requests>      Total number of requests (default 100000)",
		"-d <size>          Data size of SET/GET value in bytes (default 3)",
		"--dbnum <db>       SELECT the specified db number (default 0)",
		"-k <boolean>       1=keep alive 0=reconnect (default 1)",
		"-r <keyspacelen>   Use random keys for SET/GET/INCR, random values for SADD",
		"	Using this option the benchmark will expand the string __rand_int__",
		"	inside an argument with a 12 digits number in the specified range",
		"	from 0 to keyspacelen-1. The substitution changes every time a command",
		"	is executed. Default tests use this to hit random keys in the",
		"	specified range.",
		"-P <numreq>        Pipeline <numreq> requests. Default 1 (no pipeline).",
		"-e                 If server replies with errors, show them on stdout.",
		"(no more than 1 error per second is displayed)",
		"-q                 Quiet. Just show query/sec values",
		"--csv              Output in CSV format",
		"-l                 Loop. Run the tests forever",
		"-t <tests>         Only run the comma separated list of tests. The test",
		"names are the same as the ones produced as reportResult.",
		"-I                 Idle mode. Just open N idle connections and wait.",
		"",
		"Examples:",
		"Run the benchmark with the default configuration against 127.0.0.1:6379:",
		"$ redis-benchmark",
		"Use 20 parallel clients, for a total of 100k requests, against 192.168.1.1:",
		"$ redis-benchmark -h 192.168.1.1 -p 6379 -n 100000 -c 20",
		"Fill 127.0.0.1:6379 with about 1 million keys only using the SET test:",
		"$ redis-benchmark -t set -n 1000000 -r 100000000",
		"Benchmark 127.0.0.1:6379 for a few commands producing CSV reportResult:",
		"$ redis-benchmark -t ping, set, get -n 100000 --csv",
		"Benchmark a specific command line:",
		"$ redis-benchmark -r 10000 -n 10000 eval 'return redis.call(\"ping\")' 0",
		"Fill a list with 10000 random elements:",
		"$ redis-benchmark -r 10000 -n 10000 lpush mylist __rand_int__",
		"On user specified command lines __rand_int__ is replaced with a random integer",
		"with a range of values selected by the -r option.",
	}
	for _, line := range helpWords {
		fmt.Println(line)
	}
}
