// Copyright 2019 XNG-gopher. All Rights Reserved.
// Licensed will be under the XNG (XNG-LICENSE.txt) license.

package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

const (
	opSadd      = "sadd"
	opSrem      = "srem"
	opSismember = "sismember"
)

func stressSet(c redis.Conn, offset, count int) {
	defer wg.Done()

	for i := 0; i < count; i++ {
		member := fmt.Sprintf("%s%d", *key, offset+i)

		start := time.Now().UnixNano()

		if *opType == opSadd || *opType == opSrem || *opType == opSismember {
			action(c, *key, member)
		}

		end := time.Now().UnixNano()

		spend := (end - start) / 1e6
		spendCounterInc(int(spend))
	}
	c.Close()
}
