// Copyright 2019 XNG-gopher. All Rights Reserved.
// Licensed will be under the XNG (XNG-LICENSE.txt) license.

package main

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"time"
)

const (
	opZadd   = "zadd"
	opZrem   = "zrem"
	opZrank  = "zrank"
	opZscore = "zscore"
)

//should not release conn in stressZset.
//who Allocates who Releases...please follow this rule
//but I am not happy today.
func stressZset(c redis.Conn, offset, count int) {
	defer wg.Done()

	for i := 0; i < count; i++ {
		member := fmt.Sprintf("%s%d", *key, offset+i)

		start := time.Now().UnixNano()

		if *opType == opZadd {
			action(c, *key, offset+i, member)
		} else if *opType == opZrank || *opType == opZrem {
			action(c, *key, member)
		}
		end := time.Now().UnixNano()
		spend := (end - start) / 1e6
		spendCounterInc(int(spend))
	}
	c.Close()
}
