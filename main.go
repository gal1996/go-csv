package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

const (
	fileName = "source.csv"
)

func main() {
	rand.Seed(time.Now().UnixNano())
	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	buf := bytes.NewBuffer(nil)
	_r := io.TeeReader(f, buf)
	ra := csv.NewReader(_r)
	allRow, err := ra.ReadAll()
	if err != nil {
		log.Fatal(err)
	}
	rowCount := len(allRow)

	r := csv.NewReader(buf)
	retryQueue := make(chan rowWithRetryCount, rowCount)
	succeedRowCh := make(chan struct{}, rowCount)
	failedRowCh := make(chan struct{}, rowCount)
	// 実行回数は行数よりもretryする分多くなるので
	execCountCh := make(chan struct{}, rowCount*10)
	wg := &sync.WaitGroup{}
	for {
		record, err := r.Read()
		if err == io.EOF {
			fmt.Println("読み込み終わり")
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("取り出したデータはこれ: %+v\n", record)
		wg.Add(1)
		go doSomething(newRowWithRetryCount(record, 0), retryQueue, succeedRowCh, failedRowCh, execCountCh, wg)
	}

	go func() {
		for v := range retryQueue {
			go doSomething(v, retryQueue, succeedRowCh, failedRowCh, execCountCh, wg)
		}
	}()
	wg.Wait()
	close(retryQueue)
	close(succeedRowCh)
	close(failedRowCh)
	close(execCountCh)

	log.Printf("execution count: %d\n", len(execCountCh))
	log.Printf("failed %d rows.\n", len(failedRowCh))
	log.Printf("%d rows succeeded!\n", len(succeedRowCh))
}

func doSomething(rowData rowWithRetryCount, retryQueue chan rowWithRetryCount, succeedRowCh, failedRowCh, execCountCh chan struct{}, wg *sync.WaitGroup) {
	execCountCh <- struct{}{}
	if rowData.retryCount > 9 {
		// 10回retryしてもダメだったときは、その行は失敗として完了させる
		failedRowCh <- struct{}{}
		wg.Done()
		return
	}
	ms := rand.Intn(10)*100 + 1
	time.Sleep(time.Duration(ms) * time.Millisecond)
	n := rand.Intn(100)
	if 0 <= n && n < 5 {
		// 5%の確率で失敗したらexecQueueに積み直す
		fmt.Printf("この行は失敗！: %+v\n", rowData.row)
		retryQueue <- newRowWithRetryCount(rowData.row, rowData.retryCount+1)
		// 失敗したときはwg.Done()しないので、成功するまでwaitされる
		return
	}

	fmt.Printf("この行は成功！: %+v\n", rowData.row)
	succeedRowCh <- struct{}{}
	wg.Done()
	return
}

func newRowWithRetryCount(row []string, count int) rowWithRetryCount {
	return rowWithRetryCount{row, count}
}

type rowWithRetryCount struct {
	row        []string
	retryCount int
}
