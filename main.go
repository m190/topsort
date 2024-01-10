package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const ResultMaxSize = 10
const ReadChunkSize = 100_000_000
const MaxReadRoutines = 100
const MaxMergingRoutines = 100

func main() {
	args := os.Args

	var filePath string
	if len(args) < 2 {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Print("Enter the absolute path of a file: ")
		if !scanner.Scan() {
			return
		}
		filePath = scanner.Text()
		filePath = strings.TrimSpace(filePath)
	} else {
		filePath = args[1]
	}

	_, err := findLargest(filePath, ResultMaxSize)
	if err != nil {
		fmt.Println(err)
	} /*else {
		for _, r := range result {
			fmt.Println(r)
		}
	}*/
}

type Record struct {
	Link string
	Size int64
}

func NewRecord(raw string) Record {
	data := strings.Split(raw, " ")
	link := data[0]
	size, _ := strconv.ParseInt(data[1], 10, 64)
	return Record{Link: link, Size: size}
}

func findLargest(filePath string, resultSize int) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	// reading
	read := make(chan struct{}, 1)
	partialResults := make(chan []Record, MaxReadRoutines)
	go func() {
		readings := make(chan struct{}, MaxReadRoutines) // semafore
		var wg sync.WaitGroup

		for i := int64(0); i < fileSize; i += ReadChunkSize {
			readings <- struct{}{} // acquire
			wg.Add(1)
			go func(i int64) {
				defer wg.Done()
				candidates, err := findLargestInChunk(filePath, i, i+ReadChunkSize, resultSize)
				if err != nil {
					fmt.Println("Error getting partial result", err)
					return
				}

				if len(candidates) > 0 {
					partialResults <- candidates
				}

				<-readings // release
			}(i)
		}

		wg.Wait()
		read <- struct{}{}
	}()

	// merging
	reading := true
	merges := make(chan struct{}, MaxMergingRoutines) // semafore
	processingBuffer := make([][]Record, 0, 2)
	for reading || len(partialResults) > 0 || len(merges) > 0 {
		select {
		case r := <-partialResults:
			processingBuffer = append(processingBuffer, r)
			if len(processingBuffer) == 2 {
				merges <- struct{}{} // acquire
				r1 := processingBuffer[0]
				r2 := processingBuffer[1]
				processingBuffer = make([][]Record, 0, 2)
				go func() {
					partialResults <- mergeResults(r1, r2, resultSize)
					<-merges // release
				}()
			}
		case <-read:
			reading = false
		}
	}

	if len(processingBuffer) == 0 {
		return nil, errors.New("No results")
	} else {
		result := make([]string, 0, len(processingBuffer[0]))
		for _, r := range processingBuffer[0] {
			result = append(result, r.Link)
			fmt.Println(r.Link, r.Size)
		}
		return result, nil
	}
}

func findLargestInChunk(filePath string, startPos, endPos int64, resultSize int) ([]Record, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	pos := startPos

	var scanner *bufio.Scanner
	if pos == 0 {
		scanner = bufio.NewScanner(file)
	} else {
		// set position to the next line to prevent partial read
		_, err = file.Seek(int64(pos), 0)
		if err != nil {
			return nil, err
		}

		scanner = bufio.NewScanner(file)
		if !scanner.Scan() {
			return nil, nil
		}

		pos += int64(len(scanner.Bytes()) + 1) // add 1 for the newline character
	}

	// find largest from chunk
	candidates := make([]Record, 0, resultSize)
	heapCandidates := NewMinHeap(resultSize)
	for pos < endPos {
		read := int64(0)
		// candidates, read = processLine(scanner, candidates, resultSize)
		read = processLineWithHeap(scanner, heapCandidates)
		if read == 0 {
			break
		}
		pos += read
	}
	// read one more line to prevent loosing data due to inaccurate splitting
	// candidates, _ = processLine(scanner, candidates, resultSize)
	processLineWithHeap(scanner, heapCandidates)

	candidates = heapCandidates.values
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Size > candidates[j].Size
	})

	return candidates, nil
}

func processLineWithHeap(scanner *bufio.Scanner, candidates *MinHeap) int64 {
	if !scanner.Scan() {
		return 0
	}

	line := scanner.Text()
	newRecord := NewRecord(line)
	candidates.Push(newRecord)

	return int64(len(scanner.Bytes()) + 1)
}

func processLine(scanner *bufio.Scanner, candidates []Record, resultSize int) ([]Record, int64) {
	if !scanner.Scan() {
		return candidates, 0
	}

	line := scanner.Text()
	newRecord := NewRecord(line)
	candidates = appendElement(candidates, newRecord, resultSize)

	return candidates, int64(len(scanner.Bytes()) + 1)
}

func appendElement(arr []Record, r Record, resultSize int) []Record {
	low, high := 0, len(arr)-1
	pos := -1

	for low <= high {
		mid := low + (high-low)/2

		if arr[mid].Size > r.Size {
			low = mid + 1 // search in the right half
		} else {
			pos = mid
			high = mid - 1 // search in the left half
		}
	}

	if len(arr) < resultSize {
		if pos == -1 {
			return append(arr, r)
		}

		return append(arr[:pos], append([]Record{r}, arr[pos+1:]...)...)
	}

	if pos == -1 {
		return arr
	}

	return append(arr[:pos], append([]Record{r}, arr[pos+1:]...)...)[:resultSize]
}

func mergeResults(arr1, arr2 []Record, resultSize int) []Record {
	result := make([]Record, 0, resultSize)
	i, j := 0, 0

	for len(result) < resultSize && i < len(arr1) && j < len(arr2) {
		if arr1[i].Size >= arr2[j].Size {
			result = append(result, arr1[i])
			i++
		} else {
			result = append(result, arr2[j])
			j++
		}
	}

	// append remaining elements from arr1, if any
	for len(result) < resultSize && i < len(arr1) {
		result = append(result, arr1[i])
		i++
	}

	// append remaining elements from arr2, if any
	for len(result) < resultSize && j < len(arr2) {
		result = append(result, arr2[j])
		j++
	}

	return result
}
