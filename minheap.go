package main

import "container/heap"

// MinHeap is a min heap implementation with a fixed capacity
type MinHeap struct {
	values []Record
	cap    int
}

// NewMinHeap creates a new MinHeap with the specified capacity
func NewMinHeap(capacity int) *MinHeap {
	return &MinHeap{
		cap: capacity,
	}
}

// Push adds a new element to the heap
func (h *MinHeap) Push(x interface{}) {
	value := x.(Record)
	if len(h.values) < h.cap {
		// If the heap is not at capacity, simply append the new element
		h.values = append(h.values, value)
		heap.Fix(h, len(h.values)-1)
	} else if value.Size > h.values[0].Size {
		// If the new element is greater than the smallest element, replace the smallest element
		h.values[0] = value
		heap.Fix(h, 0)
	}
}

// Pop removes and returns the minimum element from the heap
func (h *MinHeap) Pop() interface{} {
	if len(h.values) == 0 {
		return nil
	}
	min := h.values[0]
	lastIndex := len(h.values) - 1
	h.values[0], h.values[lastIndex] = h.values[lastIndex], h.values[0]
	h.values = h.values[:lastIndex]
	heap.Fix(h, 0)
	return min
}

// Len returns the number of elements in the heap
func (h *MinHeap) Len() int {
	return len(h.values)
}

// Less defines the comparison function for the heap
func (h *MinHeap) Less(i, j int) bool {
	return h.values[i].Size < h.values[j].Size // Min heap
}

// Swap swaps elements in the heap
func (h *MinHeap) Swap(i, j int) {
	h.values[i], h.values[j] = h.values[j], h.values[i]
}

func (h *MinHeap) siftDown(currentIdx int, endIdx int) {
	leftChildIdx := currentIdx*2 + 1
	for leftChildIdx <= endIdx {
		rightChildIdx := currentIdx*2 + 2
		if rightChildIdx > endIdx {
			rightChildIdx = -1
		}

		// get the smaller child node to swap
		idxToSwap := leftChildIdx
		if rightChildIdx != -1 && h.values[rightChildIdx].Size < h.values[leftChildIdx].Size {
			idxToSwap = rightChildIdx
		}

		// check if value of swap node is less than the value at currentIdx
		if h.values[idxToSwap].Size < h.values[currentIdx].Size {
			h.Swap(idxToSwap, currentIdx)
			currentIdx = idxToSwap
			leftChildIdx = currentIdx*2 + 1

		} else {
			return
		}
	}
}

func (h *MinHeap) siftUp() {
	currentIdx := len(h.values) - 1
	parentIdx := (currentIdx - 1) / 2
	for currentIdx > 0 && h.values[currentIdx].Size < h.values[parentIdx].Size {
		h.Swap(currentIdx, parentIdx)
		currentIdx = parentIdx
		parentIdx = (currentIdx - 1) / 2
	}
}

// Time: O(logn) | Space: O(1)
// insert a new value to the end of the tree and update heap ordering
func (h *MinHeap) Insert(value Record) {
	if len(h.values) < h.cap {
		// If the heap is not at capacity, simply append the new element
		h.values = append(h.values, value)
		h.siftUp()
	} else if value.Size > h.values[0].Size {
		// If the new element is greater than the smallest element, replace the smallest element
		h.values[0] = value
		h.siftDown(0, len(h.values)-1)
	}
}

// Time: O(logn) | Space: O(1)
// remove and return the minimum value and update heap ordering
func (h *MinHeap) Remove() Record {
	n := len(h.values)
	// swap the first element and the last element in the array
	h.Swap(0, n-1)
	valueToRemove := h.values[n-1]
	// pop the last element in the array
	h.values = h.values[:n-1]
	// call siftDown to update heap ordering
	h.siftDown(0, n-2)

	return valueToRemove
}
