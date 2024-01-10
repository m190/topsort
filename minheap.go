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
