package diodes

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

// AtomicValue is a non-blocking, single-slot buffer designed for many writers
// (multiple goroutines calling Set) and a single reader (one goroutine calling TryNext).
// When multiple writers call Set, only the last value written is retained,
// effectively discarding any intermediate values.
// The reader consumes the value from the slot, making it empty until a new value is set.
// This implementation is not thread-safe for multiple concurrent readers.
type AtomicValue struct {
	buffer unsafe.Pointer

	readChannel   chan struct{}
	closedChannel chan struct{}

	closedChannelIsClosed      bool
	closedChannelIsClosedMutex sync.RWMutex
}

// NewAtomicValue creates a new single-slot AtomicValue buffer.
// It initializes the internal channels for signaling.
func NewAtomicValue() *AtomicValue {
	d := &AtomicValue{
		readChannel:   make(chan struct{}, 1),
		closedChannel: make(chan struct{}),
	}

	return d
}

// Set stores the data in the single buffer slot, overwriting any previous value.
//
//go:nosplit
func (d *AtomicValue) Set(data GenericDataType) {
	newBucket := &bucket{
		data: data,
	}

	atomic.StorePointer(&d.buffer, unsafe.Pointer(newBucket))

	return
}

// TryNext attempts to read the data from the buffer.
// If data is present, it is returned and the buffer slot is cleared (consumed).
// If there is no data available, it will return (nil, false).
//
//go:nosplit
func (d *AtomicValue) TryNext() (data GenericDataType, ok bool) {
	result := (*bucket)(atomic.SwapPointer(&d.buffer, nil))

	// When the result is nil that means the writer has not had the
	// opportunity to write a value into the diode. This value must be ignored
	// and the read head must not increment.
	if result == nil {
		return nil, false
	}

	return result.data, true
}

func (d *AtomicValue) Close() {
	d.closedChannelIsClosedMutex.Lock()
	defer d.closedChannelIsClosedMutex.Unlock()

	if !d.closedChannelIsClosed {
		d.closedChannelIsClosed = true

		close(d.closedChannel)
	}
}

func (d *AtomicValue) GetReadChannel() chan struct{} {
	return d.readChannel
}

func (d *AtomicValue) GetClosedChannel() chan struct{} {
	return d.closedChannel
}
