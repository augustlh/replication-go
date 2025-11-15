package genericatomic

import "sync"

type GenericAtomic[T comparable] struct {
	value T
	mu sync.Mutex
}

func (atm *GenericAtomic[T]) Store(value T) {
	atm.mu.Lock();
	atm.value = value;
	atm.mu.Unlock();
}

func (atm *GenericAtomic[T]) Load() T {
	atm.mu.Lock();
	tmp := atm.value;
	atm.mu.Unlock();

	return tmp;
}

func (atm *GenericAtomic[T]) CompareAndSwap(before T, new T) bool {
	atm.mu.Lock();

	var success = false;
	if atm.value == before {
		atm.value = new;
		success = true;
	}

	atm.mu.Unlock();

	return success;
}

