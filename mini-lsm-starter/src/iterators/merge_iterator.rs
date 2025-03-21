#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;
use std::env::current_exe;
use std::thread::current;
use std::usize;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap: BinaryHeap<HeapWrapper<I>> = BinaryHeap::new();
        for (idx, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(idx, iter));
            }
        }

        let current = heap.pop();
        Self {
            iters: heap,
            current: current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|iter| iter.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        let current_iter = self.current.as_mut().unwrap();

        // go through the heap starting from the top. Call next on the item if it's current key is the same as
        // the key of current_iter. then check if it's valid. If it's not valid remove it from the heap
        while let Some(mut heap_top) = self.iters.peek_mut() {
            if heap_top.1.key() == current_iter.1.key() {
                if let Err(e) = heap_top.1.next() {
                    PeekMut::pop(heap_top);
                    return Err(e);
                }

                if !heap_top.1.is_valid() {
                    PeekMut::pop(heap_top);
                }
            } else {
                break;
            }
        }

        current_iter.1.next()?;

        if !current_iter.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current_iter = iter;
            }
            return Ok(());
        }

        /* 
        The comparison here looks weird but it's right.
        When the keys are different we want the samllest key to be current_iter.
        But the cmp operation defined for HeapWrapper above reverses the ordering
        
        So, if a < b then a is small, but we want a to be weighted more so we reverse the ordering 
        and then it returns Ordering::Greater.

        That is why we only do the swap if current_iter > heap_top and not the other way round.
        It means either the key for current_iter is larger or it's the same as heap_top but it's index it larger. 
        */ 
        if let Some(mut heap_top) = self.iters.peek_mut() {
            if *current_iter < *heap_top {
                std::mem::swap(current_iter, &mut *heap_top);
            }
        }

        Ok(())
    }
}
