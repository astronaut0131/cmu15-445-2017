/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <include/common/logger.h>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <iostream>

#include "hash/hash_table.h"

using std::vector;
using std::unordered_map;
using std::pair;
using std::mutex;
using std::lock_guard;
using std::shared_ptr;

namespace cmudb {
template <typename K, typename V>
class Bucket{
public:
  explicit Bucket(size_t size,int local_depth);
  bool Full() const;
  // call Full() before Insert
  void Insert(const K& key, const V& value);
  bool Find(const K&key, V& value) const;
  bool Remove(const K&key);
  vector<pair<K,V>> GetItems() const;
  void Clear();
  int GetLocalDepth() const;
  void IncLocalDepth();

private:
  int local_depth_;
  size_t size_;
  size_t capacity_;
  vector<pair<K,V>> items_;
  vector<bool> exists_;
  mutable mutex latch_;
};
template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

private:
  // helper function
  // a thread safe way to access buckets_table_
  shared_ptr<Bucket<K,V>> EntryAt(size_t index) const{
    lock_guard<mutex> guard(latch_);
    return buckets_table_[index];
  }
  // add your own member variables here
  // directory to buckets
  vector<shared_ptr<Bucket<K,V>>> buckets_table_;
  std::atomic_int global_depth_;
  std::atomic_int num_buckets_;
  // max capacity for each bucket
  size_t bucket_capacity_;
  // lock to protect buckets_table_
  mutable mutex latch_;
};
} // namespace cmudb
