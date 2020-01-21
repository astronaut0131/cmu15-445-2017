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
  explicit Bucket(size_t size,int local_depth):local_depth_(local_depth),size_(0),capacity_(size) {
    items_.resize(size);
    exists_.resize(size);
    std::fill(exists_.begin(),exists_.end(),false);
  }
  bool Full() {
    lock_guard<mutex> guard(latch_);
    return size_ == capacity_;
  }
  // call Full() before Insert
  void Insert(const K& key, const V& value) {
    lock_guard<mutex> guard(latch_);
    for (size_t i = 0; i < capacity_; i++) {
      if (!exists_[i]) {
        exists_[i] = true;
        size_++;
        items_[i].first = key;
        items_[i].second = value;
        return;
      }
    }
  }
  bool Find(const K&key, V& value) {
    lock_guard<mutex> guard(latch_);
    for (size_t i = 0; i < capacity_; i++) {
      if (exists_[i] && items_[i].first == key) {
        value = items_[i].second;
        return true;
      }
    }
    return false;
  }
  bool Remove(const K&key) {
    lock_guard<mutex> guard(latch_);
    auto it = items_.begin();
    for (;it != items_.end(); it++) {
      if (it->first == key) break;
    }
    if (it == items_.end()) return false;
    else {
      size_--;
      exists_[it - items_.begin()] = false;
      return true;
    }
  }
  vector<pair<K,V>> GetItems() {
    vector<pair<K,V>> items;
    for (size_t i = 0; i < capacity_; i++) {
      if (exists_[i]) {
        items.emplace_back(items_[i]);
      }
    }
    return items;
  }
  void Clear() {
    size_ = 0;
    fill(exists_.begin(),exists_.end(),false);
  }
  int GetLocalDepth() {
    lock_guard<mutex> guard(latch_);
    return local_depth_;
  }
  void IncLocalDepth() {
    lock_guard<mutex> guard(latch_);
    local_depth_++;
  }

private:
  int local_depth_;
  size_t size_;
  size_t capacity_;
  vector<pair<K,V>> items_;
  vector<bool> exists_;
  mutex latch_;
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
  
  // add your own member variables here
  vector<shared_ptr<Bucket<K,V>>> buckets_table_;
  std::atomic_int global_depth_;
  std::atomic_int num_buckets_;
  size_t bucket_capacity_;
  mutex latch_;
};
} // namespace cmudb
