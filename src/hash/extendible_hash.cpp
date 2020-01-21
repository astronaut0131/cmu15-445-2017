#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {


template<typename K,typename V>
Bucket<K,V>::Bucket(size_t size,int local_depth):local_depth_(local_depth),size_(0),capacity_(size) {
  items_.resize(size);
  exists_.resize(size);
  std::fill(exists_.begin(),exists_.end(),false);
}

template<typename K,typename V>
bool Bucket<K,V>::Full() const {
  lock_guard<mutex> guard(latch_);
  return size_ == capacity_;
}

// call Full() before Insert
template<typename K,typename V>
void Bucket<K,V>::Insert(const K& key, const V& value) {
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

/*
 * lookup function to find value associated with input key
 * return true on success and false on failure
 */
template<typename K,typename V>
bool Bucket<K,V>::Find(const K&key, V& value) const{
  lock_guard<mutex> guard(latch_);
  for (size_t i = 0; i < capacity_; i++) {
    if (exists_[i] && items_[i].first == key) {
      value = items_[i].second;
      return true;
    }
  }
  return false;
}

template<typename K,typename V>
bool Bucket<K,V>::Remove(const K&key) {
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

/*
 * helper function to get all kv pairs in a bucket
 * for rehash
 */
template<typename K,typename V>
vector<pair<K,V>> Bucket<K,V>::GetItems() const{
  vector<pair<K,V>> items;
  for (size_t i = 0; i < capacity_; i++) {
    if (exists_[i]) {
      items.emplace_back(items_[i]);
    }
  }
  return items;
}

/*
 * remove all kv pairs from a bucket
 * since a bucket's capacity is never changed
 * just mark all slots invalid
 */
template<typename K,typename V>
void Bucket<K,V>::Clear() {
  size_ = 0;
  fill(exists_.begin(),exists_.end(),false);
}

template<typename K,typename V>
int Bucket<K,V>::GetLocalDepth() const{
  lock_guard<mutex> guard(latch_);
  return local_depth_;
}

template<typename K,typename V>
void Bucket<K,V>::IncLocalDepth() {
  lock_guard<mutex> guard(latch_);
  local_depth_++;
}
/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
    : global_depth_(0), num_buckets_(1), bucket_capacity_(size) {
  buckets_table_.resize(1);
  buckets_table_[0] = std::make_shared<Bucket<K,V>>(size,0);
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  std::hash<K> hash_func;
  latch_.lock();
  size_t size = buckets_table_.size();
  latch_.unlock();
  return hash_func(key) % size;
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  return global_depth_;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  return EntryAt(bucket_id)->GetLocalDepth();
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return num_buckets_;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  return EntryAt(HashKey(key))->Find(key, value);
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  return EntryAt(HashKey(key))->Remove(key);
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  int index = HashKey(key);
  if (EntryAt(index)->Full()) {
    if (EntryAt(index)->GetLocalDepth() == global_depth_) {
      global_depth_++;
      latch_.lock();
      size_t origin_size = buckets_table_.size();
      buckets_table_.resize(2*origin_size);
      for (size_t i = origin_size; i < 2 * origin_size; i++) {
        buckets_table_[i] = buckets_table_[i - origin_size];
      }
      latch_.unlock();
    }
    latch_.lock();
    buckets_table_[index]->IncLocalDepth();
    vector<size_t> indexes;
    for (size_t i = 0; i < buckets_table_.size(); i++) {
      if (buckets_table_[i].get() == buckets_table_[index].get()) {
        indexes.push_back(i);
      }
    }
    auto items = buckets_table_[index]->GetItems();
    buckets_table_[index]->Clear();
    auto new_bucket = std::make_shared<Bucket<K,V>>(bucket_capacity_,buckets_table_[index]->GetLocalDepth());
    num_buckets_++;
    for (size_t i = indexes.size()/2;i < indexes.size(); i++) {
      buckets_table_[indexes[i]] = new_bucket;
    }
    latch_.unlock();
    for (auto& kv:items) {
      Insert(kv.first,kv.second);
    }
    Insert(key,value);
  } else {
    EntryAt(index)->Insert(key,value);
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
