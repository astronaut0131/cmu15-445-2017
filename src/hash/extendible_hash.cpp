#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

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
  return hash_func(key) % buckets_table_.size();
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
  return buckets_table_[bucket_id]->GetLocalDepth();
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
  return buckets_table_[HashKey(key)]->Find(key, value);
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  return buckets_table_[HashKey(key)]->Remove(key);
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  int index = HashKey(key);
  if (buckets_table_[index]->Full()) {
    if (buckets_table_[index]->GetLocalDepth() == global_depth_) {
      global_depth_++;
      size_t origin_size = buckets_table_.size();
      buckets_table_.resize(2*origin_size);
      for (size_t i = origin_size; i < 2 * origin_size; i++) {
        buckets_table_[i] = buckets_table_[i - origin_size];
      }
    }
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
    for (auto& kv:items) {
      Insert(kv.first,kv.second);
    }
    Insert(key,value);
  } else {
    buckets_table_[index]->Insert(key,value);
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
