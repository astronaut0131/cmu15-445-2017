/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = find(list_.begin(),list_.end(),value);
  if (it != list_.end()) {
    list_.erase(it);
  }
  list_.emplace_back(value);
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> guard(latch_);
  if (list_.empty()) return false;
  value = list_.front();
  list_.pop_front();
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> guard(latch_);
  auto it = find(list_.begin(),list_.end(),value);
  if (it == list_.end()) return false;
  list_.erase(it);
  return true;
}

template <typename T> size_t LRUReplacer<T>::Size() {
  std::lock_guard<std::mutex> guard(latch_);
  return list_.size();
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
