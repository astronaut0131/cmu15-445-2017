/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* current_leaf,BufferPoolManager* buffer_pool_manager,int index):
current_leaf_(current_leaf),index_(index),buffer_pool_manager_(buffer_pool_manager),is_end_(false) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool IndexIterator<KeyType, ValueType, KeyComparator>::isEnd() {
  return is_end_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
IndexIterator<KeyType,ValueType,KeyComparator> &IndexIterator<KeyType, ValueType, KeyComparator>::operator++() {
  if (index_ < current_leaf_->GetSize()-1) index_++;
  else if (current_leaf_->GetNextPageId() == INVALID_PAGE_ID) is_end_ = true;
  else {
    auto next_page = buffer_pool_manager_->FetchPage(current_leaf_->GetNextPageId());
    if (next_page == nullptr) throw std::runtime_error("fail to fetch page");
    // unpin the last page
    buffer_pool_manager_->UnpinPage(current_leaf_->GetPageId(),false);
    current_leaf_ = reinterpret_cast<decltype(current_leaf_)> (next_page->GetData());
    index_ = 0;
  }
  return *this;
}
template <typename KeyType, typename ValueType, typename KeyComparator>
const pair<KeyType, ValueType> &
    IndexIterator<KeyType, ValueType, KeyComparator>::operator*() {
  return current_leaf_->GetItem(index_);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
