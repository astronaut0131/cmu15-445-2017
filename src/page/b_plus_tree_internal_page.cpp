/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetSize(0);
  // 20 stands for header size
  // no virtual function thus no 4 bytes vptr
  this->SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeInternalPage)) /
                   sizeof(MappingType));
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  int size = this->GetSize();
  for (int i = 0; i < size; i++) {
    if (array[i].second == value)
      return i;
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
  int size = this->GetSize();
  for (int i = 1; i < size; i++) {
    // todo:binary search?
    if (comparator(key, array[i].first) == -1)
      return array[i - 1].second;
  }
  return array[size - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  if (this->IsRootPage()) {
    array[0].second = old_value;
    array[1].first = new_key;
    array[1].second = new_value;
    SetSize(2);
  }
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  int index = ValueIndex(old_value);
  if (index != -1) {
    index++;
    int size = GetSize();
    for (int i = size; i > index; i--) {
      array[i] = std::move(array[i - 1]);
    }
    this->IncreaseSize(1);
    array[index] = std::make_pair(new_key, new_value);
  }
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int half_size = size / 2;
  recipient->CopyHalfFrom(&array[half_size], size - half_size,
                          buffer_pool_manager);
  SetSize(half_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  page_id_t this_page_id = GetPageId();
  for (int i = 0; i < size; i++) {
    array[i] = std::move(items[i]);
    page_id_t child_page_id = array[i].second;
    // update child nodes' parent_id
    auto child_page = buffer_pool_manager->FetchPage(child_page_id);
    if (child_page == nullptr)
      throw std::runtime_error("fail to fetch page");
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    child_node->SetParentPageId(this_page_id);
    buffer_pool_manager->UnpinPage(child_page_id, true);
  }
  SetSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  int size = GetSize();
  for (int i = index + 1; i < size; i++) {
    array[i - 1] = std::move(array[i]);
  }
  SetSize(size - 1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  auto v = ValueAt(1);
  Remove(0);
  Remove(1);
  return v;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
  auto parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (parent_page == nullptr)
    throw std::runtime_error("fail to fetch page");
  auto parent = reinterpret_cast<
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      parent_page->GetData());
  array[0].first = parent->KeyAt(index_in_parent);
  buffer_pool_manager->UnpinPage(parent->GetPageId(), false);
  recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  page_id_t page_id = GetPageId();
  for (int i = 0; i < size; i++) {
    array[size + i] = items[i];
    auto page = buffer_pool_manager->FetchPage(items[i].second);
    if (page == nullptr)
      throw std::runtime_error("fail to fetch page");
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    node->SetParentPageId(page_id);
    buffer_pool_manager->UnpinPage(node->GetPageId(), true);
  }
  SetSize(GetSize() + size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
/* I think this API is confusing ... */
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  auto pair = std::make_pair(array[1].first, array[0].second);
  Remove(0);
  recipient->CopyLastFrom(pair, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  auto parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (parent_page == nullptr)
    throw std::runtime_error("fail to fetch page");
  auto parent = reinterpret_cast<
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      parent_page->GetData());
  array[size] = std::make_pair(parent->KeyAt(1), pair.second);
  SetSize(size + 1);
  parent->SetKeyAt(1, pair.first);
  buffer_pool_manager->UnpinPage(parent->GetPageId(), true);
  // update parent for the moved child
  auto page = buffer_pool_manager->FetchPage(pair.second);
  if (page == nullptr)
    throw std::runtime_error("fail to fetch page");
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(pair.second, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  auto pair = array[size - 1];
  Remove(size - 1);
  recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  auto parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (parent_page == nullptr)
    throw std::runtime_error("fail to fetch page");
  auto parent = reinterpret_cast<
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      parent_page->GetData());
  int size = GetSize();
  for (int i = size; i > 0; i--) {
    array[i] = std::move(array[i - 1]);
  }
  array[0].second = pair.second;
  array[1].first = parent->KeyAt(parent_index);
  SetSize(size + 1);
  parent->SetKeyAt(parent_index, pair.first);
  buffer_pool_manager->UnpinPage(parent->GetParentPageId(), true);
  // update parent for the moved child
  auto page = buffer_pool_manager->FetchPage(pair.second);
  if (page == nullptr)
    throw std::runtime_error("fail to fetch page");
  auto node = reinterpret_cast<
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      page->GetData());
  node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(node->GetPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                     GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                     GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                     GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                     GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                     GenericComparator<64>>;
} // namespace cmudb
