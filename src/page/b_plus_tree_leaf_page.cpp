/**
 * b_plus_tree_leaf_page.cpp
 */

#include <include/page/b_plus_tree_internal_page.h>
#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
	this->SetPageId(page_id);
	this->SetParentPageId(parent_id);
	this->SetPageType(IndexPageType::LEAF_PAGE);
	this->SetSize(0);
	this->SetNextPageId(INVALID_PAGE_ID);
	this->SetMaxSize((PAGE_SIZE - sizeof(BPlusTreeLeafPage))/ sizeof(MappingType));
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
  int size = this->GetSize();
  for (int i = 0; i < size; i++) {
  	if (comparator(array[i].first,key) >= 0) return i;
  }
  return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  if (index < 0 || index >= GetSize()) throw std::runtime_error("out of range");
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
  if (this->GetSize() == 0) {
  	this->IncreaseSize(1);
  	array[0] = std::make_pair(key,value);
  	return this->GetSize();
  }
  int index = KeyIndex(key,comparator);
  if (comparator(array[index].first,key) == 0) return this->GetSize();

  int size = this->GetSize();
  if (index == -1) {
  	array[size] = std::make_pair(key,value);
  } else {
        for (int i = size; i != index; i--) {
          array[i] = std::move(array[i-1]);
        }
  	array[index] = std::make_pair(key,value);
  }
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
	int size = GetSize();
	int half_size = size / 2;
	recipient->CopyHalfFrom(&array[half_size],size - half_size);
	SetSize(half_size);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
	for (int i = 0; i < size; i++) {
		array[i] = std::move(items[i]);
	}
	SetSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
  int index = KeyIndex(key,comparator);
  if (comparator(array[index].first,key) == 0) {
  	value = array[index].second;
  	return true;
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
	int index = this->KeyIndex(key,comparator);
	if (comparator(array[index].first,key) == 0) {
		// exist
		int size = GetSize();
		for (int i = index+1;i < size; i++) {
			array[i-1] = std::move(array[i]);
		}
		this->SetSize(this->GetSize()-1);
	}
	return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int index_in_parent, BufferPoolManager *buffer_pool_manager) {
	recipient->CopyAllFrom(array,GetSize());
	recipient->SetNextPageId(GetNextPageId());
	SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
        int this_size = GetSize();
        for (int i = 0; i < size; i++) {
		array[i+this_size] = std::move(items[i]);
	}
	SetSize(size+GetSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
	auto pair = GetItem(0);
	recipient->CopyLastFrom(pair);
	int size = this->GetSize();
	for (int i = 1; i < size; i++) array[i-1] = std::move(array[i]);
	SetSize(size-1);
	auto parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
	if (parent_page == nullptr) throw std::runtime_error("fail to fetch page");
	auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(parent_page->GetData());
	parent->SetKeyAt(1,GetItem(0).first);
	buffer_pool_manager->UnpinPage(parent->GetPageId(),true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
	int size = GetSize();
	array[size] = item;
	SetSize(size+1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
	int size = GetSize();
	auto pair = GetItem(size-1);
	SetSize(size-1);
	recipient->CopyFirstFrom(pair,parentIndex,buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
	auto parent_page = buffer_pool_manager->FetchPage(GetParentPageId());
	if (parent_page == nullptr) throw std::runtime_error("fail to fetch page");
	auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(parent_page->GetData());
	parent->SetKeyAt(parentIndex,item.first);
	buffer_pool_manager->UnpinPage(parent->GetPageId(),true);
	int size = GetSize();
	for (int i = size; i > 0 ; i--) {
		array[i] = std::move(array[i-1]);
	}
	array[0] = item;
	SetSize(size+1);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
