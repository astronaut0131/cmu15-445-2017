/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                          BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator,
                          page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  if (this->IsEmpty())
    return false;
  auto leaf_node = FindLeafPage(key);
  ValueType value;
  bool exist = leaf_node->Lookup(key, value, comparator_);
  if (exist)
    result.push_back(value);
  buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), false);
  return exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  // create new node
  auto node = NewNode<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  // update root_page_id_
  root_page_id_ = node->GetPageId();
  UpdateRootPageId(true);
  // insert kv to the root node
  node->Insert(key, value, comparator_);
  // unpin the root node
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if
 * necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  auto leaf_node = FindLeafPage(key);
  ValueType v;
  // duplicate key found
  if (leaf_node->Lookup(key, v, comparator_))
    return false;
  // leaf node is not full
  if (leaf_node->GetSize() < leaf_node->GetMaxSize()) {
    leaf_node->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    return true;
  }
  auto new_leaf_node = Split<B_PLUS_TREE_LEAF_PAGE_TYPE>(leaf_node);
  if (comparator_(key,new_leaf_node->KeyAt(0)) < 0)
    leaf_node->Insert(key,value,comparator_);
  else
    new_leaf_node->Insert(key,value,comparator_);
  leaf_node->SetNextPageId(new_leaf_node->GetPageId());
  InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node,transaction);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
  auto recipient = NewNode<N>(node->GetParentPageId());
  node->MoveHalfTo(recipient, buffer_pool_manager_);
  return recipient;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    // old_node is the root node
    // create a new node containing old_node,key,new_node
    auto parent_node =
        NewNode<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    parent_node->PopulateNewRoot(old_node->GetPageId(), key,
                                 new_node->GetPageId());

    auto parent_page_id = parent_node->GetPageId();
    old_node->SetParentPageId(parent_page_id);
    new_node->SetParentPageId(parent_page_id);
    // update root_page_id_
    root_page_id_ = parent_page_id;
    UpdateRootPageId(false);
    // unpin
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(),true);
    buffer_pool_manager_->UnpinPage(parent_page_id, true);
    return;
  }
  auto parent_page = FetchPage(old_node->GetParentPageId());
  auto parent_node = reinterpret_cast<
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
      parent_page->GetData());
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    // parent node is not full
    parent_node->InsertNodeAfter(old_node->GetPageId(), key,
                                 new_node->GetPageId());
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return;
  }
  // parent node is full
  // split parent node
  auto new_parent_node =
      Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(
          parent_node);
  // insert kv
  if (comparator_(key,new_parent_node->KeyAt(1)) == -1) {
    parent_node->InsertNodeAfter(old_node->GetPageId(), key,
                                 new_node->GetPageId());
  }
  else {
    new_parent_node->InsertNodeAfter(old_node->GetPageId(), key,
                                     new_node->GetPageId());
    // remember to change new_node's parent into new_parent_node's page id
    // its parent id is set to the same with old_node in the previous Split
    new_node->SetParentPageId(new_parent_node->GetPageId());
  }
  buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

  // recursive call
  InsertIntoParent(parent_node, new_parent_node->KeyAt(0), new_parent_node);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty())
    return;
  auto leaf_node = FindLeafPage(key);
  ValueType v;
  if (leaf_node->Lookup(key, v, comparator_)) {
    leaf_node->RemoveAndDeleteRecord(key,comparator_);
    CoalesceOrRedistribute(leaf_node,transaction);
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  if (node->GetSize() >= node->GetMinSize()) {
    buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
    return false;
  }
  auto parent_page = FetchPage(node->GetParentPageId());
  auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>
                (parent_page->GetData());
  int index = parent->ValueIndex(node->GetPageId());
  N *sibling;
  decltype(parent_page) sibling_page;
  if (index == 0)
    // right sibling if node is the leftmost child
    sibling_page = FetchPage(parent->ValueAt(index + 1));
  else
    // left sibling if not
    sibling_page = FetchPage(parent->ValueAt(index-1));
  sibling = reinterpret_cast<N*>(sibling_page->GetData());
  bool deletion;
  if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
    // redistribute
    Redistribute(sibling,node,index);
    buffer_pool_manager_->UnpinPage(sibling->GetPageId(),true);
    deletion = false;
  } else {
    if (index == 0) {
      // right sibling
      // move sibling content to node and delete parent entry 1
      Coalesce(node,sibling,parent,1,transaction);
      buffer_pool_manager_->UnpinPage(node->GetPageId(),true);
    }
    else {
      // left sibling
      Coalesce(sibling, node, parent, index, transaction);
      buffer_pool_manager_->UnpinPage(sibling->GetPageId(), true);
    }
    deletion = true;
  }
  CoalesceOrRedistribute(parent,transaction);
  return deletion;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
    node->MoveAllTo(neighbor_node,index,buffer_pool_manager_);
    parent->Remove(index);
    buffer_pool_manager_->DeletePage(node->GetPageId());
    return parent->GetSize() == 0;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0)
    // neighbor_node is the right sibling of node
    neighbor_node->MoveFirstToEndOf(node,buffer_pool_manager_);
  else
    // else neighbor_node is the left sibling of node
    neighbor_node->MoveLastToFrontOf(node,index,buffer_pool_manager_);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  // old_root_node is the only node in the whole tree
  if (old_root_node->IsLeafPage()) {
    if (old_root_node->GetSize() == 0) {
      // delete root page
      buffer_pool_manager_->DeletePage(root_page_id_);
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId();
      return true;
    }
    return false;
  }

  // old_root_node is a internal node
  if (old_root_node->GetSize() == 1) {
    // has one last child
    auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>
                (old_root_node);
    auto child_page_id = root->ValueAt(0);
    // delete root page
    buffer_pool_manager_->DeletePage(root->GetPageId());
    // child node becomes the new root
    root_page_id_ = child_page_id;
    UpdateRootPageId();
    auto child_page = FetchPage(child_page_id);
    auto child = reinterpret_cast<BPlusTreePage*>(child_page->GetData());
    child->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(child->GetPageId(),true);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType k = {};
  return IndexIterator<KeyType,ValueType,KeyComparator>(FindLeafPage(k, true),buffer_pool_manager_,0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto leaf_page = FindLeafPage(key);
  return IndexIterator<KeyType,ValueType,KeyComparator>(leaf_page,buffer_pool_manager_,leaf_page->KeyIndex(key,comparator_));
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) {
  page_id_t page_id = root_page_id_;
  auto page = FetchPage(page_id);
  auto p = reinterpret_cast<BPlusTreePage *>(page->GetData());
  while (!p->IsLeafPage()) {
    auto internal_p = reinterpret_cast<
        BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(
        page->GetData());
    page_id = leftMost ? internal_p->ValueAt(0)
                       : internal_p->Lookup(key, comparator_);
    buffer_pool_manager_->UnpinPage(internal_p->GetPageId(), false);
    page = FetchPage(page_id);
    p = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }
  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
  if (IsEmpty()) return "Empty Tree";
  std::queue<BPlusTreePage*> queue;
  auto root_page = FetchPage(root_page_id_);
  queue.push(reinterpret_cast<BPlusTreePage*>(root_page->GetData()));
  std::string output;
  int size = 1;
  while (!queue.empty()) {
    int tmp_size = 0;
    for (int i = 0; i < size; i++) {
      auto front = queue.front();
      queue.pop();
      if (front->IsLeafPage()) {
        output += "|parent_id("+std::to_string(front->GetParentPageId())+") ";
        output += reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(front)->ToString(verbose);
        output += "| ";
      } else {
        auto node = reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator>*>(front);
        output += "|page_id("+std::to_string(front->GetPageId())+") ";
        output += node->ToString(verbose);
        output += "| ";
        int origin_size = queue.size();
        node->QueueUpChildren(&queue,buffer_pool_manager_);
        tmp_size += (int)queue.size() - origin_size;
      }
      buffer_pool_manager_->UnpinPage(front->GetPageId(),false);
    }
    size = tmp_size;
    output += '\n';
  }
  return output;
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FetchPage(page_id_t page_id) {
  auto ptr = buffer_pool_manager_->FetchPage(page_id);
  if (ptr == nullptr)
    throw std::runtime_error("fail to fetch page");
  return ptr;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::NewPage(page_id_t &page_id) {
  auto ptr = buffer_pool_manager_->NewPage(page_id);
  if (ptr == nullptr)
    throw std::runtime_error("run out of memory");
  return ptr;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::NewNode(page_id_t parent_id) {
  page_id_t new_page_id;
  Page *page = NewPage(new_page_id);
  auto node = reinterpret_cast<N *>(page->GetData());
  node->Init(new_page_id, parent_id);
  return node;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
