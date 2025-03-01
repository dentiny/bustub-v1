//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
 public:
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int idx, BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

  bool operator==(const IndexIterator &rhs) const { return leaf_ == rhs.leaf_ && idx_ == rhs.idx_; }

  bool operator!=(const IndexIterator &rhs) const { return leaf_ != rhs.leaf_ || idx_ != rhs.idx_; }

 private:
  int idx_;  // index inside a leaf node
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_;
  BufferPoolManager *buffer_pool_manager_;
};

}  // namespace bustub
