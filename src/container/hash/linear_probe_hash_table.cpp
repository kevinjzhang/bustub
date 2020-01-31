//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/linear_probe_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  auto index = hash_fn_.GetHash(key) / BLOCK_ARRAY_SIZE;
  int currIndex = 0;
  for (int i = 0; i < (int)header_page_->GetSize(); i++) {
    //Find page id and fetch page from buffer pool
    page_id_t page_id = header_page_->GetBlockPageId((index + i) % header_page_->GetSize());
    Page* page = buffer_pool_manager_->FetchPage(page_id);
    void* rawData = page->GetData();
    HashTableBlockPage<KeyType, ValueType, KeyComparator>* blockData = (HashTableBlockPage<KeyType, ValueType, KeyComparator>*) rawData;
    for (int j = 0; j < (int)BLOCK_ARRAY_SIZE; j++) {
      if (blockData->IsReadable(j)) {
        if (comparator_(blockData->KeyAt(j), key) == 0) {
        result->at(currIndex) = blockData->ValueAt(j);
        currIndex++;
        } else if (currIndex > 0) {
          return true;
        }
      } else {
        return (currIndex > 0);
      }
    }
  }
  return false;
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto index = hash_fn_.GetHash(key) / BLOCK_ARRAY_SIZE;
  for (int i = 0; i < (int)header_page_->GetSize(); i++) {
    page_id_t page_id = header_page_->GetBlockPageId((index + i) % header_page_->GetSize());
    Page* page = buffer_pool_manager_->FetchPage(page_id);
    void* rawData = page->GetData();
    HashTableBlockPage<KeyType, ValueType, KeyComparator>* blockData = (HashTableBlockPage<KeyType, ValueType, KeyComparator>*) rawData;
    for (int j = 0; j < (int)BLOCK_ARRAY_SIZE; j++) {
      if (blockData->Insert(j, key, value)) {
        return true;
      } 
    }
  }
  return false;
}

//Helper for remove
template <typename KeyType, typename ValueType, typename KeyComparator>
int HASH_TABLE_TYPE::FindKeyAndDelete(Transaction *transaction, const KeyType &key, const ValueType &value) {
  int slotIndex = -1;
  auto index = hash_fn_.GetHash(key) / BLOCK_ARRAY_SIZE;
  for (int i = 0; i < (int)header_page_->GetSize(); i++) {
    //Find page id and fetch page from buffer pool
    page_id_t page_id = header_page_->GetBlockPageId((index + i) % header_page_->GetSize());
    Page* page = buffer_pool_manager_->FetchPage(page_id);
    void* rawData = page->GetData();
    HashTableBlockPage<KeyType, ValueType, KeyComparator>* blockData = (HashTableBlockPage<KeyType, ValueType, KeyComparator>*) rawData;
    for (int j = 0; j < (int)BLOCK_ARRAY_SIZE; j++) {
      if (blockData->IsReadable(j)) {
        if (comparator_(blockData->KeyAt(j), key) == 0 && value == blockData->ValueAt(j)) {
          blockData->Remove(j);
          slotIndex = i * header_page_->GetSize() + j;
          return slotIndex;
        }
      } else {
        return slotIndex;
      }
    }
  }
  return slotIndex;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  //Phase one find key
  auto slotIndex = FindKeyAndDelete(transaction, key, value);
  //Full table or no key found
  if(slotIndex == -1) {
    return false;
  }
  //Phase two cleaning up table
  for (int i = 0; i < (int)header_page_->GetSize(); i++) {
    page_id_t page_id = header_page_->GetBlockPageId((slotIndex + i) % header_page_->GetSize());
    Page* page = buffer_pool_manager_->FetchPage(page_id);
    void* rawData = page->GetData();
    HashTableBlockPage<KeyType, ValueType, KeyComparator>* blockData = (HashTableBlockPage<KeyType, ValueType, KeyComparator>*) rawData;
    for (int j = 0; j < (int)BLOCK_ARRAY_SIZE; j++) {
      int origSlot = hash_fn_.GetHash(blockData->KeyAt(j + 1));
      if (origSlot != slotIndex) {
        blockData->Insert(j, blockData->KeyAt(j + 1), blockData->ValueAt(j + 1));
        blockData->Remove(j + 1);
        slotIndex++;
      } else {
        break;
      }
    }
  }
  return true;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return 0;
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
