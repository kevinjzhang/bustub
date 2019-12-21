//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  if(page_table_.count(page_id) > 0) {
    // 1.1    If P exists, pin it and return it immediately. 
    //Pin in replacer if pin count is zero, incrment pin count
    if(pages_[page_table_[page_id]].pin_count_ == 0) {
      replacer_->Pin(page_table_[page_id]);
    }
    pages_[page_table_[page_id]].pin_count_++;
    return pages_ + page_table_[page_id];
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  frame_id_t replacement_frame_id;
  if(free_list_.size() != 0) {
    replacement_frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    replacer_->Victim(&replacement_frame_id);
  }
  // 2.     If R is dirty, write it back to the disk. (Page R = pages_[replacement_frame_id])
  if(pages_[replacement_frame_id].IsDirty()) {
    pages_[replacement_frame_id].WLatch();
    disk_manager_->WritePage(pages_[replacement_frame_id].GetPageId(), pages_[replacement_frame_id].GetData());
    pages_[replacement_frame_id].WUnlatch();
  }
  // 3.     Delete R from the page table and insert P. (Page P = pages_[replacement_frame_id] now after write)
  page_table_.erase(pages_[replacement_frame_id].GetPageId());
  page_table_[page_id] = replacement_frame_id;
  
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  pages_[replacement_frame_id].RLatch();
  disk_manager_->ReadPage(page_id, pages_[replacement_frame_id].GetData());
  pages_[replacement_frame_id].page_id_ = page_id;
  pages_[replacement_frame_id].RUnlatch();
  return pages_ + replacement_frame_id;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) { 
  if(pages_[page_table_[page_id]].pin_count_ <= 0) {
    return false;
  } else {
    //Decrement Pin count
    pages_[page_table_[page_id]].pin_count_--;
    //Set dirty flag if necessary (Occurs before unpinning)
    if(is_dirty) {
      pages_[page_table_[page_id]].is_dirty_ = true;
    }    
    //Unpin if no longer pinned
    if(pages_[page_table_[page_id]].pin_count_ == 0) {
      replacer_->Unpin(page_id);
    }
    return true; 
  }
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  if(page_table_.count(page_id) == 0) {
    return false;
  } else {
    // Writes to page (frame_id = page_table_[page_id])
    if(pages_[page_table_[page_id]].IsDirty()) {
      pages_[page_table_[page_id]].WLatch();
      disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
      pages_[page_table_[page_id]].WUnlatch();
    }
    return true;
  }
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  *page_id = disk_manager_->AllocatePage();
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  bool allPinned = true;
  for(int i = 0; i < (int)pool_size_; i++) {
    if(pages_[i].GetPinCount() == 0) {
      allPinned = false;
      break;
    }
  }
  if(allPinned) {
    return nullptr;
  } else {
    // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
    frame_id_t replacement_frame_id;
    if(free_list_.size() != 0) {
      replacement_frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if(replacer_->Size() == 0) {
        return nullptr;
      }
      replacer_->Victim(&replacement_frame_id);
    }
    // 3.   Update P's metadata, zero out memory and add P to the page table.
    pages_[replacement_frame_id].RLatch();
    disk_manager_->ReadPage(*page_id, pages_[replacement_frame_id].GetData());
    pages_[replacement_frame_id].RUnlatch();
    page_table_[*page_id] = replacement_frame_id;
    // 4.   Set the page ID output parameter. Return a pointer to P.
    pages_[replacement_frame_id].page_id_ = *page_id;
    return pages_ + *page_id;
  }
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  disk_manager_->DeallocatePage(page_id);
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  if(page_table_.count(page_id) == 0) {
    return true;
  } else {
    // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
    if(pages_[page_table_[page_id]].pin_count_ > 0) {
      return false;
    } else {
    // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
      page_table_.erase(page_id);
      pages_[page_table_[page_id]].ResetMemory();
      pages_[page_table_[page_id]].page_id_ = INVALID_PAGE_ID;
      free_list_.push_back(page_table_[page_id]);
      return true;
    }
  }
}

void BufferPoolManager::FlushAllPagesImpl() {
  // You can do it!
  for(auto it = page_table_.begin(); it != page_table_.end(); it++) {
    FlushPageImpl(it->first);
  }
}

}  // namespace bustub
