//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
    replacerMaxSize = num_pages;
}

ClockReplacer::~ClockReplacer() = default;

void ClockReplacer::State() {
    std::cout << '[';
    for(auto frame: frames) {
        std::cout << frame.frame_id << ", ";
    }
    std::cout << ']' << std::endl;
}

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
    mutexLock.lock();
    for (int i = 0; i < (int)frames.size() + 1; i++) {     
        //Set ref_flag to false/ set victim.
        if ((*clockHand).ref_flag) {
            (*clockHand).ref_flag = false;
        } else {
            *frame_id = (*clockHand).frame_id;
            mutexLock.unlock();
            Pin(*frame_id);
            return true;
        }         
        //Move clockHand     
        if (std::next(clockHand) == frames.end()) {
            clockHand = frames.begin(); 
        } else {
            clockHand++;
        }  
    }
    //Only occurs with frames.size() == 0
    mutexLock.unlock();
    return false; 
}

void ClockReplacer::Pin(frame_id_t frame_id) {
    if(frameTable.count(frame_id) != 0) {
        mutexLock.lock();
        auto frame = frameTable[frame_id];    
        if(clockHand == frame) {        
            if (std::next(clockHand) == frames.end()) {
                clockHand = frames.begin(); 
            } else {
                clockHand++;
            }       
        }   
        frames.erase(frame);
        frameTable.erase(frame_id);
        mutexLock.unlock();
    }
}

//Existing frames test
void ClockReplacer::Unpin(frame_id_t frame_id) {
    //Frame exists in frameTable
    if(frameTable.count(frame_id) != 0) {
        (*frameTable[frame_id]).ref_flag = true;
    } else { //Frame doesn't exist in frameTable
        frame F;
        F.frame_id = frame_id;
        F.ref_flag = true;

        //Use replacement policy and delete page when full
        if((int)frames.size() == replacerMaxSize) {
            frame_id_t victimFrame;
            Victim(&victimFrame);
        } 
        mutexLock.lock();
        frames.push_back(F);
        //First Frame
        if(frames.size() == 1) {
            clockHand = frames.begin();
        } 
        //Insert at end  
        frameTable[frame_id] = std::prev(frames.end());
        mutexLock.unlock();
    }
}

size_t ClockReplacer::Size() { return frames.size(); }

}  // namespace bustub
