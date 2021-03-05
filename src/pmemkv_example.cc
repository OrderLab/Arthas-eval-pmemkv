/*
 * Copyright 2017, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include <iostream>
#include "pmemkv.h"
#include <climits> 
#include <time.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <signal.h>
#include <sys/time.h>
#include <unistd.h>

pthread_mutex_t calculating = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t done = PTHREAD_COND_INITIALIZER;

#define LOG(msg) std::cout << msg << "\n"

using namespace pmemkv;


KVTree *kv;


int main() {
    size_t sisi = INT_MAX;
    size_t sisi2 =  INT_MAX;
    sisi = 53 * (sisi + sisi2);
    if(access("/mnt/pmem/pmemkv.pm", F_OK) != 0){
      LOG("Opening database");
      //KVTree* kv = new KVTree("/mnt/pmem/pmemkv.pm", PMEMOBJ_MIN_POOL);
      kv = new KVTree("/mnt/pmem/pmemkv.pm", sisi);

      LOG("Putting new value");
      KVStatus s = kv->Put("key1", "value1");
      KVStatus s2;
      char keybuf[256];
      char valuebuf[256];
      assert(s == OK);
      std::string value5;
      s = kv->Get("key1", &value5);
      //for(int i = 2; i < 3; i++){
      //for(int i = 2; i < 16342000; i++){
      for(int i = 2; i < 1000; i++){
          snprintf(keybuf, sizeof(keybuf), "%s%d\n", "key", i);
          snprintf(valuebuf, sizeof(valuebuf), "%s%d\n", "value", i);
          printf("i is %d\n", i);
          s2 = kv->Put(keybuf, valuebuf);
       }
    LOG("Deleting existing value");
    s = kv->Delete("key1");
      //assert(s == NOT_FOUND);
      LOG("Closing database");
      delete kv;
     }
	//s2 = kv->Put("
    //KVStatus s2 = kv->Put("key2", "value2");
    //assert(s == OK);
    /*std::string value;
    s = kv->Get("key1", &value);
    std::cout << value << "\n";
    assert(s == OK && value == "value1");
    std::string value2;
    s = kv->Get("key2", &value2);
    std::cout << value2 << "\n";*/

    /*LOG("Replacing existing value");
    std::string value2;
    s = kv->Get("key1", &value2);
    assert(s == OK && value2 == "value1");
    s = kv->Put("key1", "value_replaced");
    assert(s == OK);
    std::string value3;
    s = kv->Get("key1", &value3);
    assert(s == OK && value3 == "value_replaced");*/


    //kv = new KVTree("/mnt/pmem/pmemkv.pm", PMEMOBJ_MIN_POOL);
   else {
    kv = new KVTree("/mnt/pmem/pmemkv.pm", sisi);
    assert(kv->GetPath() == "/mnt/pmem/pmemkv.pm");
    std::string value3;
    /*s = kv->Get("key1", &value3);
    std::cout << value3 << "\n";
    std::string value4;
    s = kv->Get("key2", &value4);
    std::cout << value4 << "\n";*/
    //assert(s == OK && value == "value1");

    /*LOG("Opening database again");
    kv = new KVTree("/mnt/pmem/pmemkv.pm", PMEMOBJ_MIN_POOL);

    s = kv->Get("key1", &value);
    assert(s == OK && value == "value1");*/

    LOG("Finished successfully");
    }
    return 0;
}
