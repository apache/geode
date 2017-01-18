/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define ROOT_NAME "testSharedPtr"

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>

#include <HostAsm.hpp>

using namespace apache::geode::client;

// Test some HostAsm code first..
DUNIT_TASK(s1p1, HostAsm)
  {
    volatile int32_t counter = 0;
    XASSERT(counter == 0);
    HostAsm::atomicAdd(counter, 1);
    XASSERT(counter == 1);
    HostAsm::atomicAdd(counter, 3);
    XASSERT(counter == 4);

    HostAsm::atomicAdd(counter, -1);
    XASSERT(counter == 3);
  }
END_TASK(HostAsm)

// Test Or and And.
DUNIT_TASK(s1p1, AndOr)
  {
    volatile uint32_t bits = 0;
    uint32_t mask1 = 0x00000001ul;
    uint32_t mask2 = 0x00000002ul;

    HostAsm::atomicOr(bits, mask1);
    XASSERT(bits == 1);
    HostAsm::atomicAnd(bits, ~mask1);
    XASSERT(bits == 0);
    HostAsm::atomicOr(bits, mask1);
    HostAsm::atomicOr(bits, mask2);
    XASSERT(bits == 3);
    HostAsm::atomicAnd(bits, ~mask1);
    XASSERT(bits == 2);
    HostAsm::atomicAnd(bits, ~mask2);
    XASSERT(bits == 0);
  }
END_TASK(AndOr)

bool deleted = false;

class TestObj : public SharedBase {
 public:
  TestObj() : SharedBase() {}

  ~TestObj() { deleted = true; }
};

typedef SharedPtr<TestObj> TestObjPtr;

DUNIT_TASK(s1p1, A)
  {
    char logmsg[1024];
    deleted = false;
    TestObj* obj = new TestObj();
    sprintf(logmsg, "TestObj->refCount(): %d\n", obj->refCount());
    LOG(logmsg);
    ASSERT(obj->refCount() == 0, "refcount should be 0, no ptrs yet.");
    TestObjPtr* ptr = new TestObjPtr(obj);
    sprintf(logmsg, "TestObj->refCount(): %d\n", obj->refCount());
    LOG(logmsg);
    ASSERT((*ptr)->refCount() == 1, "Expected refCount == 1");
    delete ptr;
    ASSERT(deleted == true, "Expected destruction.");
  }
END_TASK(A)

DUNIT_TASK(s1p1, B)
  {
    deleted = false;
    TestObjPtr* heapPtr = new TestObjPtr();
    {
      TestObjPtr ptr(new TestObj());
      ASSERT(ptr->refCount() == 1, "Expected refCount == 1");
      *heapPtr = ptr;
      ASSERT(ptr->refCount() == 2, "Expected refCount == 2");
    }
    ASSERT(deleted == false, "Only one reference went away, should be alive.");
    ASSERT((*heapPtr)->refCount() == 1, "Expected refCount == 1");
    delete heapPtr;
    ASSERT(deleted == true,
           "Now last reference is gone, so TestObj should be deleted.");
    LOG("Finished successfully.");
  }
END_TASK(B)
