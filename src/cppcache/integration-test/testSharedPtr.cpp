/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testSharedPtr"

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>

#include <HostAsm.hpp>

using namespace gemfire;

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
