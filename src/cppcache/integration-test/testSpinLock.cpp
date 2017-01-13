/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testSpinLock"

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>

#include <Condition.hpp>

#include <ace/Task.h>
#include <ace/Time_Value.h>
#include <ace/Guard_T.h>

namespace gemfire {

CPPCACHE_EXPORT void* testSpinLockCreate();
CPPCACHE_EXPORT void testSpinLockAcquire(void* lock);
CPPCACHE_EXPORT void testSpinLockRelease(void* lock);
}

DUNIT_TASK(s1p1, Basic)
  {
    void* lock = gemfire::testSpinLockCreate();
    gemfire::testSpinLockAcquire(lock);
    gemfire::testSpinLockRelease(lock);
  }
END_TASK(Basic)

perf::Semaphore* triggerA;
perf::Semaphore* triggerB;
perf::Semaphore* triggerM;

void* lock;
ACE_Time_Value* btime;

class ThreadA : public ACE_Task_Base {
 public:
  ThreadA() : ACE_Task_Base() {}

  int svc() {
    gemfire::testSpinLockAcquire(lock);
    LOG("ThreadA: Acquired lock x.");
    triggerM->release();
    triggerA->acquire();
    gemfire::testSpinLockRelease(lock);
    LOG("ThreadA: Released lock.");
    return 0;
  }
};

class ThreadB : public ACE_Task_Base {
 public:
  ThreadB() : ACE_Task_Base() {}

  int svc() {
    triggerB->acquire();
    gemfire::testSpinLockAcquire(lock);
    btime = new ACE_Time_Value(ACE_OS::gettimeofday());
    LOG("ThreadB: Acquired lock.");
    triggerM->release();
    gemfire::testSpinLockRelease(lock);  // for cleanly ness.
    return 0;
  }
};

DUNIT_TASK(s1p1, TwoThreads)
  {
    triggerA = new perf::Semaphore(0);
    triggerB = new perf::Semaphore(0);
    triggerM = new perf::Semaphore(0);

    lock = gemfire::testSpinLockCreate();

    ThreadA* threadA = new ThreadA();
    ThreadB* threadB = new ThreadB();

    threadA->activate();
    threadB->activate();

    // A runs, locks the spinlock, and triggers me. B is idle.
    triggerM->acquire();
    // A is now idle, but holds lock. Tell B to acquire the lock
    ACE_Time_Value stime = ACE_OS::gettimeofday();
    triggerB->release();
    SLEEP(5000);
    // B will be stuck until we tell A to release it.
    triggerA->release();
    // wait until B tells us it has acquired the lock.
    triggerM->acquire();

    // Now diff btime (when B acquired the lock) and stime to see that it
    // took longer than the 5000 seconds before A released it.
    ACE_Time_Value delta = *btime - stime;
    char msg[1024];
    sprintf(msg, "acquire delay was %lu\n", delta.msec());
    LOG(msg);
    ASSERT(delta.msec() >= 4900, "Expected 5 second or more spinlock delay");
    // Note the test is against 4900 instead of 5000 as there are some
    // measurement
    // issues. Often delta comes back as 4999 on linux.

    threadA->wait();
    delete threadA;
    threadB->wait();
    delete threadB;
  }
END_TASK(TwoThreads)

DUNIT_TASK(s1p1, Cond)
  {
    // Test that the Condtion wrapper times out properly.
    // Does not test signal..
    ACE_Time_Value stopAt = ACE_OS::gettimeofday();
    stopAt -= 10;  // Make sure it is in the past.

    ACE_Recursive_Thread_Mutex mutex;
    ACE_Guard<ACE_Recursive_Thread_Mutex> guard(mutex);
    gemfire::Condition cond(mutex);
    LOG("About to wait on Condition with past time.");
    if (cond.waitUntil(&stopAt) != false) {
      FAIL("Should have timed out immediately.");
    }

    stopAt = ACE_OS::gettimeofday();
    LOG("About to wait on Condition with present time.");
    if (cond.waitUntil(&stopAt) != false) {
      FAIL("Should have timed out immediately.");
    }

    ACE_Time_Value begin = ACE_OS::gettimeofday();
    stopAt = begin;
    ACE_Time_Value delay(5);
    stopAt += delay;
    if (cond.waitUntil(&stopAt) != false) {
      FAIL("Should have timed out immediately.");
    }
    ACE_Time_Value delta = ACE_OS::gettimeofday();
    delta -= begin;
    printf("delta is %lu, delay is %lu", delta.msec(), delay.msec());
    XASSERT(delta.msec() >= (delay.msec() - 50));
  }
ENDTASK
