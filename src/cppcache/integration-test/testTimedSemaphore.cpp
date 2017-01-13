/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#define ROOT_NAME "testTimedSemaphore"

#include "fw_helper.hpp"
#include <ace/Synch.h>

class ThreadAcquire : public ACE_Task_Base {
 public:
  ThreadAcquire(ACE_Thread_Semaphore& sema, int acquireSecs)
      : ACE_Task_Base(),
        m_sema(sema),
        m_acquireSecs(acquireSecs),
        m_status(0) {}

  int svc() {
    ACE_Time_Value start = ACE_OS::gettimeofday();
    ACE_Time_Value interval(m_acquireSecs, 0);  // 10 seconds
    ACE_Time_Value expireAt = start + interval;

    printf("Thread acquiring lock at %ld msecs.\n", start.msec());
    if (m_sema.acquire(expireAt) == 0) {
      interval = ACE_OS::gettimeofday() - start;
      printf("Thread acquired lock after %ld msecs.\n", interval.msec());
      m_status = 0;
    } else {
      interval = ACE_OS::gettimeofday() - start;
      printf("Thread failed to acquire lock after %ld msecs.\n",
             interval.msec());
      m_status = -1;
    }
    return m_status;
  }

  int getStatus() { return m_status; }

 private:
  ACE_Thread_Semaphore& m_sema;
  int m_acquireSecs;
  int m_status;
};

BEGIN_TEST(CheckTimedAcquire)
  {
    ACE_Thread_Semaphore sema(1);
    ThreadAcquire* thread = new ThreadAcquire(sema, 10);

    sema.acquire();
    thread->activate();

    LOG("Sleeping for 8 secs.");
    ACE_OS::sleep(8);
    ASSERT(thread->thr_count() == 1, "Expected thread to be running.");
    sema.release();
    SLEEP(50);  // Sleep for a few millis for the thread to end.
    ASSERT(thread->thr_count() == 0, "Expected no thread to be running.");
    ASSERT(thread->wait() == 0, "Expected successful end of thread.");
    ASSERT(thread->getStatus() == 0, "Expected zero exit status from thread.");

    delete thread;
  }
END_TEST(CheckTimedAcquire)

BEGIN_TEST(CheckTimedAcquireFail)
  {
    ACE_Thread_Semaphore sema(0);
    ThreadAcquire* thread = new ThreadAcquire(sema, 10);

    thread->activate();

    LOG("Sleeping for 8 secs.");
    ACE_OS::sleep(8);
    ASSERT(thread->thr_count() == 1, "Expected thread to be running.");
    ACE_OS::sleep(3);
    ASSERT(thread->thr_count() == 0, "Expected no thread to be running.");
    ASSERT(thread->wait() == 0, "Expected successful end of thread.");
    ASSERT(thread->getStatus() == -1,
           "Expected non-zero exit status from thread.");

    delete thread;
  }
END_TEST(CheckTimedAcquireFail)

BEGIN_TEST(CheckNoWait)
  {
    ACE_Thread_Semaphore sema(0);
    ThreadAcquire* thread = new ThreadAcquire(sema, 10);

    sema.release();
    thread->activate();

    ACE_OS::sleep(1);
    ASSERT(thread->thr_count() == 0, "Expected no thread to be running.");
    ASSERT(thread->wait() == 0, "Expected successful end of thread.");
    ASSERT(thread->getStatus() == 0, "Expected zero exit status from thread.");

    delete thread;
  }
END_TEST(CheckNoWait)

BEGIN_TEST(CheckResetAndTimedAcquire)
  {
    ACE_Thread_Semaphore sema(1);
    ThreadAcquire* thread = new ThreadAcquire(sema, 10);

    sema.acquire();
    ACE_OS::sleep(1);
    sema.release();
    sema.release();
    sema.release();
    while (sema.tryacquire() != -1) {
      ;
    }
    thread->activate();

    LOG("Sleeping for 8 secs.");
    ACE_OS::sleep(8);
    ASSERT(thread->thr_count() == 1, "Expected thread to be running.");
    sema.release();
    SLEEP(50);  // Sleep for a few millis for the thread to end.
    ASSERT(thread->thr_count() == 0, "Expected no thread to be running.");
    ASSERT(thread->wait() == 0, "Expected successful end of thread.");
    ASSERT(thread->getStatus() == 0, "Expected zero exit status from thread.");

    delete thread;
  }
END_TEST(CheckResetAndTimedAcquire)
