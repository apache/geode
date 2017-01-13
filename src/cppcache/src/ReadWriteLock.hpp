/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _GEMFIRE_IMPL_READWRITELOCK_HPP_
#define _GEMFIRE_IMPL_READWRITELOCK_HPP_

#include <ace/RW_Thread_Mutex.h>
#include "Condition.hpp"

namespace gemfire {
class TimedTryWriteGuard {
 public:
  TimedTryWriteGuard(ACE_RW_Thread_Mutex& lock, uint32_t usec);
  bool tryAcquireLock(uint32_t usec);
  ~TimedTryWriteGuard() {
    if (isAcquired_) lock_.release();
  }
  bool isAcquired() const { return isAcquired_; }

 private:
  ACE_RW_Thread_Mutex& lock_;
  bool isAcquired_;
  ACE_Recursive_Thread_Mutex mutex_;
  Condition cond_;
};

class CPPCACHE_EXPORT ReadGuard {
 public:
  ReadGuard(ACE_RW_Thread_Mutex& lock) : lock_(lock) { lock_.acquire_read(); }

  ~ReadGuard() { lock_.release(); }
  bool isAcquired() { return true; }

 private:
  ACE_RW_Thread_Mutex& lock_;
};

class WriteGuard {
 public:
  WriteGuard(ACE_RW_Thread_Mutex& lock) : lock_(lock) { lock_.acquire_write(); }

  ~WriteGuard() { lock_.release(); }

 private:
  ACE_RW_Thread_Mutex& lock_;
};

class TryReadGuard {
 public:
  TryReadGuard(ACE_RW_Thread_Mutex& lock, const volatile bool& exitCondition);
  ~TryReadGuard() {
    if (isAcquired_) lock_.release();
  }
  bool isAcquired() const { return isAcquired_; }

 private:
  ACE_RW_Thread_Mutex& lock_;
  bool isAcquired_;
};

class TryWriteGuard {
 public:
  TryWriteGuard(ACE_RW_Thread_Mutex& lock, const volatile bool& exitCondition);
  ~TryWriteGuard() {
    if (isAcquired_) lock_.release();
  }
  bool isAcquired() const { return isAcquired_; }

 private:
  ACE_RW_Thread_Mutex& lock_;
  bool isAcquired_;
};
}

#endif
