#pragma once

#ifndef GEODE_READWRITELOCK_H_
#define GEODE_READWRITELOCK_H_

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

#include <ace/RW_Thread_Mutex.h>
#include "Condition.hpp"

namespace apache {
namespace geode {
namespace client {
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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_READWRITELOCK_H_
