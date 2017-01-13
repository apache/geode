/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>

#include "SpinLock.hpp"

namespace gemfire {

void* testSpinLockCreate() { return (void*)new SpinLock(); }

void testSpinLockAcquire(void* lock) {
  (reinterpret_cast<SpinLock*>(lock))->acquire();
}

void testSpinLockRelease(void* lock) {
  (reinterpret_cast<SpinLock*>(lock))->release();
}
}  // namespace gemfire
