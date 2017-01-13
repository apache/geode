/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "NativeType.hpp"

#include <vector>
#include <string>

namespace
{
  volatile bool g_nativeTypeDestroyed = false;
}

NativeType::NativeType()
{
  g_nativeTypeDestroyed = false;
}

NativeType::~NativeType()
{
#ifdef _MANAGED
  GemStone::GemFire::Cache::Generic::Log::Info("Invoked destructor of NativeType");
#endif
  g_nativeTypeDestroyed = true;
}

bool NativeType::doOp(int size, int numOps, int numGCOps)
{
  if (numOps == 0) { // special case for performance testing
    std::string s(size, 'A');
    return (!g_nativeTypeDestroyed && s.size() > 0);
  }
  if (numGCOps == 0) {
    numGCOps = 1;
  }
  int sum = 0;
  std::vector<std::string> vec;
  for (int i = 1; i <= numOps; ++i) {
#ifdef _MANAGED
	  GemStone::GemFire::Cache::Generic::Log::Info("Allocating string number {0} with "
      "size {1}", i, size);
#endif
    std::string s(size, 'A' + i);
    sum += static_cast<int> (s.size());
    vec.push_back(s);
#ifdef _MANAGED
    System::GC::AddMemoryPressure(size);
    if ((i % numGCOps) == 0) {
      GemStone::GemFire::Cache::Generic::Log::Info("Started GC collection.");
      System::GC::Collect();
      System::GC::WaitForPendingFinalizers();
	  GemStone::GemFire::Cache::Generic::Log::Info("Completed GC collection.");
    }
    System::Threading::Thread::Sleep(500);
#else
    SLEEP(500);
#endif
  }
#ifdef _MANAGED
  System::GC::RemoveMemoryPressure(sum);
#endif
  return !g_nativeTypeDestroyed;
}
