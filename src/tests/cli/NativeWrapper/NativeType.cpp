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

#include "NativeType.hpp"

#include <vector>
#include <string>

namespace
{
  volatile bool g_nativeTypeDestroyed = false;
}  // namespace

NativeType::NativeType()
{
  g_nativeTypeDestroyed = false;
}

NativeType::~NativeType()
{
#ifdef _MANAGED
  Apache::Geode::Client::Generic::Log::Info("Invoked destructor of NativeType");
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
	  Apache::Geode::Client::Generic::Log::Info("Allocating string number {0} with "
      "size {1}", i, size);
#endif
    std::string s(size, 'A' + i);
    sum += static_cast<int> (s.size());
    vec.push_back(s);
#ifdef _MANAGED
    System::GC::AddMemoryPressure(size);
    if ((i % numGCOps) == 0) {
      Apache::Geode::Client::Generic::Log::Info("Started GC collection.");
      System::GC::Collect();
      System::GC::WaitForPendingFinalizers();
	  Apache::Geode::Client::Generic::Log::Info("Completed GC collection.");
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
