#pragma once

#ifndef GEODE_INTEGRATION_TEST_TALLYLOADER_H_
#define GEODE_INTEGRATION_TEST_TALLYLOADER_H_

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

#include <gfcpp/GeodeCppCache.hpp>

using namespace apache::geode::client;
using namespace test;

class TallyLoader;

typedef apache::geode::client::SharedPtr<TallyLoader> TallyLoaderPtr;

class TallyLoader : virtual public CacheLoader {
 private:
  int32_t m_loads;

 public:
  TallyLoader()
      : CacheLoader(),
        m_loads(0)

  {}
  virtual ~TallyLoader() {}

  CacheablePtr load(const RegionPtr& rp, const CacheableKeyPtr& key,
                    const UserDataPtr& aCallbackArgument) {
    LOGDEBUG("TallyLoader::load invoked for %d.", m_loads);
    char buf[1024];
    sprintf(buf, "TallyLoader state: (loads = %d)", m_loads);
    LOG(buf);
    return CacheableInt32::create(m_loads++);
  }

  virtual void close(const RegionPtr& region) { LOG("TallyLoader::close"); }

  int expectLoads(int expected) {
    int tries = 0;
    while ((m_loads < expected) && (tries < 200)) {
      SLEEP(100);
      tries++;
    }
    return m_loads;
  }

  int getLoads() { return m_loads; }

  void reset() { m_loads = 0; }

  void showTallies() {
    char buf[1024];
    sprintf(buf, "TallyLoader state: (loads = %d)", getLoads());
    LOG(buf);
  }
};


#endif // GEODE_INTEGRATION_TEST_TALLYLOADER_H_
