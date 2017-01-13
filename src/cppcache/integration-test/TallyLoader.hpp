/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef _TEST_TALLYLOADER_HPP_
#define _TEST_TALLYLOADER_HPP_ 1

#include <gfcpp/GemfireCppCache.hpp>

using namespace gemfire;
using namespace test;

class TallyLoader;

typedef gemfire::SharedPtr<TallyLoader> TallyLoaderPtr;

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

#endif  //_TEST_TALLYLOADER_HPP_
