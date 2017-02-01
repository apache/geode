#pragma once

#ifndef APACHE_GEODE_GUARD_4bb699f806bb1f49d020175dd0840394
#define APACHE_GEODE_GUARD_4bb699f806bb1f49d020175dd0840394

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

/*
 * @brief User class for testing the query functionality.
 */


#include <gfcpp/GeodeCppCache.hpp>
#include <string.h>
#include "fwklib/Timer.hpp"
#include "fwklib/FrameworkTest.hpp"
#include "TimestampedObject.hpp"
#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Time_Value.h>

#ifdef _WIN32
#ifdef BUILD_TESTOBJECT
#define TESTOBJECT_EXPORT LIBEXP
#else
#define TESTOBJECT_EXPORT LIBIMP
#endif
#else
#define TESTOBJECT_EXPORT
#endif

using namespace apache::geode::client;
using namespace testframework;
namespace testobject {
class TESTOBJECT_EXPORT FastAssetAccount : public TimestampedObject {
 protected:
  bool encodeTimestamp;
  int32_t acctId;
  CacheableStringPtr customerName;
  double netWorth;
  CacheableHashMapPtr assets;
  uint64_t timestamp;

  inline uint32_t getObjectSize(const SerializablePtr& obj) const {
    return (obj == NULLPTR ? 0 : obj->objectSize());
  }

 public:
  FastAssetAccount()
      : encodeTimestamp(0),
        acctId(0),
        customerName(NULLPTR),
        netWorth(0.0),
        assets(NULLPTR),
        timestamp(0) {}
  FastAssetAccount(int index, bool encodeTimestp, int maxVal, int asstSize = 0);
  virtual ~FastAssetAccount();
  virtual void toData(apache::geode::client::DataOutput& output) const;
  virtual apache::geode::client::Serializable* fromData(
      apache::geode::client::DataInput& input);
  virtual int32_t classId() const { return 23; }
  CacheableStringPtr toString() const;

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(FastAssetAccount);
    return objectSize;
  }

  int getAcctId() { return acctId; }

  CacheableStringPtr getCustomerName() { return customerName; }

  double getNetWorth() { return netWorth; }

  void incrementNetWorth() { ++netWorth; }

  CacheableHashMapPtr getAssets() { return assets; }
  int getIndex() { return acctId; }
  uint64_t getTimestamp() {
    if (encodeTimestamp) {
      return timestamp;
    } else {
      return 0;
    }
  }

  void resetTimestamp() {
    if (encodeTimestamp) {
      ACE_Time_Value startTime;
      startTime = ACE_OS::gettimeofday();
      ACE_UINT64 tusec;
      startTime.to_usec(tusec);
      timestamp = tusec * 1000;
    } else {
      timestamp = 0;
    }
  }
  void update() {
    incrementNetWorth();
    if (encodeTimestamp) {
      resetTimestamp();
    }
  }
  static apache::geode::client::Serializable* createDeserializable() {
    return new FastAssetAccount();
  }
};

typedef apache::geode::client::SharedPtr<FastAssetAccount> FastAssetAccountPtr;
}  // namespace testobject

#endif // APACHE_GEODE_GUARD_4bb699f806bb1f49d020175dd0840394
