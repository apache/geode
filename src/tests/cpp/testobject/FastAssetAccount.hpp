/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/*
 * @brief User class for testing the query functionality.
 */

#ifndef __FASTASSETACT__HPP__
#define __FASTASSETACT__HPP__

#include <gfcpp/GemfireCppCache.hpp>
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

using namespace gemfire;
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
  virtual void toData(gemfire::DataOutput& output) const;
  virtual gemfire::Serializable* fromData(gemfire::DataInput& input);
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
  static gemfire::Serializable* createDeserializable() {
    return new FastAssetAccount();
  }
};

typedef gemfire::SharedPtr<FastAssetAccount> FastAssetAccountPtr;
}
#endif
