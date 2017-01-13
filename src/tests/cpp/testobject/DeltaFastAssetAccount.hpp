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

#ifndef __DELTAFASTASSETACT__HPP__
#define __DELTAFASTASSETACT__HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include <string.h>
#include "fwklib/FrameworkTest.hpp"
#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Time_Value.h>
#include "FastAsset.hpp"

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
class DeltaFastAssetAccount;
typedef gemfire::SharedPtr<DeltaFastAssetAccount> DeltaFastAssetAccountPtr;
class TESTOBJECT_EXPORT DeltaFastAssetAccount : public Cacheable, public Delta {
 private:
  bool encodeTimestamp;
  int32_t acctId;
  CacheableStringPtr customerName;
  double netWorth;
  CacheableHashMapPtr assets;
  uint64_t timestamp;
  bool getBeforeUpdate;

  inline uint32_t getObjectSize(const SerializablePtr& obj) const {
    return (obj == NULLPTR ? 0 : obj->objectSize());
  }

 public:
  DeltaFastAssetAccount()
      : encodeTimestamp(0),
        acctId(0),
        customerName(NULLPTR),
        netWorth(0.0),
        assets(NULLPTR),
        timestamp(0),
        getBeforeUpdate(false) {}
  DeltaFastAssetAccount(int index, bool encodeTimestp, int maxVal,
                        int asstSize = 0, bool getbfrUpdate = false);

  virtual ~DeltaFastAssetAccount() {}
  void toData(gemfire::DataOutput& output) const;
  gemfire::Serializable* fromData(gemfire::DataInput& input);
  void toDelta(gemfire::DataOutput& output) const;
  void fromDelta(gemfire::DataInput& input);

  CacheableStringPtr toString() const {
    char buf[102500];
    sprintf(buf,
            "DeltaFastAssetAccount:[acctId = %d customerName = %s netWorth = "
            "%f timestamp = %lld]",
            acctId, customerName->toString(), netWorth, timestamp);
    return CacheableString::create(buf);
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
  int32_t classId() const { return 41; }
  bool hasDelta() { return true; }

  uint32_t objectSize() const {
    uint32_t objectSize = sizeof(DeltaFastAssetAccount);
    return objectSize;
  }

  virtual DeltaPtr clone() {
    DeltaFastAssetAccountPtr clonePtr(new DeltaFastAssetAccount());
    clonePtr->assets = CacheableHashMap::create();
    for (HashMapOfCacheable::Iterator item = this->assets->begin();
         item != this->assets->end(); item++) {
      CacheableInt32Ptr key = dynCast<CacheableInt32Ptr>(item.first());
      FastAssetPtr asset = dynCast<FastAssetPtr>(item.second());
      clonePtr->assets->insert(key, asset->copy());
    }
    return clonePtr;
  }

  static gemfire::Serializable* createDeserializable() {
    return new DeltaFastAssetAccount();
  }
};
}
#endif
