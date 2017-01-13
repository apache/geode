/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/*
 * @brief User class for testing the put functionality for object.
 */

#ifndef __EQSTRUCT_HPP__
#define __EQSTRUCT_HPP__

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
class TESTOBJECT_EXPORT EqStruct : public TimestampedObject {
  int myIndex;
  char* state;
  uint64_t timestamp;
  double executedPriceSum;
  int cxlQty;
  int isSyntheticOrder;
  int64_t availQty;
  double positionQty;
  int isRestricted;
  char* demandInd;
  char* side;
  int orderQty;
  double price;
  char* ordType;
  double stopPx;
  char* senderCompID;
  char* tarCompID;
  char* tarSubID;
  char* handlInst;
  char* orderID;
  char* timeInForce;
  char* clOrdID;
  char* orderCapacity;
  int cumQty;
  char* symbol;
  char* symbolSfx;
  char* execInst;
  char* oldClOrdID;
  double pegDifference;
  char* discretionInst;
  double discretionOffset;
  char* financeInd;
  char* securityID;
  char* targetCompID;
  char* targetSubID;
  int isDoneForDay;
  int revisionSeqNum;
  int replaceQty;
  int64_t usedClientAvailability;
  char* clientAvailabilityKey;
  int isIrregularSettlmnt;

  char* var1;
  char* var2;
  char* var3;
  char* var4;
  char* var5;
  char* var6;
  char* var7;
  char* var8;
  char* var9;

  inline uint32_t getObjectSize(const SerializablePtr& obj) const {
    return (obj == NULLPTR ? 0 : obj->objectSize());
  }

 public:
  EqStruct() {}
  EqStruct(int index);
  virtual ~EqStruct();
  virtual void toData(gemfire::DataOutput& output) const;
  virtual gemfire::Serializable* fromData(gemfire::DataInput& input);
  virtual int32_t classId() const { return 101; }
  CacheableStringPtr toString() const;

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(EqStruct);
    return objectSize;
  }

  int getIndex() { return myIndex; }
  void validate(int index) {
    int encodedIndex = myIndex;
    if (encodedIndex != index) {
      char logmsg[2048];
      sprintf(logmsg, "Expected index %d , got %d.\n", index, encodedIndex);
      throw FwkException(logmsg);
    }
  }
  void update() {
    var1 = (char*)"abcdefghi";
    cumQty = 39;
    usedClientAvailability = 1310447848683LL;
    discretionOffset = 12.3456789;
    resetTimestamp();
  }

  uint64_t getTimestamp() { return timestamp; }
  void resetTimestamp() {
    ACE_Time_Value startTime;
    startTime = ACE_OS::gettimeofday();
    ACE_UINT64 tusec;
    startTime.to_usec(tusec);
    timestamp = tusec * 1000;
  }

  static gemfire::Serializable* createDeserializable() {
    return new EqStruct();
  }
};

typedef gemfire::SharedPtr<EqStruct> EqStructPtr;
}
#endif
