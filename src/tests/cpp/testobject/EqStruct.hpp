#pragma once

#ifndef APACHE_GEODE_GUARD_01922391216844a010651b52b4fa268b
#define APACHE_GEODE_GUARD_01922391216844a010651b52b4fa268b

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
 * @brief User class for testing the put functionality for object.
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
  virtual void toData(apache::geode::client::DataOutput& output) const;
  virtual apache::geode::client::Serializable* fromData(
      apache::geode::client::DataInput& input);
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

  static apache::geode::client::Serializable* createDeserializable() {
    return new EqStruct();
  }
};

typedef apache::geode::client::SharedPtr<EqStruct> EqStructPtr;
}  // namespace testobject

#endif // APACHE_GEODE_GUARD_01922391216844a010651b52b4fa268b
