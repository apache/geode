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

#ifndef __DELTAPSTOBJECT_HPP__
#define __DELTAPSTOBJECT_HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include <string.h>
#include "fwklib/Timer.hpp"
#include "fwklib/FrameworkTest.hpp"
#include "TimestampedObject.hpp"
#include "testobject/PSTObject.hpp"
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
class TESTOBJECT_EXPORT DeltaPSTObject : public Cacheable, public Delta {
 private:
  uint64_t timestamp;
  int32_t field1;
  int8_t field2;
  CacheableBytesPtr valueData;

 public:
  DeltaPSTObject() : timestamp(0), valueData(NULLPTR) {}
  DeltaPSTObject(int size, bool encodeKey, bool encodeTimestamp);
  virtual ~DeltaPSTObject() {}
  void toData(gemfire::DataOutput& output) const;
  gemfire::Serializable* fromData(gemfire::DataInput& input);
  void fromDelta(DataInput& input);
  void toDelta(DataOutput& output) const;
  CacheableStringPtr toString() const;
  bool hasDelta() { return true; }
  int32_t classId() const { return 42; }

  uint32_t objectSize() const {
    uint32_t objectSize = sizeof(DeltaPSTObject);
    return objectSize;
  }
  void incrementField1() { ++field1; }

  void update() {
    incrementField1();
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
  DeltaPtr clone() {
    DeltaPtr clonePtr(this);
    return clonePtr;
  }

  static Serializable* createDeserializable() { return new DeltaPSTObject(); }
};
typedef gemfire::SharedPtr<DeltaPSTObject> DeltaPSTObjectPtr;
}
#endif
