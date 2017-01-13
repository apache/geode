/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/*
 * @brief User class for testing the cq functionality.
 */

#ifndef __BATCHOBJECT_HPP__
#define __BATCHOBJECT_HPP__

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
class TESTOBJECT_EXPORT BatchObject : public TimestampedObject {
 private:
  int32_t index;
  uint64_t timestamp;
  int32_t batch;
  CacheableBytesPtr byteArray;

  inline uint32_t getObjectSize(const SerializablePtr& obj) const {
    return (obj == NULLPTR ? 0 : obj->objectSize());
  }

 public:
  BatchObject() : index(0), timestamp(0), batch(0), byteArray(NULLPTR) {}
  BatchObject(int32_t anIndex, int32_t batchSize, int32_t size);
  virtual ~BatchObject();
  virtual void toData(gemfire::DataOutput& output) const;
  virtual gemfire::Serializable* fromData(gemfire::DataInput& input);
  virtual int32_t classId() const { return 25; }
  CacheableStringPtr toString() const;

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(BatchObject);
    return objectSize;
  }

  uint64_t getTimestamp() { return timestamp; }
  int getIndex() { return index; }
  int getBatch() { return batch; }
  void resetTimestamp() {
    ACE_Time_Value startTime;
    startTime = ACE_OS::gettimeofday();
    ACE_UINT64 tusec;
    startTime.to_usec(tusec);
    timestamp = tusec * 1000;
  }

  static gemfire::Serializable* createDeserializable() {
    return new BatchObject();
  }
};

typedef gemfire::SharedPtr<BatchObject> BatchObjectPtr;
}
#endif
