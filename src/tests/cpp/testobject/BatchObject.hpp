#pragma once

#ifndef APACHE_GEODE_GUARD_018c280d1ad9e881b092dcb9f3304d88
#define APACHE_GEODE_GUARD_018c280d1ad9e881b092dcb9f3304d88

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
 * @brief User class for testing the cq functionality.
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
  virtual void toData(apache::geode::client::DataOutput& output) const;
  virtual apache::geode::client::Serializable* fromData(
      apache::geode::client::DataInput& input);
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

  static apache::geode::client::Serializable* createDeserializable() {
    return new BatchObject();
  }
};

typedef apache::geode::client::SharedPtr<BatchObject> BatchObjectPtr;
}  // namespace testobject

#endif // APACHE_GEODE_GUARD_018c280d1ad9e881b092dcb9f3304d88
