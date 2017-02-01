#pragma once

#ifndef APACHE_GEODE_GUARD_3063688f9fda2bf5152f1eec97ed4928
#define APACHE_GEODE_GUARD_3063688f9fda2bf5152f1eec97ed4928

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
class TESTOBJECT_EXPORT PSTObject : public TimestampedObject {
 protected:
  uint64_t timestamp;
  int32_t field1;
  int8_t field2;
  CacheableBytesPtr valueData;

  inline uint32_t getObjectSize(const SerializablePtr& obj) const {
    return (obj == NULLPTR ? 0 : obj->objectSize());
  }

 public:
  PSTObject() : timestamp(0), valueData(NULLPTR) {}
  PSTObject(int size, bool encodeKey, bool encodeTimestamp);
  virtual ~PSTObject();
  virtual void toData(apache::geode::client::DataOutput& output) const;
  virtual apache::geode::client::Serializable* fromData(
      apache::geode::client::DataInput& input);
  virtual int32_t classId() const { return 0x04; }
  CacheableStringPtr toString() const;

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(PSTObject);
    objectSize += getObjectSize(valueData);
    return objectSize;
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
    return new PSTObject();
  }
};

typedef apache::geode::client::SharedPtr<PSTObject> PSTObjectPtr;
}  // namespace testobject

#endif // APACHE_GEODE_GUARD_3063688f9fda2bf5152f1eec97ed4928
