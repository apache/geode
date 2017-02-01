#pragma once

#ifndef APACHE_GEODE_GUARD_ae1a70d692bfcc850903bc09f7fe0b96
#define APACHE_GEODE_GUARD_ae1a70d692bfcc850903bc09f7fe0b96

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

#include <gfcpp/GeodeCppCache.hpp>
#include "DeltaTestImpl.hpp"

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
class TESTOBJECT_EXPORT DeltaTestObj : public DeltaTestImpl {
 private:
  int64_t toDeltaCounter;
  int64_t fromDeltaCounter;
  bool enableFailure;

 public:
  DeltaTestObj(int intVal, CacheableStringPtr str, bool enableFail = false)
      : DeltaTestImpl(intVal, str), enableFailure(enableFail) {
    // DeltaTestImpl::DeltaTestImpl(intVal, str);
  }
  /*
  DeltaTestObj(int intVal, std::string str, int64_t toDeltaCnt, bool enableFail
  = false):
    toDeltaCounter(toDeltaCnt),enableFailure(enableFail){
    //DeltaTestImpl::DeltaTestImpl(intVal, str);
  }*/
  DeltaTestObj(DeltaTestObj& obj) : DeltaTestImpl(obj) {}
  DeltaTestObj()
      : DeltaTestImpl(),
        toDeltaCounter(0),
        fromDeltaCounter(0),
        enableFailure(false) {
    // DeltaTestImpl::DeltaTestImpl();
  }
  int64_t getFromDeltaCounter() { return fromDeltaCounter; }
  int64_t getToDeltaCounter() { return toDeltaCounter; }
  void toDelta(DataOutput& out) {
    toDeltaCounter++;
    toDelta(out);
  }
  void fromDelta(DataInput& in) {
    fromDeltaCounter++;
    DeltaTestImpl::fromDelta(in);
  }
  void setFromDeltaCounter(int counter) { fromDeltaCounter = counter; }
  void setToDeltaCounter(int counter) { toDeltaCounter = counter; }
  CacheableStringPtr toString() const {
    char buf[102500];
    sprintf(buf,
            "DeltaTestObj: toDeltaCounter = %lld fromDeltaCounter = %lld\n",
            toDeltaCounter, fromDeltaCounter);
    // DeltaTestImpl::toString();
    return CacheableString::create(buf);
  }
  static Serializable* create() { return new DeltaTestObj(); }
  int32_t classId() const { return 32; }

 protected:
  void checkInvalidInt2(int intVal) {
    if (enableFailure) {
      if (intVal % 30 == 27) {
        throw InvalidDeltaException(
            "Delta could not be applied DeltaTestObj. ");
      }
    }
  }
};

typedef apache::geode::client::SharedPtr<DeltaTestObj> DeltaTestObjPtr;
}

#endif // APACHE_GEODE_GUARD_ae1a70d692bfcc850903bc09f7fe0b96
