#pragma once

#ifndef APACHE_GEODE_GUARD_f0dde88a60c43a402c317bb0bafa6c73
#define APACHE_GEODE_GUARD_f0dde88a60c43a402c317bb0bafa6c73

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
 * DeltaTestImpl.hpp
 *
 *  Created on: Jul 14, 2009
 *      Author: abhaware
 */


#include <ace/ACE.h>
#include <ace/OS.h>
#include <ace/Task.h>
#include <ace/Condition_T.h>
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/Time_Value.h>
#include "TestObject1.hpp"

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

namespace testobject {
class DeltaTestImpl;
typedef SharedPtr<DeltaTestImpl> DeltaTestImplPtr;
class TESTOBJECT_EXPORT DeltaTestImpl : public Cacheable, public Delta {
 private:
  static uint8_t INT_MASK;
  static uint8_t STR_MASK;
  static uint8_t DOUBLE_MASK;
  static uint8_t BYTE_ARR_MASK;
  static uint8_t TEST_OBJ_MASK;
  static uint8_t COMPLETE_MASK;

  int32_t intVar;             // 0000 0001
  CacheableStringPtr str;     // 0000 0010
  double doubleVar;           // 0000 0100
  CacheableBytesPtr byteArr;  // 0000 1000
  TestObject1Ptr testObj;     // 0001 0000

  bool m_hasDelta;
  uint8_t deltaBits;
  mutable int64_t toDeltaCounter;
  int64_t fromDeltaCounter;
  ACE_Recursive_Thread_Mutex m_lock;

 public:
  DeltaTestImpl();
  DeltaTestImpl(int intValue, CacheableStringPtr strptr);
  DeltaTestImpl(DeltaTestImplPtr rhs);
  Serializable* fromData(DataInput& input);
  void toData(DataOutput& output) const;

  void fromDelta(DataInput& input);
  void toDelta(DataOutput& output) const;
  bool hasDelta() { return m_hasDelta; }

  int32_t classId() const { return 30; }

  uint32_t objectSize() const { return 0; }

  DeltaPtr clone() {
    DeltaPtr clonePtr(this);
    return clonePtr;
    // return DeltaPtr( this );
  }

  static Serializable* create() { return new DeltaTestImpl(); }

  int32_t getIntVar() { return intVar; }
  CacheableStringPtr getStr() { return str; }
  double getDoubleVar() { return doubleVar; }
  CacheableBytesPtr getByteArr() { return byteArr; }
  TestObject1Ptr getTestObj() { return testObj; }

  int64_t getFromDeltaCounter() { return fromDeltaCounter; }
  int64_t getToDeltaCounter() { return toDeltaCounter; }

  void setIntVar(int32_t value) {
    intVar = value;
    deltaBits |= INT_MASK;
    m_hasDelta = true;
  }
  void setStr(char* str1) { str = CacheableString::create(str1); }
  void setDoubleVar(double value) { doubleVar = value; }
  void setByteArr(CacheableBytesPtr value) { byteArr = value; }
  void setTestObj(TestObject1Ptr value) { testObj = value; }

  void setDelta(bool value) { m_hasDelta = value; }

  void setIntVarFlag() { deltaBits = deltaBits | INT_MASK; }
  void setStrFlag() { deltaBits = deltaBits | STR_MASK; }
  void setDoubleVarFlag() { deltaBits = deltaBits | DOUBLE_MASK; }
  void setByteArrFlag() { deltaBits = deltaBits | BYTE_ARR_MASK; }
  void setTestObjFlag() { deltaBits = deltaBits | TEST_OBJ_MASK; }
  CacheableStringPtr toString() const;
};
}  // namespace testobject

#endif // APACHE_GEODE_GUARD_f0dde88a60c43a402c317bb0bafa6c73
