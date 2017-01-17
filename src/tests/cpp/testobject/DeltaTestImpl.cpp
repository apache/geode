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
 *DeltaTestImpl.cpp
 *
 *Created on: Jul 14, 2009
 *Author: abhaware
 */
#include "DeltaTestImpl.hpp"
#include <ace/Guard_T.h>
using namespace testobject;

uint8_t DeltaTestImpl::INT_MASK = 0x1;
uint8_t DeltaTestImpl::STR_MASK = 0x2;
uint8_t DeltaTestImpl::DOUBLE_MASK = 0x4;
uint8_t DeltaTestImpl::BYTE_ARR_MASK = 0x8;
uint8_t DeltaTestImpl::TEST_OBJ_MASK = 0x10;
uint8_t DeltaTestImpl::COMPLETE_MASK = 0x1F;

DeltaTestImpl::DeltaTestImpl() {
  intVar = 1;
  str = CacheableString::create("test");
  doubleVar = 1.1;
  uint8_t byte = 'A';
  byteArr = CacheableBytes::create(&byte, 1);
  testObj = NULLPTR;
  m_hasDelta = false;
  deltaBits = 0;
  toDeltaCounter = 0;
  fromDeltaCounter = 0;
}
DeltaTestImpl::DeltaTestImpl(int intValue, CacheableStringPtr strptr)
    : intVar(intValue),
      str(strptr),
      doubleVar(0),
      toDeltaCounter(0),
      fromDeltaCounter(0) {}
DeltaTestImpl::DeltaTestImpl(DeltaTestImplPtr rhs) {
  intVar = rhs->intVar;
  str = CacheableString::create(rhs->str->asChar());
  doubleVar = rhs->doubleVar;
  byteArr = (rhs->byteArr == NULLPTR ? NULLPTR : CacheableBytes::create(
                                                     rhs->byteArr->value(),
                                                     rhs->byteArr->length()));
  testObj = (rhs->testObj == NULLPTR
                 ? NULLPTR
                 : TestObject1Ptr(new TestObject1(*(rhs->testObj.ptr()))));
  toDeltaCounter = rhs->getToDeltaCounter();
  fromDeltaCounter = rhs->getFromDeltaCounter();
}

Serializable* DeltaTestImpl::fromData(DataInput& input) {
  input.readInt(&intVar);
  input.readObject(str);
  input.readDouble(&doubleVar);
  input.readObject(byteArr);
  input.readObject(testObj);
  return this;
}

void DeltaTestImpl::toData(DataOutput& output) const {
  output.writeInt(intVar);
  output.writeObject(str);
  output.writeDouble(doubleVar);
  output.writeObject(byteArr);
  output.writeObject(testObj);
}

void DeltaTestImpl::toDelta(DataOutput& output) const {
  {
    ACE_Recursive_Thread_Mutex* lock =
        const_cast<ACE_Recursive_Thread_Mutex*>(&m_lock);
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(*lock);
    toDeltaCounter++;
  }
  output.write(deltaBits);
  if ((deltaBits & INT_MASK) == INT_MASK) {
    output.writeInt(intVar);
  }
  if ((deltaBits & STR_MASK) == STR_MASK) {
    output.writeObject(str);
  }
  if ((deltaBits & DOUBLE_MASK) == DOUBLE_MASK) {
    output.writeDouble(doubleVar);
  }
  if ((deltaBits & BYTE_ARR_MASK) == BYTE_ARR_MASK) {
    output.writeObject(byteArr);
  }
  if ((deltaBits & TEST_OBJ_MASK) == TEST_OBJ_MASK) {
    output.writeObject(testObj);
  }
}

void DeltaTestImpl::fromDelta(DataInput& input) {
  {
    ACE_Guard<ACE_Recursive_Thread_Mutex> _guard(m_lock);
    fromDeltaCounter++;
  }
  input.read(&deltaBits);
  if ((deltaBits & INT_MASK) == INT_MASK) {
    input.readInt(&intVar);
  }
  if ((deltaBits & STR_MASK) == STR_MASK) {
    input.readObject(str);
  }
  if ((deltaBits & DOUBLE_MASK) == DOUBLE_MASK) {
    input.readDouble(&doubleVar);
  }
  if ((deltaBits & BYTE_ARR_MASK) == BYTE_ARR_MASK) {
    input.readObject(byteArr);
    /*
        uint8_t* bytes;
        int32_t len;
        input.readBytes( &bytes, &len );
        byteArr = CacheableBytes::create( bytes, len );
        delete bytes;
    */
  }
  if ((deltaBits & TEST_OBJ_MASK) == TEST_OBJ_MASK) {
    input.readObject(testObj);
  }
}
CacheableStringPtr DeltaTestImpl::toString() const {
  char buf[102500];
  sprintf(buf, "DeltaTestImpl[hasDelta=%d int=%d double=%f str=%s \n",
          m_hasDelta, intVar, doubleVar, str->toString());
  return CacheableString::create(buf);
}
