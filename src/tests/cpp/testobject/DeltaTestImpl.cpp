/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
