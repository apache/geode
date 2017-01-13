/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TestObject1.cpp
 *
 *  Created on: Jul 15, 2009
 *      Author: abhaware
 */

#include "TestObject1.hpp"
using namespace testobject;

TestObject1::TestObject1()
    : name(NULLPTR), arr(CacheableBytes::create(4 * 1024)), identifier(1) {}

TestObject1::TestObject1(std::string& str, int32_t id) {
  name = CacheableString::create(str.c_str());
  identifier = id;
  uint8_t* bytes;
  GF_NEW(bytes, uint8_t[1024 * 4]);
  bytes[0] = 'A';
  for (int i = 1; i <= 1024 * 2; i = i * 2) {
    memcpy(bytes + i, bytes, i);
  }
  arr = CacheableBytes::create(bytes, 1024 * 4);
  delete bytes;
}

TestObject1::TestObject1(TestObject1& rhs) {
  name = rhs.name == NULLPTR ? NULLPTR
                             : CacheableString::create(rhs.name->asChar());
  identifier = rhs.identifier;
  arr = CacheableBytes::create(rhs.arr->value(), rhs.arr->length());
}

void TestObject1::toData(DataOutput& output) const {
  output.writeBytes(arr->value(), arr->length());
  output.writeObject(name);
  output.writeInt(identifier);
}

Serializable* TestObject1::fromData(DataInput& input) {
  uint8_t* bytes;
  int32_t len;
  input.readBytes(&bytes, &len);
  arr = CacheableBytes::create(bytes, len);
  delete bytes;
  input.readObject(name);
  input.readInt(&identifier);
  return this;
}

Serializable* TestObject1::create() { return new TestObject1(); }
