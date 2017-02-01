#pragma once

#ifndef GEODE_INTEGRATION_TEST_DELTAEX_H_
#define GEODE_INTEGRATION_TEST_DELTAEX_H_

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

#include "fw_dunit.hpp"
#include <gfcpp/GeodeCppCache.hpp>
#include <ace/OS.h>
#include "CacheHelper.hpp"
class DeltaEx : public Cacheable, public Delta {
 public:
  int counter;
  bool isDelta;

 public:
  static int toDeltaCount;
  static int toDataCount;
  static int fromDeltaCount;
  static int fromDataCount;
  static int cloneCount;
  DeltaEx() {
    counter = 1;
    isDelta = false;
  }
  DeltaEx(int count) {
    counter = 0;
    isDelta = false;
  }
  virtual bool hasDelta() { return isDelta; }
  virtual void toDelta(DataOutput& out) const {
    out.writeInt(counter);
    toDeltaCount++;
  }

  virtual void fromDelta(DataInput& in) {
    LOG("From delta gets called");
    int32_t val;
    in.readInt(&val);
    if (fromDeltaCount == 1) {
      fromDeltaCount++;
      LOG("Invalid Delta expetion thrown");
      throw InvalidDeltaException("aaa", "nnn");
    }
    counter += val;
    fromDeltaCount++;
  }
  virtual void toData(DataOutput& output) const {
    output.writeInt(counter);
    toDataCount++;
  }
  virtual Serializable* fromData(DataInput& input) {
    input.readInt(&counter);
    fromDataCount++;
    return this;
  }
  virtual int32_t classId() const { return 1; }
  virtual uint32_t objectSize() const { return 0; }
  DeltaPtr clone() {
    cloneCount++;
    return DeltaPtr(this);
  }
  virtual ~DeltaEx() {}
  void setDelta(bool delta) { this->isDelta = delta; }
  static Serializable* create() { return new DeltaEx(); }
};

class PdxDeltaEx : public PdxSerializable, public Delta {
 public:
  int m_counter;
  bool m_isDelta;

 public:
  static int m_toDeltaCount;
  static int m_toDataCount;
  static int m_fromDeltaCount;
  static int m_fromDataCount;
  static int m_cloneCount;
  PdxDeltaEx() {
    m_counter = 1;
    m_isDelta = false;
  }
  PdxDeltaEx(int count) {
    m_counter = 0;
    m_isDelta = false;
  }
  virtual bool hasDelta() { return m_isDelta; }
  virtual void toDelta(DataOutput& out) const {
    out.writeInt(m_counter);
    m_toDeltaCount++;
  }

  virtual void fromDelta(DataInput& in) {
    LOG("From delta gets called");
    int32_t val;
    in.readInt(&val);
    if (m_fromDeltaCount == 1) {
      m_fromDeltaCount++;
      LOG("Invalid Delta expetion thrown");
      throw InvalidDeltaException("aaa", "nnn");
    }
    m_counter += val;
    m_fromDeltaCount++;
  }

  const char* getClassName() const { return "PdxTests.PdxDeltaEx"; }

  void toData(PdxWriterPtr pw) {
    pw->writeInt("counter", m_counter);
    m_toDataCount++;
  }

  void fromData(PdxReaderPtr pr) {
    m_counter = pr->readInt("counter");
    m_fromDataCount++;
  }

  static PdxSerializable* createDeserializable() { return new PdxDeltaEx(); }

  DeltaPtr clone() {
    m_cloneCount++;
    return DeltaPtr(this);
  }

  virtual ~PdxDeltaEx() {}

  void setDelta(bool delta) { this->m_isDelta = delta; }

  CacheableStringPtr toString() const {
    char idbuf[1024];
    sprintf(idbuf, "PdxDeltaEx :: [counter=%d]  [isDelta=%d]", m_counter,
            m_isDelta);
    return CacheableString::create(idbuf);
  }
};
typedef SharedPtr<PdxDeltaEx> PdxDeltaExPtr;

#endif // GEODE_INTEGRATION_TEST_DELTAEX_H_
