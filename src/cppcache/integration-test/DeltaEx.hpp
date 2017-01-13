/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __DELTAEX_HPP
#define __DELTAEX_HPP

#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
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
#endif
