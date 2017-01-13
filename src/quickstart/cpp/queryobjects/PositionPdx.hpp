/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/*
 * @brief User class for testing the put functionality for object.
 */

#ifndef __POSITIONPDX_HPP__
#define __POSITIONPDX_HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/PdxSerializable.hpp>
#include <gfcpp/PdxWriter.hpp>
#include <gfcpp/PdxReader.hpp>
#include <string.h>

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

namespace testobject {

class TESTOBJECT_EXPORT PositionPdx : public gemfire::PdxSerializable {
 private:
  int64_t avg20DaysVol;
  char* bondRating;
  double convRatio;
  char* country;
  double delta;
  int64_t industry;
  int64_t issuer;
  double mktValue;
  double qty;
  char* secId;
  char* secLinks;
  // wchar_t* secType;
  // wchar_t* secType;
  char* secType;
  int32_t sharesOutstanding;
  char* underlyer;
  int64_t volatility;

  int32_t pid;

 public:
  static int32_t cnt;

  PositionPdx();
  PositionPdx(const char* id, int32_t out);
  // This constructor is just for some internal data validation test
  PositionPdx(int32_t iForExactVal);
  virtual ~PositionPdx();
  virtual void toData(PdxWriterPtr pw);
  virtual void fromData(PdxReaderPtr pr);

  CacheableStringPtr toString() const;

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(PositionPdx);
    return objectSize;
  }

  static void resetCounter() { cnt = 0; }

  char* getSecId() { return secId; }

  int32_t getId() { return pid; }

  int32_t getSharesOutstanding() { return sharesOutstanding; }

  static PdxSerializable* createDeserializable() { return new PositionPdx(); }

  const char* getClassName() const { return "testobject.PositionPdx"; }

 private:
  void init();
};

typedef gemfire::SharedPtr<PositionPdx> PositionPdxPtr;
}
#endif
