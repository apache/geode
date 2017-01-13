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

#ifndef __POSITIONPDX_AUTO__HPP__
#define __POSITIONPDX_AUTO__HPP__

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

#define GFID

class PositionPdxAuto : public gemfire::PdxSerializable {
 private:
  GFID int64_t avg20DaysVol;
  GFID char* bondRating;
  GFID double convRatio;
  GFID char* country;
  GFID double delta;
  GFID int64_t industry;
  GFID int64_t issuer;
  GFID double mktValue;
  GFID double qty;
  GFID char* secId;
  GFID char* secLinks;
  // wchar_t* secType;
  // wchar_t* secType;
  GFID char* secType;
  GFID int32_t sharesOutstanding;
  GFID char* underlyer;
  GFID int64_t volatility;

  GFID int32_t pid;

 public:
  static int32_t cnt;

  PositionPdxAuto();
  PositionPdxAuto(const char* id, int32_t out);
  // This constructor is just for some internal data validation test
  PositionPdxAuto(int32_t iForExactVal);
  virtual ~PositionPdxAuto();
  virtual void toData(PdxWriterPtr pw);
  virtual void fromData(PdxReaderPtr pr);

  CacheableStringPtr toString() const;

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(PositionPdxAuto);
    return objectSize;
  }

  static void resetCounter() { cnt = 0; }

  char* getSecId() { return secId; }

  int32_t getId() { return pid; }

  int32_t getSharesOutstanding() { return sharesOutstanding; }

  static PdxSerializable* createDeserializable();

  const char* getClassName() const;

 private:
  void init();
};

typedef gemfire::SharedPtr<PositionPdxAuto> PositionPdxPtr;
}
#endif
