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

#ifndef __AutoPOSITIONPDX_HPP__
#define __AutoPOSITIONPDX_HPP__

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
#define GFIGNORE(X) X
#define GFEXCLUDE
#define GFID
#define GFARRAYSIZE(X)
#define GFARRAYELEMSIZE(X)

using namespace gemfire;

namespace AutoPdxTests {

class GFIGNORE(TESTOBJECT_EXPORT) PositionPdx
    : public gemfire::PdxSerializable {
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
  GFID char *secId, *secLinks, *secType;
  GFID int32_t sharesOutstanding;
  GFID char* underlyer;
  GFID int64_t volatility;

  GFID int32_t pid;

 public:
  GFEXCLUDE static int32_t cnt;

  PositionPdx();
  PositionPdx(const char* id, int32_t out);
  // This constructor is just for some internal data validation test
  PositionPdx(int32_t iForExactVal);
  virtual ~PositionPdx();
  using PdxSerializable::toData;
  using PdxSerializable::fromData;
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

  static PdxSerializable* createDeserializable();

  const char* getClassName() const;

 private:
  void init();
};

typedef gemfire::SharedPtr<PositionPdx> PositionPdxPtr;
}
#endif
