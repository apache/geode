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

#ifndef __POSITION_HPP__
#define __POSITION_HPP__

#include <gfcpp/GemfireCppCache.hpp>
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

class TESTOBJECT_EXPORT Position : public gemfire::Serializable {
 private:
  int64_t avg20DaysVol;
  CacheableStringPtr bondRating;
  double convRatio;
  CacheableStringPtr country;
  double delta;
  int64_t industry;
  int64_t issuer;
  double mktValue;
  double qty;
  CacheableStringPtr secId;
  CacheableStringPtr secLinks;
  // wchar_t* secType;
  wchar_t* secType;
  int32_t sharesOutstanding;
  CacheableStringPtr underlyer;
  int64_t volatility;
  int32_t pid;

  inline uint32_t getObjectSize(const SerializablePtr& obj) const {
    return (obj == NULLPTR ? 0 : obj->objectSize());
  }

 public:
  static int32_t cnt;

  Position();
  Position(const char* id, int32_t out);
  // This constructor is just for some internal data validation test
  Position(int32_t iForExactVal);
  virtual ~Position();
  virtual void toData(gemfire::DataOutput& output) const;
  virtual gemfire::Serializable* fromData(gemfire::DataInput& input);
  virtual int32_t classId() const { return 0x02; }
  CacheableStringPtr toString() const;

  virtual uint32_t objectSize() const {
    uint32_t objectSize = sizeof(Position);
    objectSize += getObjectSize(bondRating);
    objectSize += getObjectSize(country);
    objectSize += getObjectSize(secId);
    objectSize += getObjectSize(secLinks);
    objectSize +=
        (secType == NULL
             ? 0
             : sizeof(wchar_t) * static_cast<uint32_t>(wcslen(secType)));
    objectSize += getObjectSize(underlyer);
    return objectSize;
  }

  static void resetCounter() { cnt = 0; }
  CacheableStringPtr getSecId() { return secId; }
  int32_t getId() { return pid; }
  int32_t getSharesOutstanding() { return sharesOutstanding; }
  static gemfire::Serializable* createDeserializable() {
    return new Position();
  }

 private:
  void init();
};

typedef gemfire::SharedPtr<Position> PositionPtr;
}
#endif
