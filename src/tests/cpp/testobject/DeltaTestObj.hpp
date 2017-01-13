/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __DeltaTestObj_HPP__
#define __DeltaTestObj_HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include "DeltaTestImpl.hpp"

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
using namespace testframework;
namespace testobject {
class TESTOBJECT_EXPORT DeltaTestObj : public DeltaTestImpl {
 private:
  int64_t toDeltaCounter;
  int64_t fromDeltaCounter;
  bool enableFailure;

 public:
  DeltaTestObj(int intVal, CacheableStringPtr str, bool enableFail = false)
      : DeltaTestImpl(intVal, str), enableFailure(enableFail) {
    // DeltaTestImpl::DeltaTestImpl(intVal, str);
  }
  /*
  DeltaTestObj(int intVal, std::string str, int64_t toDeltaCnt, bool enableFail
  = false):
    toDeltaCounter(toDeltaCnt),enableFailure(enableFail){
    //DeltaTestImpl::DeltaTestImpl(intVal, str);
  }*/
  DeltaTestObj(DeltaTestObj& obj) : DeltaTestImpl(obj) {}
  DeltaTestObj()
      : DeltaTestImpl(),
        toDeltaCounter(0),
        fromDeltaCounter(0),
        enableFailure(false) {
    // DeltaTestImpl::DeltaTestImpl();
  }
  int64_t getFromDeltaCounter() { return fromDeltaCounter; }
  int64_t getToDeltaCounter() { return toDeltaCounter; }
  void toDelta(DataOutput& out) {
    toDeltaCounter++;
    toDelta(out);
  }
  void fromDelta(DataInput& in) {
    fromDeltaCounter++;
    DeltaTestImpl::fromDelta(in);
  }
  void setFromDeltaCounter(int counter) { fromDeltaCounter = counter; }
  void setToDeltaCounter(int counter) { toDeltaCounter = counter; }
  CacheableStringPtr toString() const {
    char buf[102500];
    sprintf(buf,
            "DeltaTestObj: toDeltaCounter = %lld fromDeltaCounter = %lld\n",
            toDeltaCounter, fromDeltaCounter);
    // DeltaTestImpl::toString();
    return CacheableString::create(buf);
  }
  static Serializable* create() { return new DeltaTestObj(); }
  int32_t classId() const { return 32; }

 protected:
  void checkInvalidInt2(int intVal) {
    if (enableFailure) {
      if (intVal % 30 == 27) {
        throw InvalidDeltaException(
            "Delta could not be applied DeltaTestObj. ");
      }
    }
  }
};

typedef gemfire::SharedPtr<DeltaTestObj> DeltaTestObjPtr;
}
#endif
