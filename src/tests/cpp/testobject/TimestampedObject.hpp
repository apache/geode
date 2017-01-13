/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef __TimestampedObjectHPP__
#define __TimestampedObjectHPP__

#include <gfcpp/GemfireCppCache.hpp>

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

class TESTOBJECT_EXPORT TimestampedObject : public gemfire::Serializable {
 public:
  virtual uint64_t getTimestamp() { return 0; }
  virtual void resetTimestamp() {}
  virtual Serializable* fromData(gemfire::DataInput& input) { return this; }
  virtual void toData(gemfire::DataOutput& output) const {}
  virtual int32_t classId() const { return 0; }
  virtual uint32_t objectSize() const { return 0; }
  virtual ~TimestampedObject() {}
};
typedef gemfire::SharedPtr<TimestampedObject> TimestampedObjectPtr;
}
#endif  // __TimestampedObjectHPP__
