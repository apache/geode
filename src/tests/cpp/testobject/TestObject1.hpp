/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * TestObject1.hpp
 *
 *  Created on: Jul 15, 2009
 *      Author: abhaware
 */

#ifndef TESTOBJECT1_HPP_
#define TESTOBJECT1_HPP_

#include <gfcpp/GemfireCppCache.hpp>
#include <string>

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
class TESTOBJECT_EXPORT TestObject1 : public Cacheable {
 private:
  CacheableStringPtr name;
  CacheableBytesPtr arr;
  int32_t identifier;

 public:
  TestObject1();
  TestObject1(int32_t id)
      : name(NULLPTR), arr(CacheableBytes::create(4 * 1024)), identifier(id) {}
  TestObject1(std::string& str, int32_t id);
  TestObject1(TestObject1& rhs);
  void toData(DataOutput& output) const;
  Serializable* fromData(DataInput& input);

  int32_t getIdentifier() { return identifier; }

  int32_t classId() const { return 31; }

  uint32_t objectSize() const { return 0; }

  static Serializable* create();
};

typedef SharedPtr<TestObject1> TestObject1Ptr;
}
#endif /* TESTOBJECT1_HPP_ */
