#pragma once

#ifndef APACHE_GEODE_GUARD_e0125e37a3eca017a80d206a8d22c2e8
#define APACHE_GEODE_GUARD_e0125e37a3eca017a80d206a8d22c2e8

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
/*
 * TestObject1.hpp
 *
 *  Created on: Jul 15, 2009
 *      Author: abhaware
 */


#include <gfcpp/GeodeCppCache.hpp>
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

using namespace apache::geode::client;
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
}  // namespace testobject

#endif // APACHE_GEODE_GUARD_e0125e37a3eca017a80d206a8d22c2e8
