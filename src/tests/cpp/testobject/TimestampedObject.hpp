#pragma once

#ifndef APACHE_GEODE_GUARD_f3340c9b2d447c884bfa8bef6b826bb9
#define APACHE_GEODE_GUARD_f3340c9b2d447c884bfa8bef6b826bb9

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

#include <gfcpp/GeodeCppCache.hpp>

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
using namespace testframework;

namespace testobject {

class TESTOBJECT_EXPORT TimestampedObject
    : public apache::geode::client::Serializable {
 public:
  virtual uint64_t getTimestamp() { return 0; }
  virtual void resetTimestamp() {}
  virtual Serializable* fromData(apache::geode::client::DataInput& input) {
    return this;
  }
  virtual void toData(apache::geode::client::DataOutput& output) const {}
  virtual int32_t classId() const { return 0; }
  virtual uint32_t objectSize() const { return 0; }
  virtual ~TimestampedObject() {}
};
typedef apache::geode::client::SharedPtr<TimestampedObject>
    TimestampedObjectPtr;
}  // namespace testobject

#endif // APACHE_GEODE_GUARD_f3340c9b2d447c884bfa8bef6b826bb9
