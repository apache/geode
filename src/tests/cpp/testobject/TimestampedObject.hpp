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
