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

#include <gtest/gtest.h>

#include <gfcpp/SharedBase.hpp>

using namespace apache::geode::client;

namespace {
class TestSharedBase : public SharedBase {
 public:
  explicit TestSharedBase(bool& destructed) : m_destructed(destructed) {
    // NOP
  }

  virtual ~TestSharedBase() { m_destructed = true; }

 private:
  bool& m_destructed;
};
}  // namespace

TEST(SharedBaseTest, ProperlyInitializedAfterConstructor) {
  bool destructed = false;
  SharedBase* obj = new TestSharedBase(destructed);
  EXPECT_EQ(0, obj->refCount());
}

TEST(SharedBaseTest, PreserveIncrementsCount) {
  bool destructed = false;
  SharedBase* obj = new TestSharedBase(destructed);
  obj->preserveSB();
  EXPECT_EQ(1, obj->refCount());
}

TEST(SharedBaseTest, ReleaseDecrementsCount) {
  bool destructed = false;
  SharedBase* obj = new TestSharedBase(destructed);
  obj->preserveSB();
  obj->releaseSB();
  // Because SharedBase::releaseSB() will take the reference count to
  // zero and thus delete the object, the reference count can no longer
  // safely be inspected as that memory may have already been reused.
  // Thus, inspect the destructed flag which have been set if and only
  // if the reference count went to zero.
  EXPECT_EQ(true, destructed);
}
