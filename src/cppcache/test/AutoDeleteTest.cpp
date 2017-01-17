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
#include <AutoDelete.hpp>

using namespace gemfire;

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

TEST(AutoDeleteTest, DestructorDoesDeleteByDefault) {
  bool destructed = false;
  TestSharedBase* ptr = new TestSharedBase(destructed);
  { gemfire::DeleteObject<TestSharedBase> obj(ptr); }
  EXPECT_EQ(true, destructed);
}

TEST(AutoDeleteTest, DestructorDoesNotDeleteWhenAsked) {
  bool destructed = false;
  TestSharedBase* ptr = new TestSharedBase(destructed);
  {
    gemfire::DeleteObject<TestSharedBase> obj(ptr);
    obj.noDelete();
  }
  EXPECT_EQ(false, destructed);
  delete ptr;
  EXPECT_EQ(true, destructed);
}
