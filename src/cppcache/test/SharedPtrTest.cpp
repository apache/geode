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

#include <functional>

#include <gtest/gtest.h>

#include <gfcpp/SharedPtr.hpp>

using namespace apache::geode::client;

class NotifyOnDelete : public SharedBase {
 public:
  explicit NotifyOnDelete(bool &deleted) : deleted(deleted) {}

  ~NotifyOnDelete() { deleted = true; }

 private:
  bool &deleted;
};

TEST(SharedPtrTest, ASharedPtrToASharedBaseHasAnInitialReferenceCountOfOne) {
  SharedPtr<SharedBase> my_pointer = SharedPtr<SharedBase>(new SharedBase());

  EXPECT_EQ(1, my_pointer->refCount());
}

TEST(SharedPtrTest, ASharedBaseWithoutASharedPtrHasAReferenceCountOfZero) {
  SharedBase *my_object = new SharedBase();

  EXPECT_EQ(0, my_object->refCount());
}

TEST(SharedPtrTest, AddingReferenceToASharedPtrIncrementsReferenceCount) {
  SharedPtr<SharedBase> my_pointer = SharedPtr<SharedBase>(new SharedBase());
  SharedPtr<SharedBase> your_pointer = my_pointer;

  EXPECT_EQ(2, my_pointer->refCount());
  EXPECT_EQ(2, your_pointer->refCount());
}

TEST(SharedPtrTest, CreatingSharedPtrFromSharedPtrIncrementsReferenceCount) {
  SharedPtr<SharedBase> my_pointer = SharedPtr<SharedBase>(new SharedBase());
  SharedPtr<SharedBase> your_pointer = SharedPtr<SharedBase>(my_pointer);

  EXPECT_EQ(2, my_pointer->refCount());
  EXPECT_EQ(2, your_pointer->refCount());
}

TEST(SharedPtrTest, CallingImplicitDestructorWillDecrementReferenceCount) {
  SharedPtr<SharedBase> my_pointer = SharedPtr<SharedBase>(new SharedBase());
  {
    SharedPtr<SharedBase> your_pointer = SharedPtr<SharedBase>(my_pointer);

    // At following "}" your_pointer reference is destroyed
    EXPECT_EQ(2, my_pointer->refCount());
  }

  EXPECT_EQ(1, my_pointer->refCount());
}

TEST(SharedPtrTest, CallingExplicitDestructorWillDecrementReferenceCount) {
  SharedPtr<SharedBase> *my_pointer =
      new SharedPtr<SharedBase>(new SharedBase());
  SharedPtr<SharedBase> *your_pointer = new SharedPtr<SharedBase>(*my_pointer);

  EXPECT_EQ(2, (*my_pointer)->refCount());
  delete your_pointer;

  EXPECT_EQ(1, (*my_pointer)->refCount());
}

TEST(SharedPtrTest, SharedPtrIsDestroyedWhenReferenceCountIsZero) {
  bool is_shared_object_deleted = false;

  SharedPtr<NotifyOnDelete> *my_pointer = new SharedPtr<NotifyOnDelete>(
      new NotifyOnDelete(is_shared_object_deleted));

  delete my_pointer;

  EXPECT_TRUE(is_shared_object_deleted);
}

TEST(SharedPtrTest, SharedPtrIsNotDestroyedUntilReferenceCountIsZero) {
  bool is_shared_object_deleted = false;
  {
    SharedPtr<NotifyOnDelete> my_pointer =
        SharedPtr<NotifyOnDelete>(new NotifyOnDelete(is_shared_object_deleted));

    {
      SharedPtr<NotifyOnDelete> your_pointer = my_pointer;

      EXPECT_EQ(false, is_shared_object_deleted);
    }

    EXPECT_EQ(false, is_shared_object_deleted);
  }

  EXPECT_EQ(true, is_shared_object_deleted);
}
