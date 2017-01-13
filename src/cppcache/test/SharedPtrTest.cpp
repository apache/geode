#include <functional>

#include <gtest/gtest.h>

#include <gfcpp/SharedPtr.hpp>

using namespace gemfire;

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
