#include <gtest/gtest.h>

#include <gfcpp/SharedBase.hpp>

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
