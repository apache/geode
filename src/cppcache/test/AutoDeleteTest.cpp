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
