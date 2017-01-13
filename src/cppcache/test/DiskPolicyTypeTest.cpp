#include <gtest/gtest.h>

#include <gfcpp/DiskPolicyType.hpp>

using namespace gemfire;

TEST(DiskPolicyTypeTest, VerifyOrdinalAndNameSymmetryForNone) {
  const char* name = DiskPolicyType::fromOrdinal(0);
  EXPECT_STREQ("none", name) << "Correct name for none";
  const DiskPolicyType::PolicyType policyType = DiskPolicyType::fromName(name);
  EXPECT_EQ(DiskPolicyType::NONE, policyType) << "Correct policy type for none";
}

TEST(DiskPolicyTypeTest, VerifyOrdinalAndNameSymmetryForOverflows) {
  const char* name = DiskPolicyType::fromOrdinal(1);
  EXPECT_STREQ("overflows", name) << "Correct name for overflows";
  const DiskPolicyType::PolicyType policyType = DiskPolicyType::fromName(name);
  EXPECT_EQ(DiskPolicyType::OVERFLOWS, policyType)
      << "Correct policy type for overflows";
}

TEST(DiskPolicyTypeTest, VerifyOrdinalAndNameSymmetryForPersist) {
  const char* name = DiskPolicyType::fromOrdinal(2);
  EXPECT_STREQ("persist", name) << "Correct name for persist";
  const DiskPolicyType::PolicyType policyType = DiskPolicyType::fromName(name);
  EXPECT_EQ(DiskPolicyType::PERSIST, policyType)
      << "Correct policy type for persist";
}

TEST(DiskPolicyTypeTest, ValidateIsNone) {
  EXPECT_EQ(true, DiskPolicyType::isNone(DiskPolicyType::NONE))
      << "NONE is none";
  EXPECT_EQ(false, DiskPolicyType::isNone(DiskPolicyType::OVERFLOWS))
      << "OVERFLOWS is not none";
  EXPECT_EQ(false, DiskPolicyType::isNone(DiskPolicyType::PERSIST))
      << "PERSIST is not none";
}

TEST(DiskPolicyTypeTest, ValidateIsOverflow) {
  EXPECT_EQ(false, DiskPolicyType::isOverflow(DiskPolicyType::NONE))
      << "NONE is not overflow";
  EXPECT_EQ(true, DiskPolicyType::isOverflow(DiskPolicyType::OVERFLOWS))
      << "OVERFLOWS is overflow";
  EXPECT_EQ(false, DiskPolicyType::isOverflow(DiskPolicyType::PERSIST))
      << "PERSIST is not overflow";
}

TEST(DiskPolicyTypeTest, ValidateIsPersist) {
  EXPECT_EQ(false, DiskPolicyType::isPersist(DiskPolicyType::NONE))
      << "NONE is not persist";
  EXPECT_EQ(false, DiskPolicyType::isPersist(DiskPolicyType::OVERFLOWS))
      << "OVERFLOWS is not persist";
  EXPECT_EQ(true, DiskPolicyType::isPersist(DiskPolicyType::PERSIST))
      << "PERSIST is persist";
}
