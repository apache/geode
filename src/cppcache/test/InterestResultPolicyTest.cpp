#include <gtest/gtest.h>

#include <InterestResultPolicy.hpp>

using namespace gemfire;

TEST(InterestResultPolicyTest, VerifyOrdinals) {
  EXPECT_NE(InterestResultPolicy::NONE.getOrdinal(),
            InterestResultPolicy::KEYS.getOrdinal())
      << "NONE and KEYS have different ordinals";
  EXPECT_NE(InterestResultPolicy::KEYS.getOrdinal(),
            InterestResultPolicy::KEYS_VALUES.getOrdinal())
      << "KEYS and KEYS_VALUES have different ordinals";
  EXPECT_NE(InterestResultPolicy::KEYS_VALUES.getOrdinal(),
            InterestResultPolicy::NONE.getOrdinal())
      << "KEYS_VALUES and NONE have different ordinals";
}
