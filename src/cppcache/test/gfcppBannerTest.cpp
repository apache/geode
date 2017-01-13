#include <gtest/gtest.h>

#include <gfcppBanner.hpp>

using namespace gemfire;

TEST(gfcppBannerTest, ValidateBanner) {
  EXPECT_LT(0, gfcppBanner::getBanner().size()) << "Non-empty banner";
}
