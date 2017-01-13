#include <gtest/gtest.h>

#include <NanoTimer.hpp>

using namespace gemfire;

TEST(NanoTimerTest, ValidateSleep) {
  const int64_t before = NanoTimer::now();
  NanoTimer::sleep(3000000UL /* nano-seconds */);
  const int64_t after = NanoTimer::now();
  EXPECT_LE(3000000L /* nano-seconds */, (after - before))
      << "Slept at least three milli-seconds";
}
