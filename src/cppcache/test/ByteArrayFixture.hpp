#pragma once

#include <gtest/gtest.h>

#include "ByteArray.hpp"

#define EXPECT_BYTEARRAY_EQ(e, a) \
  EXPECT_PRED_FORMAT2(assertByteArrayEqual, e, a)

class ByteArrayFixture {
 public:
  ::testing::AssertionResult assertByteArrayEqual(
      const char* expectedStr, const char* bytesStr, const char* expected,
      const gemfire::ByteArray& bytes);
};
