#include <gtest/gtest.h>

#include <gfcpp/CacheableKeys.hpp>

using namespace gemfire;

TEST(CacheableKeysTest, boolDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode(false), serializer::hashcode(true))
      << "Two different bool values have different hashcodes";
}

TEST(CacheableKeysTest, uint8_tDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode((uint8_t)37U),
            serializer::hashcode((uint8_t)42U))
      << "Two different uint8_t values have different hashcodes";
}

TEST(CacheableKeysTest, int8_tDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode((int8_t)37), serializer::hashcode((int8_t)42))
      << "Two different int8_t values have different hashcodes";
}

TEST(CacheableKeysTest, uint16_tDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode((uint16_t)37U),
            serializer::hashcode((uint16_t)42U))
      << "Two different uint16_t values have different hashcodes";
}

TEST(CacheableKeysTest, int16_tDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode((int16_t)37),
            serializer::hashcode((int16_t)42))
      << "Two different int16_t values have different hashcodes";
}

TEST(CacheableKeysTest, uint32_tDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode((uint32_t)37U),
            serializer::hashcode((uint32_t)42U))
      << "Two different uint32_t values have different hashcodes";
}

TEST(CacheableKeysTest, int32_tDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode((int32_t)37),
            serializer::hashcode((int32_t)42))
      << "Two different int32_t values have different hashcodes";
}

TEST(CacheableKeysTest, uint64_tDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode((uint64_t)37U),
            serializer::hashcode((uint64_t)42U))
      << "Two different uint64_t values have different hashcodes";
}

TEST(CacheableKeysTest, int64_tDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode((int64_t)37),
            serializer::hashcode((int64_t)42))
      << "Two different int64_t values have different hashcodes";
}

TEST(CacheableKeysTest, floatDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode(37.F), serializer::hashcode(42.F))
      << "Two different float values have different hashcodes";
}

TEST(CacheableKeysTest, doubleDifferentHashCodes) {
  EXPECT_NE(serializer::hashcode(37.), serializer::hashcode(42.))
      << "Two different double values have different hashcodes";
}
