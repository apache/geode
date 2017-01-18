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

#include <gtest/gtest.h>

#include <gfcpp/CacheableKeys.hpp>

using namespace apache::geode::client;

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
