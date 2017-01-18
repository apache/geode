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

#include <InterestResultPolicy.hpp>

using namespace apache::geode::client;

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
