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

#include <gfcpp/ExpirationAction.hpp>

using namespace apache::geode::client;

TEST(ExpirationActionTest, VerifyOrdinalAndNameSymmetryForInvalidate) {
  const char* name = ExpirationAction::fromOrdinal(0);
  EXPECT_STREQ("INVALIDATE", name) << "Correct name for invalidate";
  const ExpirationAction::Action action = ExpirationAction::fromName(name);
  EXPECT_EQ(ExpirationAction::INVALIDATE, action)
      << "Correct action for invalidate";
}

TEST(ExpirationActionTest, VerifyOrdinalAndNameSymmetryForLocalInvalidate) {
  const char* name = ExpirationAction::fromOrdinal(1);
  EXPECT_STREQ("LOCAL_INVALIDATE", name) << "Correct name for local invalidate";
  const ExpirationAction::Action action = ExpirationAction::fromName(name);
  EXPECT_EQ(ExpirationAction::LOCAL_INVALIDATE, action)
      << "Correct action for local invalidate";
}

TEST(ExpirationActionTest, VerifyOrdinalAndNameSymmetryForDestroy) {
  const char* name = ExpirationAction::fromOrdinal(2);
  EXPECT_STREQ("DESTROY", name) << "Correct name for destroy";
  const ExpirationAction::Action action = ExpirationAction::fromName(name);
  EXPECT_EQ(ExpirationAction::DESTROY, action) << "Correct action for destroy";
}

TEST(ExpirationActionTest, VerifyOrdinalAndNameSymmetryForLocalDestroy) {
  const char* name = ExpirationAction::fromOrdinal(3);
  EXPECT_STREQ("LOCAL_DESTROY", name) << "Correct name for local destroy";
  const ExpirationAction::Action action = ExpirationAction::fromName(name);
  EXPECT_EQ(ExpirationAction::LOCAL_DESTROY, action)
      << "Correct action for local destroy";
}

TEST(ExpirationActionTest, ValidateIsInvalidate) {
  EXPECT_EQ(true, ExpirationAction::isInvalidate(ExpirationAction::INVALIDATE))
      << "INVALIDATE is invalidate";
  EXPECT_EQ(false,
            ExpirationAction::isInvalidate(ExpirationAction::LOCAL_INVALIDATE))
      << "LOCAL_INVALIDATE is not invalidate";
  EXPECT_EQ(false, ExpirationAction::isInvalidate(ExpirationAction::DESTROY))
      << "DESTROY is not invalidate";
  EXPECT_EQ(false,
            ExpirationAction::isInvalidate(ExpirationAction::LOCAL_DESTROY))
      << "LOCAL_DESTROY is not invalidate";
  EXPECT_EQ(false,
            ExpirationAction::isInvalidate(ExpirationAction::INVALID_ACTION))
      << "INVALID_ACTION is not invalidate";
}

TEST(ExpirationActionTest, ValidateIsLocalInvalidate) {
  EXPECT_EQ(false,
            ExpirationAction::isLocalInvalidate(ExpirationAction::INVALIDATE))
      << "INVALIDATE is not local invalidate";
  EXPECT_EQ(true, ExpirationAction::isLocalInvalidate(
                      ExpirationAction::LOCAL_INVALIDATE))
      << "LOCAL_INVALIDATE is local invalidate";
  EXPECT_EQ(false,
            ExpirationAction::isLocalInvalidate(ExpirationAction::DESTROY))
      << "DESTROY is not local invalidate";
  EXPECT_EQ(false, ExpirationAction::isLocalInvalidate(
                       ExpirationAction::LOCAL_DESTROY))
      << "LOCAL_DESTROY is not local invalidate";
  EXPECT_EQ(false, ExpirationAction::isLocalInvalidate(
                       ExpirationAction::INVALID_ACTION))
      << "INVALID_ACTION is not local invalidate";
}

TEST(ExpirationActionTest, ValidateIsDestroy) {
  EXPECT_EQ(false, ExpirationAction::isDestroy(ExpirationAction::INVALIDATE))
      << "INVALIDATE is not destroy";
  EXPECT_EQ(false,
            ExpirationAction::isDestroy(ExpirationAction::LOCAL_INVALIDATE))
      << "LOCAL_INVALIDATE is not destroy";
  EXPECT_EQ(true, ExpirationAction::isDestroy(ExpirationAction::DESTROY))
      << "DESTROY is destroy";
  EXPECT_EQ(false, ExpirationAction::isDestroy(ExpirationAction::LOCAL_DESTROY))
      << "LOCAL_DESTROY is not destroy";
  EXPECT_EQ(false,
            ExpirationAction::isDestroy(ExpirationAction::INVALID_ACTION))
      << "INVALID_ACTION is not destroy";
}

TEST(ExpirationActionTest, ValidateIsLocalDestroy) {
  EXPECT_EQ(false,
            ExpirationAction::isLocalDestroy(ExpirationAction::INVALIDATE))
      << "INVALIDATE is not local destroy";
  EXPECT_EQ(false, ExpirationAction::isLocalDestroy(
                       ExpirationAction::LOCAL_INVALIDATE))
      << "LOCAL_INVALIDATE is not local destroy";
  EXPECT_EQ(false, ExpirationAction::isLocalDestroy(ExpirationAction::DESTROY))
      << "DESTROY is not local destroy";
  EXPECT_EQ(true,
            ExpirationAction::isLocalDestroy(ExpirationAction::LOCAL_DESTROY))
      << "LOCAL_DESTROY is local destroy";
  EXPECT_EQ(false,
            ExpirationAction::isLocalDestroy(ExpirationAction::INVALID_ACTION))
      << "INVALID_ACTION is not local destroy";
}

TEST(ExpirationActionTest, ValidateIsLocal) {
  EXPECT_EQ(false, ExpirationAction::isLocal(ExpirationAction::INVALIDATE))
      << "INVALIDATE is not local";
  EXPECT_EQ(true, ExpirationAction::isLocal(ExpirationAction::LOCAL_INVALIDATE))
      << "LOCAL_INVALIDATE is local";
  EXPECT_EQ(false, ExpirationAction::isLocal(ExpirationAction::DESTROY))
      << "DESTROY is not local";
  EXPECT_EQ(true, ExpirationAction::isLocal(ExpirationAction::LOCAL_DESTROY))
      << "LOCAL_DESTROY is local";
  EXPECT_EQ(false, ExpirationAction::isLocal(ExpirationAction::INVALID_ACTION))
      << "INVALID_ACTION is not local";
}

TEST(ExpirationActionTest, ValidateIsDistributed) {
  EXPECT_EQ(true, ExpirationAction::isDistributed(ExpirationAction::INVALIDATE))
      << "INVALIDATE is distributed";
  EXPECT_EQ(false,
            ExpirationAction::isDistributed(ExpirationAction::LOCAL_INVALIDATE))
      << "LOCAL_INVALIDATE is not distributed";
  EXPECT_EQ(true, ExpirationAction::isDistributed(ExpirationAction::DESTROY))
      << "DESTROY is distributed";
  EXPECT_EQ(false,
            ExpirationAction::isDistributed(ExpirationAction::LOCAL_DESTROY))
      << "LOCAL_DESTROY is not distributed";
  EXPECT_EQ(false,
            ExpirationAction::isDistributed(ExpirationAction::INVALID_ACTION))
      << "INVALID_ACTION is not distributed";
}
