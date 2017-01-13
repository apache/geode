/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/UserFunctionExecutionException.hpp>

namespace gemfire {

void UserFunctionExecutionException::toData(DataOutput& output) const {
  throw IllegalStateException(
      "UserFunctionExecutionException::toData is not intended for use.");
}

Serializable* UserFunctionExecutionException::fromData(DataInput& input) {
  throw IllegalStateException(
      "UserFunctionExecutionException::fromData is not intended for use.");
  return NULL;
}

int32_t UserFunctionExecutionException::classId() const {
  throw IllegalStateException(
      "UserFunctionExecutionException::classId is not intended for use.");
  return 0;
}

uint32_t UserFunctionExecutionException::objectSize() const {
  throw IllegalStateException(
      "UserFunctionExecutionException::objectSize is not intended for use.");
  return 0;
}

int8_t UserFunctionExecutionException::typeId() const {
  return static_cast<int8_t>(0);
}

UserFunctionExecutionException::UserFunctionExecutionException(
    CacheableStringPtr msg)
    : m_message(msg) {}
}  // namespace gemfire
