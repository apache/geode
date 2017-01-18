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
#include <gfcpp/UserFunctionExecutionException.hpp>

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache
