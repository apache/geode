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

#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace apache {
namespace geode {
namespace client {
class ByteArray {
 public:
  static ByteArray fromString(const std::string &str);

  static ByteArray fromString(const wchar_t *wstr);

  static std::string toString(const ByteArray &bytes);

  ByteArray();

  ByteArray(const ByteArray &other);

  ByteArray(const uint8_t *bytes, const std::size_t size);

  virtual ~ByteArray();

  ByteArray &operator=(const ByteArray &other);

  operator const uint8_t *() const { return m_bytes.get(); }

  operator uint8_t *() { return m_bytes.get(); }

  const uint8_t *get() const { return m_bytes.get(); }

  std::size_t size() const { return m_size; }

  std::string toString() const;

 private:
  std::shared_ptr<uint8_t> m_bytes;
  std::size_t m_size;
};
}  // namespace client
}  // namespace geode
}  // namespace apache
