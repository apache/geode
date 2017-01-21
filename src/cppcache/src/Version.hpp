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
#ifndef __GEMFIRE_VERSION_HPP__
#define __GEMFIRE_VERSION_HPP__
#include "CacheImpl.hpp"

namespace apache {
namespace geode {
namespace client {

class Version {
 public:
  // getter for ordinal
  static int8_t getOrdinal() { return Version::m_ordinal; }

  friend void apache::geode::client::CacheImpl::setVersionOrdinalForTest(
      int8_t newOrdinal);
  friend int8_t apache::geode::client::CacheImpl::getVersionOrdinalForTest();

 private:
  static int8_t m_ordinal;

  Version(){};
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif  //__GEMFIRE_VERSION_HPP__s
