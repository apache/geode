#pragma once

#ifndef APACHE_GEODE_GUARD_43257b24105ffca3ff3bd16dc0cd17d2
#define APACHE_GEODE_GUARD_43257b24105ffca3ff3bd16dc0cd17d2

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

/**
  * @file    FwkException.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------


#include <string>

// ----------------------------------------------------------------------------

namespace apache {
namespace geode {
namespace client {
namespace testframework {
// ----------------------------------------------------------------------------

/** @class FwkException
  * @brief Framework exception handler
  */
class FwkException {
 public:
  /** @brief exception message to handle */
  inline FwkException(const std::string& sMessage) : m_sMessage(sMessage) {}

  /** @brief exception message to handle */
  inline FwkException(const char* pszMessage) : m_sMessage(pszMessage) {}

  /** @brief get message */
  inline const char* getMessage() const { return m_sMessage.c_str(); }

  /** @brief get message */
  inline const char* what() const { return m_sMessage.c_str(); }

 private:
  std::string m_sMessage;
};

// ----------------------------------------------------------------------------

}  // namespace  testframework
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_43257b24105ffca3ff3bd16dc0cd17d2
