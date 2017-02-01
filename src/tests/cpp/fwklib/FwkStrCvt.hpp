#pragma once

#ifndef APACHE_GEODE_GUARD_596076185d7a905f2040a9113cab41f2
#define APACHE_GEODE_GUARD_596076185d7a905f2040a9113cab41f2

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
  * @file    FwkStrCvt.hpp
  * @since   1.0
  * @version 1.0
  * @see
  *
  */

// ----------------------------------------------------------------------------


#include "config.h"
#include <gfcpp/gfcpp_globals.hpp>

#if defined(_MACOSX)
#include <inttypes.h>
#endif
#include <string>
#include "ace/OS.h"

// ----------------------------------------------------------------------------

namespace apache {
namespace geode {
namespace client {
namespace testframework {

// ----------------------------------------------------------------------------

#if defined(WIN32)
#define Int64_FMT "%I64d"
#define UInt64_FMT "%I64u"
#elif defined(_LINUX)
#define Int64_FMT "%jd"
#define UInt64_FMT "%ju"
#elif defined(_SOLARIS)
#define Int64_FMT "%lld"
#define UInt64_FMT "%llu"
#elif defined(_MACOSX)
#define Int64_FMT "%" PRId64
#define UInt64_FMT "%" PRIu64
#else
#error not implemented
#endif

// ----------------------------------------------------------------------------

/**
  * @class FwkStrCvt
  * @brief basic string converting class
  */
class FwkStrCvt {
 public:
  static int64_t hton64(int64_t value);
  static int64_t ntoh64(int64_t value);
  static int32_t hton32(int32_t value);
  static int32_t ntoh32(int32_t value);
  static int16_t hton16(int16_t value);
  static int16_t ntoh16(int16_t value);
  static char* hexify(uint8_t* buff, int32_t len);

  /** @brief convert from string value */
  FwkStrCvt(const std::string& text) { m_sText = text; };
  /** @brief convert from string value */
  FwkStrCvt(const char* pszText) {
    if (pszText) m_sText = pszText;
  };
  /** @brief convert from double value */
  FwkStrCvt(const double dValue) {
    char szText[50];
    if (ACE_OS::snprintf(szText, sizeof(szText) - 1, "%lf", dValue)) {
      m_sText = szText;
    } else {
      m_sText.clear();
    }
  };
  /** @brief convert from float value */
  FwkStrCvt(const float fValue) {
    char szText[50];
    if (ACE_OS::snprintf(szText, sizeof(szText) - 1, "%f", fValue)) {
      m_sText = szText;
    } else {
      m_sText.clear();
    }
  };
  /** @brief convert from uint32_t value */
  FwkStrCvt(const uint32_t uiValue) {
    char szText[50];
    if (ACE_OS::snprintf(szText, sizeof(szText) - 1, "%u", uiValue)) {
      m_sText = szText;
    } else {
      m_sText.clear();
    }
  };
  /** @brief convert from int32_t value */
  FwkStrCvt(const int32_t iValue) {
    char szText[50];
    if (ACE_OS::snprintf(szText, sizeof(szText) - 1, "%d", iValue)) {
      m_sText = szText;
    } else {
      m_sText.clear();
    }
  };
  /** @brief convert from bool value */
  FwkStrCvt(const bool bValue) { m_sText = (bValue) ? "true" : "false"; };

  /** @brief convert from uint64_t value */
  FwkStrCvt(const uint64_t uiValue) {
    char szText[100];
    if (ACE_OS::snprintf(szText, sizeof(szText) - 1, UInt64_FMT, uiValue)) {
      m_sText = szText;
    } else {
      m_sText.clear();
    }
  }

  /** @brief convert from int64_t value */
  FwkStrCvt(const int64_t iValue) {
    char szText[100];
    if (ACE_OS::snprintf(szText, sizeof(szText) - 1, Int64_FMT, iValue)) {
      m_sText = szText;
    } else {
      m_sText.clear();
    }
  }

  /** @brief gets double value */
  double toDouble() { return ACE_OS::strtod(m_sText.c_str(), 0); };

  /** @brief gets float value */
  float toFloat() {
    return static_cast<float>(ACE_OS::strtod(m_sText.c_str(), 0));
  };

  /** @brief gets int32_t value */
  int32_t toInt() { return ACE_OS::atoi(m_sText.c_str()); };

  /** @brief gets uint32_t value */
  uint32_t toUint() {
    return static_cast<uint32_t>(ACE_OS::atoi(m_sText.c_str()));
  };

  /** @brief gets int32_t value */
  // TODO: : why this returns in32_t? if so why different from toInt()
  int32_t toLong() { return toInt(); };

  /** @brief gets uint32_t value */
  // TODO: : why this returns uin32_t? if so why different from toUInt()
  uint32_t toUlong() { return toUint(); };

  /** @brief gets int32_t value */
  int32_t toInt32() { return toInt(); };

  /** @brief gets uint32_t value */
  uint32_t toUInt32() { return toUint(); };

  /** @brief gets uint64_t value */
  uint64_t toUInt64() {
    uint64_t uiValue = 0;
    sscanf(m_sText.c_str(), UInt64_FMT, &uiValue);
    return uiValue;
  };

  /** @brief gets int64_t value */
  int64_t toInt64() {
    int64_t iValue = 0;
    sscanf(m_sText.c_str(), Int64_FMT, &iValue);
    return iValue;
  };

  /** @brief gets bool value */
  bool toBool() {
    bool bBool = false;
    if (m_sText.size()) {
      if ((m_sText.at(0) == 't') || (m_sText.at(0) == 'T')) bBool = true;
    }
    return bBool;
  }

  /** @brief gets float value */
  static float toFloat(const char* value) {
    if (value == NULL) return 0.0;
    return static_cast<float>(ACE_OS::strtod(value, 0));
  };

  /** @brief gets float value */
  static float toFloat(const std::string& value) {
    if (value.empty()) {
      return -1.0;
    }
    return FwkStrCvt::toFloat(value.c_str());
  };

  /** @brief gets double value */
  static double toDouble(const char* value) {
    if (value == NULL) return 0.0;
    return ACE_OS::strtod(value, 0);
  };

  /** @brief gets double value */
  static double toDouble(const std::string& value) {
    if (value.empty()) {
      return -1.0;
    }
    return FwkStrCvt::toDouble(value.c_str());
  };

  /** @brief gets int32_t value */
  static int32_t toInt32(const char* value) {
    if (value == NULL) return 0;
    return ACE_OS::atoi(value);
  };

  /** @brief gets int32_t value */
  static int32_t toInt32(const std::string& value) {
    if (value.empty()) {
      return -1;
    }
    return ACE_OS::atoi(value.c_str());
  };

  /** @brief gets uint32_t value */
  static uint32_t toUInt32(const char* value) {
    if (value == NULL) return 0;
    return static_cast<uint32_t>(ACE_OS::atoi(value));
  };

  /** @brief gets uint32_t value */
  static uint32_t toUInt32(const std::string& value) {
    if (value.empty()) {
      return 0;
    }
    return static_cast<uint32_t>(ACE_OS::atoi(value.c_str()));
  };

  /** @brief gets uint64_t value */
  static uint64_t toUInt64(const std::string& value) {
    if (value.empty()) {
      return 0;
    }
    return toUInt64(value.c_str());
  };

  /** @brief gets uint64_t value */
  static uint64_t toUInt64(const char* value) {
    uint64_t uiValue = 0;
    if (value == NULL) return uiValue;
    sscanf(value, UInt64_FMT, &uiValue);
    return uiValue;
  };

  /** @brief gets int64_t value */
  static int64_t toInt64(const std::string& value) {
    if (value.empty()) {
      return 0;
    }
    return toInt64(value.c_str());
  };

  /** @brief gets int64_t value */
  static int64_t toInt64(const char* value) {
    int64_t iValue = 0;
    if (value == NULL) return iValue;
    sscanf(value, Int64_FMT, &iValue);
    return iValue;
  };

  /** @brief from int64_t value */
  static std::string toString(int64_t value) {
    char text[100];
    ACE_OS::snprintf(text, 99, Int64_FMT, value);
    return text;
  }

  /** @brief from uint64_t value */
  static std::string toString(uint64_t value) {
    char text[100];
    ACE_OS::snprintf(text, 99, UInt64_FMT, value);
    return text;
  }

  /** @brief gets bool value */
  static bool toBool(const std::string& value) {
    if (value.empty()) {
      return false;
    }
    return FwkStrCvt::toBool(value.c_str());
  }

  /** @brief gets bool value */
  static bool toBool(const char* value) {
    if ((value != NULL) && ((*value == 't') || (*value == 'T'))) return true;
    return false;
  }

  static int32_t toSeconds(const std::string& str) {
    if (str.empty()) {
      return -1;
    }
    return toSeconds(str.c_str());
  }

  static std::string toTimeString(int32_t seconds);

  // This expects a string of the form: 12h23m43s
  // and returns the time encoded in the string as
  // the integer number of seconds it represents.
  static int32_t toSeconds(const char* str);

  /** @brief gets string value */
  std::string& toString() { return m_sText; };

  /** @brief gets string value */
  const std::string& asString() { return m_sText; };

  /** @brief gets char * value */
  const char* asChar() { return m_sText.c_str(); };

 private:
  static int32_t asSeconds(int32_t val, char typ);

  std::string m_sText;
};

// ----------------------------------------------------------------------------

}  // namespace  testframework
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // APACHE_GEODE_GUARD_596076185d7a905f2040a9113cab41f2
