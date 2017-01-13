#ifndef __GEMFIRE_IMPL_UTILS_H__
#define __GEMFIRE_IMPL_UTILS_H__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *
 * The specification of function behaviors is found in the corresponding
 * .cpp file.
 *
 *========================================================================
 */

/**
 * @file
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_base.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/DataOutput.hpp>
#include "NanoTimer.hpp"
#include <gfcpp/statistics/Statistics.hpp>
#include <gfcpp/SystemProperties.hpp>
#include <gfcpp/DistributedSystem.hpp>
#include <typeinfo>
#include <string>
#include <unordered_set>
#ifdef __GNUC__
extern "C" {
#include <cxxabi.h>
}
#endif

namespace gemfire {
class CPPCACHE_EXPORT Utils {
  /**
   * utilities
   *
   */
 public:
#ifdef _WIN32
  static pNew s_pNew;
  static pDelete s_pDelete;
  static bool s_setNewAndDelete;
#endif

  /**
   * Get the value of an environment variable.
   * On windows the maximum length of value supported is 8191.
   */
  static std::string getEnv(const char* varName);
  static int32_t getLastError();

#ifdef __GNUC__
  inline static char* _gnuDemangledName(const char* typeIdName, size_t& len) {
    int status;
    char* demangledName = abi::__cxa_demangle(typeIdName, NULL, &len, &status);
    if (status == 0 && demangledName != NULL) {
      return demangledName;
    }
    return NULL;
  }
#endif

  inline static void demangleTypeName(const char* typeIdName,
                                      std::string& str) {
#ifdef __GNUC__
    size_t len;
    char* demangledName = _gnuDemangledName(typeIdName, len);
    if (demangledName != NULL) {
      str.append(demangledName, len);
      free(demangledName);
      return;
    }
#endif
    str.append(typeIdName);
  }

  inline static CacheableStringPtr demangleTypeName(const char* typeIdName) {
#ifdef __GNUC__
    size_t len;
    char* demangledName = _gnuDemangledName(typeIdName, len);
    if (demangledName != NULL) {
      return CacheableString::createNoCopy(demangledName, len);
    }
#endif
    return CacheableString::create(typeIdName);
  }

  static int logWideString(char* buf, size_t maxLen, const wchar_t* wStr);

  /**
   * The only operations that is well defined on the result is "asChar".
   */
  inline static CacheableStringPtr getCacheableKeyString(
      const CacheableKeyPtr& key) {
    CacheableStringPtr result;
    if (key != NULLPTR) {
      char* buf;
      GF_NEW(buf, char[_GF_MSG_LIMIT + 1]);
      key->logString(buf, _GF_MSG_LIMIT);
      // the length given here is not correct but we want to save
      // the cost of a "strlen" and the value here does not matter
      // assuming the caller will use the result only for logging by
      // invoking "->asChar()"
      result = CacheableString::createNoCopy(buf, _GF_MSG_LIMIT);
    } else {
      result = CacheableString::create("(null)");
    }
    return result;
  }

  static CacheableStringPtr getCacheableString(const CacheablePtr& val) {
    if (val != NULLPTR) {
      if (instanceOf<CacheableKeyPtr>(val)) {
        const CacheableKeyPtr& key = staticCast<CacheableKeyPtr>(val);
        return getCacheableKeyString(key);
      } else {
        std::string str;
        const CacheableStringPtr& cStr = val->toString();
        if (cStr != NULLPTR) {
          if (cStr->isCString()) {
            return cStr;
          } else {
            char buf[_GF_MSG_LIMIT + 1];
            (void)logWideString(buf, _GF_MSG_LIMIT, cStr->asWChar());
            return CacheableString::create(buf);
          }
        } else {
          return CacheableString::create("(null)");
        }
      }
    } else {
      return CacheableString::create("(null)");
    }
  }

  inline static int64 startStatOpTime() {
    if (DistributedSystem::getSystemProperties() != NULL)
      return (DistributedSystem::getSystemProperties()
                  ->getEnableTimeStatistics())
                 ? NanoTimer::now()
                 : 0;
    else
      return 0;
  }

  // Check objectSize() implementation return value and log a warning at most
  // once.
  inline static uint32_t checkAndGetObjectSize(const CacheablePtr& theObject) {
    uint32_t objectSize = theObject->objectSize();
    static bool youHaveBeenWarned = false;
    if ((objectSize == 0 || objectSize == ((uint32_t)-1)) &&
        !youHaveBeenWarned) {
      LOGWARN(
          "Object size for Heap LRU returned by class ID %d is 0 (zero) or -1 "
          "(UINT32_MAX). "
          "Even for empty objects the size returned should be at least one (1 "
          "byte) and "
          "should not be -1 or UINT32_MAX.",
          theObject->classId());
      youHaveBeenWarned = true;
      LOGFINE("Type ID is %d for the object returning zero HeapLRU size",
              theObject->typeId());
    }
    GF_DEV_ASSERT(objectSize != 0 && objectSize != ((uint32_t)-1));
    return objectSize;
  }

  inline static void updateStatOpTime(
      gemfire_statistics::Statistics* m_regionStats, int32 statId,
      int64 start) {
    if (DistributedSystem::getSystemProperties() != NULL) {
      if (DistributedSystem::getSystemProperties()->getEnableTimeStatistics()) {
        m_regionStats->incLong(statId, startStatOpTime() - start);
      }
    }
  }

  static void parseEndpointNamesString(
      const char* endpoints, std::unordered_set<std::string>& endpointNames);
  static void parseEndpointString(const char* endpoints, std::string& host,
                                  uint16_t& port);

  static std::string convertHostToCanonicalForm(const char* endpoints);

  static char* copyString(const char* str);

  /**
   * Convert the byte array to a string as "%d %d ...".
   * <code>maxLength</code> as zero implies no limit.
   */
  static CacheableStringPtr convertBytesToString(
      const uint8_t* bytes, int32_t length, size_t maxLength = _GF_MSG_LIMIT);

  /**
   * Convert the byte array to a string as "%d %d ...".
   * <code>maxLength</code> as zero implies no limit.
   */
  inline static CacheableStringPtr convertBytesToString(
      const char* bytes, int32_t length, size_t maxLength = _GF_MSG_LIMIT) {
    return convertBytesToString((const uint8_t*)bytes, length);
  }
};

// Generate random numbers 0 to max-1
class RandGen {
 public:
  int operator()(size_t max);
};

}  // namespace gemfire

#endif  // ifndef __GEMFIRE_IMPL_UTILS_H__
