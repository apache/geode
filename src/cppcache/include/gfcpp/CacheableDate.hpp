#ifndef _GEMFIRE_CACHEABLEDATE_HPP_
#define _GEMFIRE_CACHEABLEDATE_HPP_
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CacheableKey.hpp"
#include "CacheableString.hpp"
#include "GemfireTypeIds.hpp"
#include "ExceptionTypes.hpp"
#include <time.h>
#ifdef _WIN32
#include <WinSock2.h>  //for struct timeval
#define GF_LOCALTIME(X, Y) localtime_s(Y, X)
#else
#include <sys/time.h>
#if defined(_LINUX) || defined(_SOLARIS) || defined(_MACOSX)
#define GF_LOCALTIME(X, Y) localtime_r(X, Y)
#endif
#endif

/** @file
*/
namespace gemfire {

/**
 * Implement a date object based on system epoch that can serve as a
 * distributable key object for caching as well as being a date value.
 */
class CPPCACHE_EXPORT CacheableDate : public CacheableKey {
 private:
  struct timeval m_timevalue;
  uint32_t m_hash;

 public:
  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const;

  /**
   *@brief deserialize this object
   **/
  virtual Serializable* fromData(DataInput& input);

  /**
   * @brief creation function for dates.
   */
  static Serializable* createDeserializable();

  /**
   *@brief Return the classId of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and deserialize into.
   */
  virtual int32_t classId() const;

  /**
   *@brief return the typeId byte of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and deserialize into.
   */
  virtual int8_t typeId() const;

  /** @return the size of the object in bytes */
  virtual uint32_t objectSize() const { return sizeof(CacheableDate); }

  /** @return true if this key matches other. */
  virtual bool operator==(const CacheableKey& other) const;

  /** @return day of the month. */
  virtual int day() const;

  /** @return month 1(Jan) - 12(Dec) . */
  virtual int month() const;

  /** @return year, example 1999. */
  virtual int year() const;

  /** @return milliseconds elapsed as per epoch time. */
  virtual int64_t milliseconds() const;

  /** @return the hashcode for this key. */
  virtual uint32_t hashcode() const;

  /**
   * Factory method for creating an instance of CacheableDate
   */
  static CacheableDatePtr create() {
    return CacheableDatePtr(new CacheableDate());
  }

  static CacheableDatePtr create(const time_t& value) {
    return CacheableDatePtr(new CacheableDate(value));
  }

  static CacheableDatePtr create(const timeval& value) {
    return CacheableDatePtr(new CacheableDate(value));
  }

  virtual CacheableStringPtr toString() const;

  /** Destructor */
  virtual ~CacheableDate();

  /** used to render as a string for logging. */
  virtual int32_t logString(char* buffer, int32_t maxLength) const;

 protected:
  /** Constructor, given a timeval value. */
  CacheableDate(const timeval& value);

  /** Constructor, used for deserialization. */
  CacheableDate(const time_t value = 0);

 private:
  // never implemented.
  void operator=(const CacheableDate& other);
  CacheableDate(const CacheableDate& other);
};

inline CacheableKeyPtr createKey(const timeval& value) {
  return CacheableKeyPtr(CacheableDate::create(value));
}

inline CacheablePtr createValue(const timeval& value) {
  return CacheablePtr(CacheableDate::create(value));
}
}

#endif
