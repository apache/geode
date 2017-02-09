#pragma once

#ifndef GEODE_GFCPP_CACHEABLEDATE_H_
#define GEODE_GFCPP_CACHEABLEDATE_H_

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

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "CacheableKey.hpp"
#include "CacheableString.hpp"
#include "GeodeTypeIds.hpp"
#include "ExceptionTypes.hpp"

#include <string>
#include <chrono>
#include <ctime>

/** @file
*/
namespace apache {
namespace geode {
namespace client {

/**
 * Implement a date object based on epoch of January 1, 1970 00:00:00 GMT that
 * can serve as a distributable key object for caching as well as being a date
 * value.
 */
class CPPCACHE_EXPORT CacheableDate : public CacheableKey {
 private:
  /**
   * Milliseconds since January 1, 1970, 00:00:00 GMT to be consistent with Java
   * Date.
   */
  int64_t m_timevalue;

 public:
  typedef std::chrono::system_clock clock;
  typedef std::chrono::time_point<clock> time_point;
  typedef std::chrono::milliseconds duration;

  /**
   * @brief serialize this object
   **/
  virtual void toData(DataOutput& output) const;

  /**
   * @brief deserialize this object
   **/
  virtual Serializable* fromData(DataInput& input);

  /**
   * @brief creation function for dates.
   */
  static Serializable* createDeserializable();

  /**
   * @brief Return the classId of the instance being serialized.
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

  /**
   * @return day of the month.
   * @deprecated Use localtime or similar for calendar conversions.
   */
  __DEPRECATED__("Use localtime or similar for calendar conversions.")
  virtual int day() const;

  /**
   * @return month 1(Jan) - 12(Dec) .
   * @deprecated Use localtime or similar for calendar conversions.
   */
  __DEPRECATED__("Use localtime or similar for calendar conversions.")
  virtual int month() const;

  /**
   * @return year, example 1999.
   * @deprecated Use localtime or similar for calendar conversions.
   */
  __DEPRECATED__("Use localtime or similar for calendar conversions.")
  virtual int year() const;

  /** @return milliseconds elapsed since January 1, 1970, 00:00:00 GMT. */
  virtual int64_t milliseconds() const;

  /**
   * Returns a hash code value for this object. The result is the exclusive OR
   * of the two halves of the primitive long value returned by the
   * milliseconds() method.
   *
   * @return the hashcode for this object. */
  virtual uint32_t hashcode() const;

  operator time_t() const { return m_timevalue / 1000; }
  operator time_point() const {
    return clock::from_time_t(0) + duration(m_timevalue);
  }
  operator duration() const { return duration(m_timevalue); }

  /**
   * Factory method for creating an instance of CacheableDate
   */
  static CacheableDatePtr create() {
    return CacheableDatePtr(new CacheableDate());
  }

  static CacheableDatePtr create(const time_t& value) {
    return CacheableDatePtr(new CacheableDate(value));
  }

  static CacheableDatePtr create(const time_point& value) {
    return CacheableDatePtr(new CacheableDate(value));
  }

  static CacheableDatePtr create(const duration& value) {
    return CacheableDatePtr(new CacheableDate(value));
  }

  virtual CacheableStringPtr toString() const;

  /** Destructor */
  virtual ~CacheableDate();

  /** used to render as a string for logging. */
  virtual int32_t logString(char* buffer, int32_t maxLength) const;

 protected:
  /** Constructor, used for deserialization. */
  CacheableDate(const time_t value = 0);

  /**
   * Construct from std::chrono::time_point<std::chrono::system_clock>.
   */
  CacheableDate(const time_point& value);

  /**
   * Construct from std::chrono::seconds since POSIX epoch.
   */
  CacheableDate(const duration& value);

 private:
  // never implemented.
  void operator=(const CacheableDate& other);
  CacheableDate(const CacheableDate& other);
};

inline CacheableKeyPtr createKey(const CacheableDate::time_point& value) {
  return CacheableKeyPtr(CacheableDate::create(value));
}

inline CacheablePtr createValue(const CacheableDate::time_point& value) {
  return CacheablePtr(CacheableDate::create(value));
}

}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CACHEABLEDATE_H_
