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

#include "config.h"
#include <gfcpp/CacheableDate.hpp>
#include <gfcpp/CacheableKeys.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/GeodeTypeIds.hpp>

#include <cwchar>
#include <ace/OS.h>

#include <chrono>
#include <ctime>

namespace apache {
namespace geode {
namespace client {

static CacheableDate::time_point posixEpoch =
    CacheableDate::clock::from_time_t(0);

void CacheableDate::toData(DataOutput& output) const {
  output.writeInt(m_timevalue);
}

Serializable* CacheableDate::fromData(DataInput& input) {
  input.readInt(&m_timevalue);
  return this;
}

Serializable* CacheableDate::createDeserializable() {
  return new CacheableDate();
}

int32_t CacheableDate::classId() const { return 0; }

int8_t CacheableDate::typeId() const { return GeodeTypeIds::CacheableDate; }

bool CacheableDate::operator==(const CacheableKey& other) const {
  if (other.typeId() != GeodeTypeIds::CacheableDate) {
    return false;
  }

  const CacheableDate& otherDt = static_cast<const CacheableDate&>(other);

  return m_timevalue == otherDt.m_timevalue;
}

int CacheableDate::day() const {
  struct tm date = {0};
  time_t sec = m_timevalue / 1000;
  ACE_OS::localtime_r(&sec, &date);
  return date.tm_mday;
}

int CacheableDate::month() const {
  struct tm date = {0};
  time_t sec = m_timevalue / 1000;
  ACE_OS::localtime_r(&sec, &date);
  return date.tm_mon + 1;
}

int CacheableDate::year() const {
  struct tm date = {0};
  time_t sec = m_timevalue / 1000;
  ACE_OS::localtime_r(&sec, &date);
  return date.tm_year + 1900;
}

int64_t CacheableDate::milliseconds() const { return m_timevalue; }

uint32_t CacheableDate::hashcode() const {
  return static_cast<int>(m_timevalue) ^ static_cast<int>(m_timevalue >> 32);
}

CacheableDate::CacheableDate(const time_t value) {
  m_timevalue = (static_cast<int64_t>(value)) * 1000;
}

CacheableDate::CacheableDate(const CacheableDate::time_point& value) {
  // Set based on time since local system clock epoch plus time since POSIX
  // epoch since local system clock epoch to get milliseconds since POSIX epoch.
  m_timevalue =
      std::chrono::duration_cast<CacheableDate::duration>(value - posixEpoch)
          .count();
}

CacheableDate::CacheableDate(const CacheableDate::duration& value) {
  m_timevalue = value.count();
}

CacheableDate::~CacheableDate() {}

CacheableStringPtr CacheableDate::toString() const {
  char buffer[25];
  struct tm date = {0};
  time_t sec = m_timevalue / 1000;
  ACE_OS::localtime_r(&sec, &date);
  ACE_OS::snprintf(buffer, 24, "%d/%d/%d %d:%d:%d", date.tm_mon + 1,
                   date.tm_mday, date.tm_year + 1900, date.tm_hour, date.tm_min,
                   date.tm_sec);
  return CacheableString::create(buffer);
}

int32_t CacheableDate::logString(char* buffer, int32_t maxLength) const {
  struct tm date = {0};
  time_t sec = m_timevalue / 1000;
  ACE_OS::localtime_r(&sec, &date);
  return ACE_OS::snprintf(buffer, maxLength,
                          "CacheableDate (mm/dd/yyyy) ( %d/%d/%d )",
                          date.tm_mon + 1, date.tm_mday, date.tm_year + 1900);
}

}  // namespace client
}  // namespace geode
}  // namespace apache
