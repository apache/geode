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
#include <gfcpp/GemfireTypeIds.hpp>

#include <cwchar>
#include <ace/OS.h>

namespace apache {
namespace geode {
namespace client {

void CacheableDate::toData(DataOutput& output) const {
  int64_t msec = static_cast<int64_t>(m_timevalue.tv_sec);
  msec *= 1000;
  msec += (m_timevalue.tv_usec / 1000);
  output.writeInt(msec);
}

Serializable* CacheableDate::fromData(DataInput& input) {
  m_hash = 0;
  int64_t msec;
  input.readInt(&msec);

  // GF_D_ASSERT( sizeof(time_t)!=4 || (msec/1000)==(time_t)(msec/1000) );

  m_timevalue.tv_sec = static_cast<long>(msec / 1000);
  m_timevalue.tv_usec = (static_cast<suseconds_t>(msec % 1000) * 1000);

  return this;
}

Serializable* CacheableDate::createDeserializable() {
  return new CacheableDate();
}

int32_t CacheableDate::classId() const { return 0; }

int8_t CacheableDate::typeId() const { return GemfireTypeIds::CacheableDate; }

bool CacheableDate::operator==(const CacheableKey& other) const {
  if (other.typeId() != GemfireTypeIds::CacheableDate) {
    return false;
  }

  const CacheableDate& otherDt = static_cast<const CacheableDate&>(other);

  return ((m_timevalue.tv_sec == otherDt.m_timevalue.tv_sec) &&
          (m_timevalue.tv_usec == otherDt.m_timevalue.tv_usec));
}
int CacheableDate::day() const {
  struct tm date = {0};
  time_t sec = m_timevalue.tv_sec;
  GF_LOCALTIME(&sec, &date);
  return date.tm_mday;
}

int CacheableDate::month() const {
  struct tm date = {0};
  time_t sec = m_timevalue.tv_sec;
  GF_LOCALTIME(&sec, &date);
  return date.tm_mon + 1;
}

int CacheableDate::year() const {
  struct tm date = {0};
  time_t sec = m_timevalue.tv_sec;
  GF_LOCALTIME(&sec, &date);
  return date.tm_year + 1900;
}

int64_t CacheableDate::milliseconds() const {
  int64_t tmp = 0;
  tmp = static_cast<int64_t>(m_timevalue.tv_sec);
  tmp *= 1000;
  tmp += (m_timevalue.tv_usec / 1000);
  return tmp;
}

uint32_t CacheableDate::hashcode() const {
  if (m_hash == 0) {
    CacheableDate* self = const_cast<CacheableDate*>(this);
    int64_t hash = self->milliseconds();
    self->m_hash = static_cast<int>(hash) ^ static_cast<int>(hash >> 32);
  }
  return m_hash;
}

CacheableDate::CacheableDate(const timeval& value) : m_hash(0) {
  m_timevalue = value;
}

CacheableDate::CacheableDate(const time_t value) : m_hash(0) {
  m_timevalue.tv_sec = static_cast<long>(value);
  m_timevalue.tv_usec = 0;
}

CacheableDate::~CacheableDate() {
  m_hash = 0;
  m_timevalue.tv_sec = 0;
  m_timevalue.tv_usec = 0;
}

CacheableStringPtr CacheableDate::toString() const {
  char buffer[25];
  struct tm date = {0};
  time_t sec = m_timevalue.tv_sec;
  GF_LOCALTIME(&sec, &date);
  ACE_OS::snprintf(buffer, 24, "%d/%d/%d %d:%d:%d", date.tm_mon + 1,
                   date.tm_mday, date.tm_year + 1900, date.tm_hour, date.tm_min,
                   date.tm_sec);
  return CacheableString::create(buffer);
}

int32_t CacheableDate::logString(char* buffer, int32_t maxLength) const {
  struct tm date = {0};
  time_t sec = m_timevalue.tv_sec;
  GF_LOCALTIME(&sec, &date);
  return ACE_OS::snprintf(buffer, maxLength,
                          "CacheableDate (mm/dd/yyyy) ( %d/%d/%d )",
                          date.tm_mon + 1, date.tm_mday, date.tm_year + 1900);
}
}  // namespace client
}  // namespace geode
}  // namespace apache
