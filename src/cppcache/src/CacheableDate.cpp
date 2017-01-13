/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

namespace gemfire {

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
}  // namespace gemfire
