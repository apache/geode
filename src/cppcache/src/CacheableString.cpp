/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/GemfireTypeIds.hpp>
#include <Utils.hpp>

#include <cwchar>
#include <cstdlib>
#include <ace/ACE.h>
#include <ace/OS.h>

using namespace gemfire;

void CacheableString::toData(DataOutput& output) const {
  if (m_type == GF_STRING) {
    output.writeASCII(reinterpret_cast<const char*>(m_str), m_len);
  } else if (m_type == GF_WIDESTRING) {
    output.writeUTF(reinterpret_cast<const wchar_t*>(m_str), m_len);
  } else if (m_type == GF_STRING_HUGE) {
    output.writeASCIIHuge(reinterpret_cast<const char*>(m_str), m_len);
  } else if (m_type == GF_WIDESTRING_HUGE) {
    output.writeUTFHuge(reinterpret_cast<const wchar_t*>(m_str), m_len);
  }
}

Serializable* CacheableString::fromData(DataInput& input) {
  uint16_t shortLen = 0;
  if (m_type == GF_STRING) {
    input.readASCII(reinterpret_cast<char**>(&m_str), &shortLen);
    m_len = shortLen;
  } else if (m_type == GF_WIDESTRING) {
    input.readUTF(reinterpret_cast<wchar_t**>(&m_str), &shortLen);
    m_len = shortLen;
  } else if (m_type == GF_STRING_HUGE) {
    input.readASCIIHuge(reinterpret_cast<char**>(&m_str), &m_len);
  } else if (m_type == GF_WIDESTRING_HUGE) {
    input.readUTFHuge(reinterpret_cast<wchar_t**>(&m_str), &m_len);
  }
  return this;
}

Serializable* CacheableString::createDeserializable() {
  return new CacheableString(GF_STRING);
}

Serializable* CacheableString::createDeserializableHuge() {
  return new CacheableString(GF_STRING_HUGE);
}

Serializable* CacheableString::createUTFDeserializable() {
  return new CacheableString(GF_WIDESTRING);
}

Serializable* CacheableString::createUTFDeserializableHuge() {
  return new CacheableString(GF_WIDESTRING_HUGE);
}

int32_t CacheableString::classId() const { return 0; }

int8_t CacheableString::typeId() const { return m_type; }

// assumes that the lengths are equal
template <typename TChar1, typename TChar2>
bool compareStrings(const TChar1* str1, const TChar2* str2) {
  TChar1 wc;
  while ((wc = *str1) != 0) {
    if (wc != *str2) {
      return false;
    }
    ++str1, ++str2;
  }
  return true;
}

bool CacheableString::operator==(const CacheableKey& other) const {
  // use typeId() call instead of m_type to work correctly with derived
  // classes like CacheableFileName
  int8_t thisType = typeId();
  int8_t otherType = other.typeId();
  if (thisType != otherType) {
    if (!(thisType == GF_STRING || thisType == GF_WIDESTRING ||
          thisType == GF_STRING_HUGE || thisType == GF_WIDESTRING_HUGE) ||
        !(otherType == GF_STRING || otherType == GF_WIDESTRING ||
          otherType == GF_STRING_HUGE || otherType == GF_WIDESTRING_HUGE)) {
      return false;
    }
  }

  const CacheableString& otherStr = static_cast<const CacheableString&>(other);
  // check for lengths first
  if (m_len != otherStr.m_len) {
    return false;
  }
  if (m_str == NULL) {
    return true;
  } else if (thisType == GF_STRING || thisType == GF_STRING_HUGE) {
    if (otherType == GF_STRING || otherType == GF_STRING_HUGE) {
      return compareStrings(reinterpret_cast<const char*>(m_str),
                            reinterpret_cast<const char*>(otherStr.m_str));
    } else {
      return compareStrings(reinterpret_cast<const char*>(m_str),
                            reinterpret_cast<const wchar_t*>(otherStr.m_str));
    }
  } else {
    if (otherType == GF_STRING || otherType == GF_STRING_HUGE) {
      return compareStrings(reinterpret_cast<const wchar_t*>(m_str),
                            reinterpret_cast<const char*>(otherStr.m_str));
    } else {
      return compareStrings(reinterpret_cast<const wchar_t*>(m_str),
                            reinterpret_cast<const wchar_t*>(otherStr.m_str));
    }
  }
}

uint32_t CacheableString::hashcode() const {
  if (m_str == NULL) {
    return 0;
  }
  int localHash = 0;

  if (m_hashcode == 0) {
    uint32_t prime = 31;
    if (isCString()) {
      const char* data = reinterpret_cast<const char*>(m_str);
      for (uint32_t i = 0; i < m_len; i++) {
        localHash = prime * localHash + data[i];
      }
    } else {
      GF_DEV_ASSERT(isWideString());
      const wchar_t* data = reinterpret_cast<const wchar_t*>(m_str);
      for (uint32_t i = 0; i < m_len; i++) {
        localHash = prime * localHash + data[i];
      }
    }
    m_hashcode = localHash;
  }
  return m_hashcode;
}

/** Private method to get ASCII char for wide-char if possible */
inline char getASCIIChar(const wchar_t wc, bool& isASCII, int32_t& encodedLen) {
  if (wc & 0xfc00) {
    // three bytes required.
    encodedLen += 3;
    isASCII = false;
  } else if (wc & 0x0380) {
    // two byte.
    encodedLen += 2;
    isASCII = false;
  } else {
    // one byte.
    ++encodedLen;
    return (wc & 0x7f);
  }
  return 0;
}

char* CacheableString::getASCIIString(const wchar_t* value, int32_t& len,
                                      int32_t& encodedLen) {
  wchar_t currentChar;
  char* buf = NULL;
  bool isASCII = true;
  char c;
  if (len > 0) {
    int32_t clen = len;
    buf = new char[clen + 1];
    while (clen > 0 && (currentChar = *value) != 0) {
      c = getASCIIChar(currentChar, isASCII, encodedLen);
      if (isASCII) {
        buf[encodedLen - 1] = c;
      }
      ++value, --clen;
    }
    if (isASCII) {
      buf[encodedLen] = '\0';
    } else {
      delete[] buf;
      buf = NULL;
    }
    len -= clen;
  } else {
    DataOutput out;
    const wchar_t* pvalue = value;
    while ((currentChar = *pvalue) != 0) {
      c = getASCIIChar(currentChar, isASCII, encodedLen);
      if (isASCII) {
        out.write(static_cast<int8_t>(c));
      }
      ++pvalue;
    }
    len = static_cast<int32_t>(pvalue - value);
    if (isASCII) {
      uint32_t sz;
      const uint8_t* outBuf = out.getBuffer(&sz);
      buf = new char[sz + 1];
      memcpy(buf, outBuf, sz);
      buf[sz] = '\0';
    }
  }
  return buf;
}

void CacheableString::copyString(const char* value, int32_t len) {
  GF_NEW(m_str, char[len + 1]);
  memcpy(m_str, value, len * sizeof(char));
  (reinterpret_cast<char*>(m_str))[len] = '\0';
}

void CacheableString::copyString(const wchar_t* value, int32_t len) {
  GF_NEW(m_str, wchar_t[len + 1]);
  memcpy(m_str, value, len * sizeof(wchar_t));
  (reinterpret_cast<wchar_t*>(m_str))[len] = L'\0';
}

void CacheableString::initString(const char* value, int32_t len) {
  if (value != NULL) {
    if (len <= 0) {
      len = static_cast<int32_t>(strlen(value));
    }
    if (len > 0xFFFF) {
      m_type = GF_STRING_HUGE;
    }
    m_len = len;
    copyString(value, len);
  }
}

void CacheableString::initStringNoCopy(char* value, int32_t len) {
  if (value != NULL) {
    if (len <= 0) {
      len = static_cast<int32_t>(strlen(value));
    }
    if (len > 0xFFFF) {
      m_type = GF_STRING_HUGE;
    }
    m_len = len;
    m_str = value;
  }
}

void CacheableString::initString(const wchar_t* value, int32_t len) {
  if (value == NULL) {
    m_type = GF_STRING;
  } else {
    int32_t encodedLen = 0;
    // The call to getASCIIString() is just for the side-effect
    // of calculating encodedLen so delete any buffer that may
    // have been allocated.
    delete[] getASCIIString(value, len, encodedLen);
    copyString(value, len);
    if (encodedLen > 0xFFFF) {
      m_type = GF_WIDESTRING_HUGE;
    } else {
      m_type = GF_WIDESTRING;
    }
    m_len = len;
  }
}

void CacheableString::initStringNoCopy(wchar_t* value, int32_t len) {
  if (value == NULL) {
    m_type = GF_STRING;
  } else {
    int32_t encodedLen = DataOutput::getEncodedLength(value, len, &m_len);
    if (encodedLen > 0xFFFF) {
      m_type = GF_WIDESTRING_HUGE;
    } else {
      m_type = GF_WIDESTRING;
    }
    m_str = value;
  }
}

CacheableString::~CacheableString() {
  if (isCString()) {
    delete[] reinterpret_cast<char*>(m_str);
  } else {
    GF_DEV_ASSERT(isWideString());
    delete[] reinterpret_cast<wchar_t*>(m_str);
  }
}

int32_t CacheableString::logString(char* buffer, int32_t maxLength) const {
  if (isCString()) {
    return ACE_OS::snprintf(
        buffer, maxLength, "%s( %s )", className(),
        (m_str != NULL ? reinterpret_cast<const char*>(m_str) : "null"));
  } else {
    GF_DEV_ASSERT(isWideString());
    int32_t numChars = ACE_OS::snprintf(buffer, maxLength, "%s( ", className());
    if (numChars >= (int)maxLength || numChars <= 0) {
      return numChars;
    }
    const char* bufStart = buffer;
    buffer += numChars;
    maxLength -= numChars;
    numChars = Utils::logWideString(buffer, maxLength,
                                    reinterpret_cast<const wchar_t*>(m_str));
    if (numChars >= (int)maxLength || numChars <= 0) {
      return numChars + static_cast<int32_t>(buffer - bufStart);
    }
    buffer += numChars;
    maxLength -= numChars;
    numChars = ACE_OS::snprintf(buffer, maxLength, " )");
    return numChars + static_cast<int32_t>(buffer - bufStart);
  }
}

uint32_t CacheableString::objectSize() const {
  uint32_t size = sizeof(CacheableString);
  if (isCString()) {
    size += (sizeof(char) * m_len);
  } else {
    GF_DEV_ASSERT(isWideString());
    size += (sizeof(wchar_t) * m_len);
  }
  return size;
}
