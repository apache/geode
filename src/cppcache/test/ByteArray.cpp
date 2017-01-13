#include "ByteArray.hpp"

#include "config.h"

#ifdef _MACOSX
#include <stdlib.h>  // For wcstombs()
#endif
#include <wchar.h>

#include <cstring>

namespace {
std::string convertWstringToString(const wchar_t* wstr) {
  if (wstr) {
    const std::size_t len = ::wcslen(wstr);
    char* dst = new char[len + 1];
    ::wcstombs(dst, wstr, len);
    delete[] dst;
    return dst;
  }
  return std::string();
}
}  // namespace

namespace gemfire {
ByteArray ByteArray::fromString(const std::string& str) {
  ByteArray ba;
  if (!str.empty()) {
    ba.m_bytes = std::shared_ptr<uint8_t>(new uint8_t[(str.length() + 1) / 2],
                                          std::default_delete<uint8_t[]>());
    ba.m_size = 0U;
    bool shift = true;
    for (std::string::size_type pos = 0; pos < str.length(); ++pos) {
      unsigned int value = 0U;
      if ('0' <= str.at(pos) && str.at(pos) <= '9') {
        value = str.at(pos) - '0';
      } else if ('A' <= str.at(pos) && str.at(pos) <= 'F') {
        value = str.at(pos) - '7';
      } else if ('a' <= str.at(pos) && str.at(pos) <= 'f') {
        value = str.at(pos) - 'W';
      }
      if (shift) {
        shift = false;
        ba.m_bytes.get()[ba.m_size] = static_cast<uint8_t>(0xFF & (value << 4));
      } else {
        shift = true;
        ba.m_bytes.get()[ba.m_size++] |= static_cast<uint8_t>(0xFF & value);
      }
    }
    // Accommodate strings with an odd number of characters.
    if (!shift) {
      ba.m_size++;
    }
  }
  return ba;
}

ByteArray ByteArray::fromString(const wchar_t* wstr) {
  return fromString(convertWstringToString(wstr));
}

std::string ByteArray::toString(const ByteArray& ba) {
  std::string str;
  const uint8_t* bytes = ba.m_bytes.get();
  if (bytes) {
    for (std::size_t i = 0; i < ba.m_size; ++i) {
      for (std::size_t j = 2; j > 0; --j) {
        const uint8_t nibble =
            static_cast<uint8_t>(0x0F & (bytes[i] >> (4 * (j - 1))));
        if (nibble < 10U) {
          str += static_cast<char>('0' + nibble);
        } else {
          str += static_cast<char>('7' + nibble);
        }
      }
    }
  }
  return str;
}

ByteArray::ByteArray() : m_size(0) {
  // NOP
}

ByteArray::ByteArray(const ByteArray& other) : m_size(0) { operator=(other); }

ByteArray::ByteArray(const uint8_t* bytes, const std::size_t size) : m_size(0) {
  if (bytes && 0 < size) {
    m_bytes = std::shared_ptr<uint8_t>(new uint8_t[size],
                                       std::default_delete<uint8_t[]>());
    std::memcpy(m_bytes.get(), bytes, size);
    m_size = size;
  }
}

ByteArray::~ByteArray() {
  // NOP
}

ByteArray& ByteArray::operator=(const ByteArray& other) {
  if (&other != this) {
    m_bytes = other.m_bytes;
    m_size = other.m_size;
  }
  return *this;
}

std::string ByteArray::toString() const { return toString(*this); }
}  // namespace gemfire
