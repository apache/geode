#ifndef __GEMFIRE_DATAOUTPUT_H__
#define __GEMFIRE_DATAOUTPUT_H__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"
#include "ExceptionTypes.hpp"
#include "Log.hpp"
#include "Serializable.hpp"
#include "CacheableString.hpp"

extern "C" {
#include <string.h>
#include <stdlib.h>
}

/**
 * @file
 */

namespace gemfire {

/**
 * C style memory allocation that throws OutOfMemoryException
 * if it fails
 */
#define GF_ALLOC(v, t, s)                                                  \
  {                                                                        \
    v = (t*)malloc((s) * sizeof(t));                                       \
    if ((v) == NULL) {                                                     \
      throw OutOfMemoryException(                                          \
          "Out of Memory while allocating buffer for " #t " of size " #s); \
    }                                                                      \
  }

/**
 * C style memory re-allocation that throws OutOfMemoryException
 * if it fails
 */
#define GF_RESIZE(v, t, s)                                \
  {                                                       \
    v = (t*)realloc(v, (s) * sizeof(t));                  \
    if ((v) == NULL) {                                    \
      throw OutOfMemoryException(                         \
          "Out of Memory while resizing buffer for " #t); \
    }                                                     \
  }

#define GF_FREE(v) free(v)

/**
 * Provide operations for writing primitive data values, byte arrays,
 * strings, <code>Serializable</code> objects to a byte stream.
 * This class is intentionally not thread safe.
 */
class CPPCACHE_EXPORT DataOutput {
 public:
  /**
   * Construct a new DataOutput.
   */
  DataOutput();

  /**
   * Write an unsigned byte to the <code>DataOutput</code>.
   *
   * @param value the unsigned byte to be written
   */
  inline void write(uint8_t value) {
    ensureCapacity(1);
    writeNoCheck(value);
  }

  /**
   * Write a signed byte to the <code>DataOutput</code>.
   *
   * @param value the signed byte to be written
   */
  inline void write(int8_t value) { write((uint8_t)value); }

  /**
   * Write a boolean value to the <code>DataOutput</code>.
   *
   * @param value the boolean value to be written
   */
  inline void writeBoolean(bool value) { write((uint8_t)value); }

  /**
   * Write an array of unsigned bytes to the <code>DataOutput</code>.
   *
   * @param value the array of unsigned bytes to be written
   * @param len the number of bytes from the start of array to be written
   */
  inline void writeBytes(const uint8_t* bytes, int32_t len) {
    if (len >= 0) {
      ensureCapacity(len + 5);
      writeArrayLen(bytes == NULL ? 0 : len);  // length of bytes...
      if (len > 0 && bytes != NULL) {
        memcpy(m_buf, bytes, len);
        m_buf += len;
      }
    } else {
      write((int8_t)-1);
    }
  }

  /**
   * Write an array of signed bytes to the <code>DataOutput</code>.
   *
   * @param value the array of signed bytes to be written
   * @param len the number of bytes from the start of array to be written
   */
  inline void writeBytes(const int8_t* bytes, int32_t len) {
    writeBytes((const uint8_t*)bytes, len);
  }

  /**
   * Write an array of unsigned bytes without its length to the
   * <code>DataOutput</code>.
   * @remarks The difference between this and <code>writeBytes</code> is that
   *   this does write the length of bytes so the corresponding
   *   <code>DataInput::readBytesOnly</code> (unlike
   *   <code>DataInput::readBytes</code>) needs the length argument explicitly.
   *
   * @param value the array of unsigned bytes to be written
   * @param len the number of bytes from the start of array to be written
   */
  inline void writeBytesOnly(const uint8_t* bytes, uint32_t len) {
    ensureCapacity(len);
    memcpy(m_buf, bytes, len);
    m_buf += len;
  }

  /**
   * Write an array of signed bytes without its length to the
   * <code>DataOutput</code>.
   * @remarks The difference between this and <code>writeBytes</code> is that
   *   this does write the length of bytes so the corresponding
   *   <code>DataInput::readBytesOnly</code> (unlike
   *   <code>DataInput::readBytes</code>) needs the length argument explicitly.
   *
   * @param value the array of signed bytes to be written
   * @param len the number of bytes from the start of array to be written
   */
  inline void writeBytesOnly(const int8_t* bytes, uint32_t len) {
    writeBytesOnly((const uint8_t*)bytes, len);
  }

  /**
   * Write a 16-bit unsigned integer value to the <code>DataOutput</code>.
   *
   * @param value the 16-bit unsigned integer value to be written
   */
  inline void writeInt(uint16_t value) {
    ensureCapacity(2);
    *(m_buf++) = (uint8_t)(value >> 8);
    *(m_buf++) = (uint8_t)value;
  }

  /**
   * Write a 16-bit Char (wchar_t) value to the <code>DataOutput</code>.
   *
   * @param value the 16-bit wchar_t value to be written
   */
  inline void writeChar(uint16_t value) {
    ensureCapacity(2);
    *(m_buf++) = (uint8_t)(value >> 8);
    *(m_buf++) = (uint8_t)value;
  }

  /**
   * Write a 32-bit unsigned integer value to the <code>DataOutput</code>.
   *
   * @param value the 32-bit unsigned integer value to be written
   */
  inline void writeInt(uint32_t value) {
    ensureCapacity(4);
    *(m_buf++) = (uint8_t)(value >> 24);
    *(m_buf++) = (uint8_t)(value >> 16);
    *(m_buf++) = (uint8_t)(value >> 8);
    *(m_buf++) = (uint8_t)value;
  }

  /**
   * Write a 64-bit unsigned integer value to the <code>DataOutput</code>.
   *
   * @param value the 64-bit unsigned integer value to be written
   */
  inline void writeInt(uint64_t value) {
    ensureCapacity(8);
    // the defines are not reliable and can be changed by compiler options.
    // Hence using sizeof() test instead.
    //#if defined(_LP64) || ( defined(__WORDSIZE) && __WORDSIZE == 64 ) ||
    //( defined(_INTEGRAL_MAX_BITS) && _INTEGRAL_MAX_BITS >= 64 )
    if (sizeof(long) == 8) {
      *(m_buf++) = (uint8_t)(value >> 56);
      *(m_buf++) = (uint8_t)(value >> 48);
      *(m_buf++) = (uint8_t)(value >> 40);
      *(m_buf++) = (uint8_t)(value >> 32);
      *(m_buf++) = (uint8_t)(value >> 24);
      *(m_buf++) = (uint8_t)(value >> 16);
      *(m_buf++) = (uint8_t)(value >> 8);
      *(m_buf++) = (uint8_t)value;
    } else {
      uint32_t hword = (uint32_t)(value >> 32);
      *(m_buf++) = (uint8_t)(hword >> 24);
      *(m_buf++) = (uint8_t)(hword >> 16);
      *(m_buf++) = (uint8_t)(hword >> 8);
      *(m_buf++) = (uint8_t)hword;

      hword = (uint32_t)value;
      *(m_buf++) = (uint8_t)(hword >> 24);
      *(m_buf++) = (uint8_t)(hword >> 16);
      *(m_buf++) = (uint8_t)(hword >> 8);
      *(m_buf++) = (uint8_t)hword;
    }
  }

  /**
   * Write a 16-bit signed integer value to the <code>DataOutput</code>.
   *
   * @param value the 16-bit signed integer value to be written
   */
  inline void writeInt(int16_t value) { writeInt((uint16_t)value); }

  /**
   * Write a 32-bit signed integer value to the <code>DataOutput</code>.
   *
   * @param value the 32-bit signed integer value to be written
   */
  inline void writeInt(int32_t value) { writeInt((uint32_t)value); }

  /**
   * Write a 64-bit signed integer value to the <code>DataOutput</code>.
   *
   * @param value the 64-bit signed integer value to be written
   */
  inline void writeInt(int64_t value) { writeInt((uint64_t)value); }

  /**
   * Write a 32-bit signed integer array length value to the
   * <code>DataOutput</code> in a manner compatible with java server's
   * <code>DataSerializer.writeArrayLength</code>.
   *
   * @param value the 32-bit signed integer array length to be written
   */
  inline void writeArrayLen(int32_t len) {
    if (len == -1) {
      write((int8_t)-1);
    } else if (len <= 252) {  // 252 is java's ((byte)-4 && 0xFF)
      write((uint8_t)len);
    } else if (len <= 0xFFFF) {
      write((int8_t)-2);
      writeInt((uint16_t)len);
    } else {
      write((int8_t)-3);
      writeInt(len);
    }
  }

  /**
   * Write a float value to the <code>DataOutput</code>.
   *
   * @param value the float value to be written
   */
  inline void writeFloat(float value) {
    union float_uint32_t {
      float f;
      uint32_t u;
    } v;
    v.f = value;
    writeInt(v.u);
  }

  /**
   * Write a double precision real number to the <code>DataOutput</code>.
   *
   * @param value the double precision real number to be written
   */
  inline void writeDouble(double value) {
    union double_uint64_t {
      double d;
      uint64_t ll;
    } v;
    v.d = value;
    writeInt(v.ll);
  }

  /**
   * Writes the given ASCII string supporting maximum length of 64K
   * (i.e. unsigned 16-bit integer).
   * @remarks The string will be truncated if greater than the maximum
   *   permissible length of 64K. Use <code>writeBytes</code> or
   *   <code>writeASCIIHuge</code> to write ASCII strings of length larger
   *   than this.
   *
   * @param value the C string to be written
   * @param length the number of characters from start of string to be
   *   written; the default value of 0 implies the complete string
   */
  inline void writeASCII(const char* value, uint32_t length = 0) {
    if (value != NULL) {
      if (length == 0) {
        length = (uint32_t)strlen(value);
      }
      uint16_t len = (uint16_t)(length > 0xFFFF ? 0xFFFF : length);
      writeInt(len);
      writeBytesOnly((int8_t*)value, len);  // K64
    } else {
      writeInt((uint16_t)0);
    }
  }

  inline void writeNativeString(const char* value) {
    // create cacheable string
    // write typeid id.
    // call todata
    CacheableStringPtr csPtr = CacheableString::create(value);
    write(csPtr->typeId());
    csPtr->toData(*this);
  }

  /**
   * Writes the given ASCII string supporting upto maximum 32-bit
   * integer value.
   * @remarks Use this to write large ASCII strings. The other
   *   <code>writeASCII</code> method will truncate strings greater than
   *   64K in size.
   *
   * @param value the wide-character string to be written
   * @param length the number of characters from start of string to be
   *   written; the default value of 0 implies the complete string
   */
  inline void writeASCIIHuge(const char* value, uint32_t length = 0) {
    if (value != NULL) {
      if (length == 0) {
        length = (uint32_t)strlen(value);
      }
      writeInt((uint32_t)length);
      writeBytesOnly((int8_t*)value, (uint32_t)length);
    } else {
      writeInt((uint32_t)0);
    }
  }

  /**
     * Writes the given given string using java modified UTF-8 encoding
     * supporting maximum encoded length of 64K (i.e. unsigned 16-bit integer).
     * @remarks The string will be truncated if greater than the maximum
     *   permissible length of 64K. Use <code>writeUTFHuge</code> to write
     *   strings of length larger than this.
     *
     * @param value the C string to be written
     *
     */
  inline void writeFullUTF(const char* value, uint32_t length = 0) {
    if (value != NULL) {
      int32_t len = getEncodedLength(value, length);
      uint16_t encodedLen = (uint16_t)(len > 0xFFFF ? 0xFFFF : len);
      writeInt((int32_t)encodedLen);
      ensureCapacity(encodedLen);
      write((int8_t)0);  // isObject = 0 BYTE_CODE
      uint8_t* end = m_buf + encodedLen;
      while (m_buf < end) {
        encodeChar(*value++);
      }
      if (m_buf > end) m_buf = end;
    } else {
      writeInt((uint16_t)0);
    }
  }

  /**
   * Writes the given given string using java modified UTF-8 encoding
   * supporting maximum encoded length of 64K (i.e. unsigned 16-bit integer).
   * @remarks The string will be truncated if greater than the maximum
   *   permissible length of 64K. Use <code>writeUTFHuge</code> to write
   *   strings of length larger than this.
   *
   * @param value the C string to be written
   * @param length the number of characters from start of string to be
   *   written; the default value of 0 implies the complete string
   */
  inline void writeUTF(const char* value, uint32_t length = 0) {
    if (value != NULL) {
      int32_t len = getEncodedLength(value, length);
      uint16_t encodedLen = (uint16_t)(len > 0xFFFF ? 0xFFFF : len);
      writeInt(encodedLen);
      ensureCapacity(encodedLen);
      uint8_t* end = m_buf + encodedLen;
      while (m_buf < end) {
        encodeChar(*value++);
      }
      if (m_buf > end) m_buf = end;
    } else {
      writeInt((uint16_t)0);
    }
  }

  /**
   * Writes the given string using java modified UTF-8 encoding.
   * @remarks Use this to write large strings. The other
   *   <code>writeUTF</code> method will truncate strings greater than
   *   64K in size.
   *
   * @param value the C string to be written
   * @param length the number of characters from start of string to be
   *   written; the default value of 0 implies the complete string
   *   assuming a null terminated string; do not use this unless sure
   *   that the UTF string does not contain any null characters
   */
  inline void writeUTFHuge(const char* value, uint32_t length = 0) {
    if (value != NULL) {
      if (length == 0) {
        length = (uint32_t)strlen(value);
      }
      writeInt(length);
      ensureCapacity(length * 2);
      for (uint32_t pos = 0; pos < length; pos++) {
        writeNoCheck((int8_t)0);
        writeNoCheck((int8_t)value[pos]);
      }
    } else {
      writeInt((uint32_t)0);
    }
  }

  /**
   * Writes the given given string using java modified UTF-8 encoding
   * supporting maximum encoded length of 64K (i.e. unsigned 16-bit integer).
   * @remarks The string will be truncated if greater than the maximum
   *   permissible length of 64K. Use <code>writeUTFHuge</code> to write
   *   strings of length larger than this.
   *
   * @param value the wide-character string to be written
   * @param length the number of characters from start of string to be
   *   written; the default value of 0 implies the complete string
   */
  inline void writeUTF(const wchar_t* value, uint32_t length = 0) {
    if (value != NULL) {
      int32_t len = getEncodedLength(value, length);
      uint16_t encodedLen = (uint16_t)(len > 0xFFFF ? 0xFFFF : len);
      writeInt(encodedLen);
      ensureCapacity(encodedLen);
      uint8_t* end = m_buf + encodedLen;
      while (m_buf < end) {
        encodeChar(*value++);
      }
      if (m_buf > end) m_buf = end;
    } else {
      writeInt((uint16_t)0);
    }
  }

  /**
   * Writes the given string using java modified UTF-8 encoding.
   * @remarks Use this to write large strings. The other
   *   <code>writeUTF</code> method will truncate strings greater than
   *   64K in size.
   *
   * @param value the wide-character string to be written
   * @param length the number of characters from start of string to be
   *   written; the default value of 0 implies the complete string
   */
  inline void writeUTFHuge(const wchar_t* value, uint32_t length = 0) {
    if (value != NULL) {
      if (length == 0) {
        length = (uint32_t)wcslen(value);
      }
      writeInt(length);
      ensureCapacity(length * 2);
      for (uint32_t pos = 0; pos < length; pos++) {
        uint16_t item = (uint16_t)value[pos];
        writeNoCheck((uint8_t)((item & 0xFF00) >> 8));
        writeNoCheck((uint8_t)(item & 0xFF));
      }
    } else {
      writeInt((uint32_t)0);
    }
  }

  /**
   * Get the length required to represent a given wide-character string in
   * java modified UTF-8 format.
   *
   * @param value The C string.
   * @param length The length of the string; or zero to use the full string.
   * @return The length required for representation in java modified
   *         UTF-8 format.
   * @see DataInput::getDecodedLength
   */
  inline static int32_t getEncodedLength(const char* value, int32_t length = 0,
                                         uint32_t* valLength = NULL) {
    if (value == NULL) return 0;
    char currentChar;
    int32_t encodedLen = 0;
    const char* start = value;
    if (length == 0) {
      while ((currentChar = *value) != '\0') {
        getEncodedLength(currentChar, encodedLen);
        value++;
      }
    } else {
      const char* end = value + length;
      while (value < end) {
        currentChar = *value;
        getEncodedLength(currentChar, encodedLen);
        value++;
      }
    }
    if (valLength != NULL) {
      *valLength = static_cast<uint32_t>(value - start);
    }
    return encodedLen;
  }

  /**
   * Get the length required to represent a given wide-character string in
   * java modified UTF-8 format.
   *
   * @param value The wide-character string.
   * @param length The length of the string.
   * @return The length required for representation in java modified
   *         UTF-8 format.
   * @see DataInput::getDecodedLength
   */
  inline static int32_t getEncodedLength(const wchar_t* value,
                                         int32_t length = 0,
                                         uint32_t* valLength = NULL) {
    if (value == NULL) return 0;
    wchar_t currentChar;
    int32_t encodedLen = 0;
    const wchar_t* start = value;
    if (length == 0) {
      while ((currentChar = *value) != 0) {
        getEncodedLength(currentChar, encodedLen);
        value++;
      }
    } else {
      const wchar_t* end = value + length;
      while (value < end) {
        currentChar = *value;
        getEncodedLength(currentChar, encodedLen);
        value++;
      }
    }
    if (valLength != NULL) {
      *valLength = static_cast<uint32_t>(value - start);
    }
    return encodedLen;
  }

  /**
   * Write a <code>Serializable</code> object to the <code>DataOutput</code>.
   *
   * @param objptr smart pointer to the <code>Serializable</code> object
   *   to be written
   */
  template <class PTR>
  void writeObject(const SharedPtr<PTR>& objptr, bool isDelta = false) {
    writeObjectInternal(objptr.ptr(), isDelta);
  }

  /**
   * Write a <code>Serializable</code> object to the <code>DataOutput</code>.
   *
   * @param objptr pointer to the <code>Serializable</code> object
   *   to be written
   */
  void writeObject(const Serializable* objptr) { writeObjectInternal(objptr); }

  /**
   * Get an internal pointer to the current location in the
   * <code>DataOutput</code> byte array.
   */
  const uint8_t* getCursor() { return m_buf; }

  /**
   * Advance the buffer cursor by the given offset.
   *
   * @param offset the offset by which to advance the cursor
   */
  void advanceCursor(uint32_t offset) {
    ensureCapacity(offset);
    m_buf += offset;
  }

  /**
   * Rewind the buffer cursor by the given offset.
   *
   * @param offset the offset by which to rewind the cursor
   */
  void rewindCursor(uint32_t offset) { m_buf -= offset; }

  void updateValueAtPos(uint32_t offset, uint8_t value) {
    m_bytes[offset] = value;
  }

  uint8_t getValueAtPos(uint32_t offset) { return m_bytes[offset]; }
  /** Destruct a DataOutput, including releasing the created buffer. */
  ~DataOutput() {
    reset();
    DataOutput::checkinBuffer(m_bytes, m_size);
  }

  /**
   * Get a pointer to the internal buffer of <code>DataOutput</code>.
   */
  inline const uint8_t* getBuffer() const {
    // GF_R_ASSERT(!((uint32_t)(m_bytes) % 4));
    return m_bytes;
  }

  /**
   * Get a pointer to the internal buffer of <code>DataOutput</code>.
   */
  inline uint32_t getRemainingBufferLength() const {
    // GF_R_ASSERT(!((uint32_t)(m_bytes) % 4));
    return m_size - getBufferLength();
  }

  /**
   * Get a pointer to the internal buffer of <code>DataOutput</code>.
   *
   * @param rsize the size of buffer is filled in this output parameter;
   *   should not be NULL
   */
  inline const uint8_t* getBuffer(uint32_t* rsize) const {
    *rsize = (uint32_t)(m_buf - m_bytes);
    // GF_R_ASSERT(!((uint32_t)(m_bytes) % 4));
    return m_bytes;
  }

  inline uint8_t* getBufferCopy() {
    uint32_t size = (uint32_t)(m_buf - m_bytes);
    uint8_t* result;
    GF_ALLOC(result, uint8_t, size);
    memcpy(result, m_bytes, size);
    return result;
  }

  /**
   * Get the length of current data in the internal buffer of
   * <code>DataOutput</code>.
   */
  inline uint32_t getBufferLength() const {
    return (uint32_t)(m_buf - m_bytes);
  }

  /**
   * Reset the internal cursor to the start of the buffer.
   */
  inline void reset() {
    if (m_haveBigBuffer) {
      // free existing buffer
      GF_FREE(m_bytes);
      // create smaller buffer
      GF_ALLOC(m_bytes, uint8_t, m_lowWaterMark);
      m_size = m_lowWaterMark;
      // reset the flag
      m_haveBigBuffer = false;
      // release the lock
      releaseLock();
    }
    m_buf = m_bytes;
  }

  // make sure there is room left for the requested size item.
  inline void ensureCapacity(uint32_t size) {
    uint32_t offset = (uint32_t)(m_buf - m_bytes);
    if ((m_size - offset) < size) {
      uint32_t newSize = m_size * 2 + (8192 * (size / 8192));
      if (newSize >= m_highWaterMark && !m_haveBigBuffer) {
        // acquire the lock
        acquireLock();
        // set flag
        m_haveBigBuffer = true;
      }
      m_size = newSize;
      GF_RESIZE(m_bytes, uint8_t, m_size);
      m_buf = m_bytes + offset;
    }
  }

  /*
  * This is for internal use
  */
  const char* getPoolName() { return m_poolName; }

  /*
   * This is for internal use
   */
  void setPoolName(const char* poolName) { m_poolName = poolName; }

  uint8_t* getBufferCopyFrom(const uint8_t* from, uint32_t length) {
    uint8_t* result;
    GF_NEW(result, uint8_t[length]);
    memcpy(result, from, length);

    return result;
  }

  static void safeDelete(uint8_t* src) { GF_SAFE_DELETE(src); }

  static DataOutput* getDataOutput() { return new DataOutput(); }
  static void releaseDataOutput(DataOutput* dataOutput) {
    GF_SAFE_DELETE(dataOutput);
  }

 private:
  void writeObjectInternal(const Serializable* ptr, bool isDelta = false);

  static void acquireLock();
  static void releaseLock();

  const char* m_poolName;
  // memory m_buffer to encode to.
  uint8_t* m_bytes;
  // cursor.
  uint8_t* m_buf;
  // size of m_bytes.
  uint32_t m_size;
  // high and low water marks for buffer size
  static uint32_t m_lowWaterMark;
  static uint32_t m_highWaterMark;
  // flag to indicate we have a big buffer
  volatile bool m_haveBigBuffer;

  inline static void getEncodedLength(const char val, int32_t& encodedLen) {
    if ((val == 0) || (val & 0x80)) {
      // two byte.
      encodedLen += 2;
    } else {
      // one byte.
      encodedLen++;
    }
  }

  inline static void getEncodedLength(const wchar_t val, int32_t& encodedLen) {
    if (val == 0) {
      encodedLen += 2;
    } else if (val < 0x80)  // ASCII character
    {
      encodedLen++;
    } else if (val < 0x800) {
      encodedLen += 2;
    } else {
      encodedLen += 3;
    }
  }

  inline void encodeChar(const char value) {
    uint8_t tmp = (uint8_t)value;
    if ((tmp == 0) || (tmp & 0x80)) {
      // two byte.
      *(m_buf++) = (uint8_t)(0xc0 | ((tmp & 0xc0) >> 6));
      *(m_buf++) = (uint8_t)(0x80 | (tmp & 0x3f));
    } else {
      // one byte.
      *(m_buf++) = tmp;
    }
  }

  // this will lose the character set encoding.
  inline void encodeChar(const wchar_t value) {
    uint16_t c = (uint16_t)value;
    if (c == 0) {
      *(m_buf++) = 0xc0;
      *(m_buf++) = 0x80;
    } else if (c < 0x80) {  // ASCII character
      *(m_buf++) = (uint8_t)c;
    } else if (c < 0x800) {
      *(m_buf++) = (uint8_t)(0xC0 | c >> 6);
      *(m_buf++) = (uint8_t)(0x80 | (c & 0x3F));
    } else {
      *(m_buf++) = (uint8_t)(0xE0 | c >> 12);
      *(m_buf++) = (uint8_t)(0x80 | ((c >> 6) & 0x3F));
      *(m_buf++) = (uint8_t)(0x80 | (c & 0x3F));
    }
  }

  inline void writeNoCheck(uint8_t value) { *(m_buf++) = value; }

  inline void writeNoCheck(int8_t value) { writeNoCheck((uint8_t)value); }

  static uint8_t* checkoutBuffer(uint32_t* size);
  static void checkinBuffer(uint8_t* buffer, uint32_t size);

  // disable copy constructor and assignment
  DataOutput(const DataOutput&);
  DataOutput& operator=(const DataOutput&);
};
}

#endif  // __GEMFIRE_DATAOUTPUT_H__
