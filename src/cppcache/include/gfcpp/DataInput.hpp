#ifndef __GEMFIRE_DATAINPUT_H__
#define __GEMFIRE_DATAINPUT_H__

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"
#include "ExceptionTypes.hpp"
#include <string.h>
#include "gf_types.hpp"
#include "Serializable.hpp"
#include "CacheableString.hpp"

/**
 * @file
 */

#if GF_DEBUG_ASSERTS == 1
#define DINP_THROWONERROR_DEFAULT true
#else
#define DINP_THROWONERROR_DEFAULT false
#endif

#define checkBufferSize(x) _checkBufferSize(x, __LINE__)

namespace gemfire {

extern int gf_sprintf(char* buffer, const char* fmt, ...);

/**
 * Provide operations for reading primitive data values, byte arrays,
 * strings, <code>Serializable</code> objects from a byte stream.
 * This class is intentionally not thread safe.
 * @remarks None of the output parameters in the methods below can be NULL
 *   unless otherwise noted.
 */
class CPPCACHE_EXPORT DataInput {
 public:
  /**
   * Read an unsigned byte from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the unsigned byte read from stream
   */
  inline void read(uint8_t* value) {
    checkBufferSize(1);
    *value = *(m_buf++);
  }

  /**
   * Read a signed byte from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the signed byte read from stream
   */
  inline void read(int8_t* value) {
    checkBufferSize(1);
    *value = *(m_buf++);
  }

  /**
   * Read a boolean value from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the boolean read from stream
   */
  inline void readBoolean(bool* value) {
    checkBufferSize(1);
    *value = (*m_buf == 1 ? true : false);
    m_buf++;
  }

  /**
   * Read the given number of unsigned bytes from the <code>DataInput</code>.
   * @remarks This method is complimentary to
   *   <code>DataOutput::writeBytesOnly</code> and, unlike
   *   <code>readBytes</code>, does not expect the length of array
   *   in the stream.
   *
   * @param buffer array to hold the bytes read from stream
   * @param len number of unsigned bytes to be read
   */
  inline void readBytesOnly(uint8_t* buffer, uint32_t len) {
    if (len > 0) {
      checkBufferSize(len);
      memcpy(buffer, m_buf, len);
      m_buf += len;
    }
  }

  /**
   * Read the given number of signed bytes from the <code>DataInput</code>.
   * @remarks This method is complimentary to
   *   <code>DataOutput::writeBytesOnly</code> and, unlike
   *   <code>readBytes</code>, does not expect the length of array
   *   in the stream.
   *
   * @param buffer array to hold the bytes read from stream
   * @param len number of signed bytes to be read
   */
  inline void readBytesOnly(int8_t* buffer, uint32_t len) {
    if (len > 0) {
      checkBufferSize(len);
      memcpy(buffer, m_buf, len);
      m_buf += len;
    }
  }

  /**
   * Read an array of unsigned bytes from the <code>DataInput</code>
   * expecting to find the length of array in the stream at the start.
   * @remarks This method is complimentary to
   *   <code>DataOutput::writeBytes</code>.
   *
   * @param bytes output array to hold the bytes read from stream; the array
   *   is allocated by this method
   * @param len output parameter to hold the length of array read from stream
   */
  inline void readBytes(uint8_t** bytes, int32_t* len) {
    int32_t length;
    readArrayLen(&length);
    *len = length;
    uint8_t* buffer = NULL;
    if (length > 0) {
      checkBufferSize(length);
      GF_NEW(buffer, uint8_t[length]);
      memcpy(buffer, m_buf, length);
      m_buf += length;
    }
    *bytes = buffer;
  }

  /**
   * Read an array of signed bytes from the <code>DataInput</code>
   * expecting to find the length of array in the stream at the start.
   * @remarks This method is complimentary to
   *   <code>DataOutput::writeBytes</code>.
   *
   * @param bytes output array to hold the bytes read from stream; the array
   *   is allocated by this method
   * @param len output parameter to hold the length of array read from stream
   */
  inline void readBytes(int8_t** bytes, int32_t* len) {
    int32_t length;
    readArrayLen(&length);
    *len = length;
    int8_t* buffer = NULL;
    if (length > 0) {
      checkBufferSize(length);
      GF_NEW(buffer, int8_t[length]);
      memcpy(buffer, m_buf, length);
      m_buf += length;
    }
    *bytes = buffer;
  }

  /**
   * Read a 16-bit unsigned integer from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the 16-bit unsigned integer
   *   read from stream
   */
  inline void readInt(uint16_t* value) {
    checkBufferSize(2);
    uint16_t tmp = *(m_buf++);
    tmp = (uint16_t)((tmp << 8) | *(m_buf++));
    *value = tmp;
  }

  /**
   * Read a 32-bit unsigned integer from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the 32-bit unsigned integer
   *   read from stream
   */
  inline void readInt(uint32_t* value) {
    checkBufferSize(4);
    uint32_t tmp = *(m_buf++);
    tmp = (tmp << 8) | *(m_buf++);
    tmp = (tmp << 8) | *(m_buf++);
    tmp = (tmp << 8) | *(m_buf++);
    *value = tmp;
  }

  /**
   * Read a 64-bit unsigned integer from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the 64-bit unsigned integer
   *   read from stream
   */
  inline void readInt(uint64_t* value) {
    checkBufferSize(8);
    uint64_t tmp;
    if (sizeof(long) == 8) {
      tmp = *(m_buf++);
      tmp = (tmp << 8) | *(m_buf++);
      tmp = (tmp << 8) | *(m_buf++);
      tmp = (tmp << 8) | *(m_buf++);
      tmp = (tmp << 8) | *(m_buf++);
      tmp = (tmp << 8) | *(m_buf++);
      tmp = (tmp << 8) | *(m_buf++);
      tmp = (tmp << 8) | *(m_buf++);
    } else {
      uint32_t hword = *(m_buf++);
      hword = (hword << 8) | *(m_buf++);
      hword = (hword << 8) | *(m_buf++);
      hword = (hword << 8) | *(m_buf++);

      tmp = hword;
      hword = *(m_buf++);
      hword = (hword << 8) | *(m_buf++);
      hword = (hword << 8) | *(m_buf++);
      hword = (hword << 8) | *(m_buf++);
      tmp = (tmp << 32) | hword;
    }
    *value = tmp;
  }

  /**
   * Read a 16-bit signed integer from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the 16-bit signed integer
   *   read from stream
   */
  inline void readInt(int16_t* value) {
    checkBufferSize(2);
    readInt((uint16_t*)value);
  }

  /**
   * Read a 32-bit signed integer from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the 32-bit signed integer
   *   read from stream
   */
  inline void readInt(int32_t* value) {
    checkBufferSize(4);
    readInt((uint32_t*)value);
  }

  /**
   * Read a 64-bit signed integer from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the 64-bit signed integer
   *   read from stream
   */
  inline void readInt(int64_t* value) {
    checkBufferSize(8);
    readInt((uint64_t*)value);
  }

  /**
   * Read a 32-bit signed integer array length value from the
   * <code>DataInput</code> in a manner compatible with java server's
   * <code>DataSerializer.readArrayLength</code>.
   *
   * @param len output parameter to hold the 32-bit signed length
   *   read from stream
   */
  inline void readArrayLen(int32_t* len) {
    uint8_t code;
    read(&code);
    if (code == 0xFF) {
      *len = -1;
    } else {
      int32_t result = code;
      if (result > 252) {  // 252 is java's ((byte)-4 && 0xFF)
        if (code == 0xFE) {
          uint16_t val;
          readInt(&val);
          result = val;
        } else if (code == 0xFD) {
          uint32_t val;
          readInt(&val);
          result = val;
        } else {
          throw IllegalStateException("unexpected array length code");
        }
      }
      *len = result;
    }
  }

  /**
   * Decode a 64 bit integer as a variable length array.
   *
   * This is taken from the varint encoding in protobufs (BSD licensed).
   * See https://developers.google.com/protocol-buffers/docs/encoding
   */
  inline void readUnsignedVL(int64_t* value) {
    int32_t shift = 0;
    int64_t result = 0;
    while (shift < 64) {
      int8_t b;
      read(&b);
      result |= (int64_t)(b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        *value = result;
        return;
      }
      shift += 7;
    }
    throw IllegalStateException("Malformed variable length integer");
  }

  /**
   * Read a float from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the float read from stream
   */
  inline void readFloat(float* value) {
    checkBufferSize(4);
    union float_uint32_t {
      float f;
      uint32_t u;
    } v;
    readInt((uint32_t*)&v.u);
    *value = v.f;
  }

  /**
   * Read a double precision number from the <code>DataInput</code>.
   *
   * @param value output parameter to hold the double precision number
   *   read from stream
   */
  inline void readDouble(double* value) {
    checkBufferSize(8);
    union double_uint64_t {
      double d;
      uint64_t ll;
    } v;
    readInt((uint64_t*)&v.ll);
    *value = v.d;
  }

  /**
   * free the C string allocated by <code>readASCII</code>,
   * <code>readASCIIHuge</code>, <code>readUTF</code>,
   * <code>readUTFHuge</code> methods
   */
  static inline void freeUTFMemory(char* value) { delete[] value; }

  /**
   * free the wide-characted string allocated by <code>readASCII</code>,
   * <code>readASCIIHuge</code>, <code>readUTF</code>,
   * <code>readUTFHuge</code> methods
   */
  static inline void freeUTFMemory(wchar_t* value) { delete[] value; }

  /**
   * Allocates a c string buffer, and reads an ASCII string
   * having maximum length of 64K from <code>DataInput</code> into it.
   * @remarks Sets integer at length to hold the strlen of the string. Value
   *   is modified to point to the new allocation. The chars are allocated as
   *   an array, so the caller must use <code>freeUTFMemory</code> when done.
   *   Like <code>DataOutput::writeASCII</code> the maximum length supported by
   *   this method is 64K; use <code>readASCIIHuge</code> or
   *   <code>readBytes</code> to read strings of length larger than this.
   *
   * @param value output C string to hold the read characters; it is allocated
   *   by this method
   * @param len output parameter to hold the number of characters read from
   *   stream; not set if NULL
   */
  inline void readASCII(char** value, uint16_t* len = NULL) {
    uint16_t length;
    readInt(&length);
    checkBufferSize(length);
    if (len != NULL) {
      *len = length;
    }
    char* str;
    GF_NEW(str, char[length + 1]);
    *value = str;
    readBytesOnly((int8_t*)str, length);
    str[length] = '\0';
  }

  /**
   * Allocates a c string buffer, and reads an ASCII string
   * from <code>DataInput</code> into it.
   * @remarks Sets integer at length to hold the strlen of the string. Value
   *   is modified to point to the new allocation. The chars are allocated as
   *   an array, so the caller must use <code>freeUTFMemory</code> when done.
   *   Use this instead of <code>readUTF</code> when reading a string of length
   *   greater than 64K.
   *
   * @param value output C string to hold the read characters; it is allocated
   *   by this method
   * @param len output parameter to hold the number of characters read from
   *   stream; not set if NULL
   */
  inline void readASCIIHuge(char** value, uint32_t* len = NULL) {
    uint32_t length;
    readInt(&length);
    if (len != NULL) {
      *len = length;
    }
    char* str;
    GF_NEW(str, char[length + 1]);
    *value = str;
    readBytesOnly((int8_t*)str, length);
    str[length] = '\0';
  }

  /**
   * Allocates a c string buffer, and reads a java modified UTF-8
   * encoded string having maximum encoded length of 64K from
   * <code>DataInput</code> into it.
   * @remarks Sets integer at length to hold the strlen of the string. Value
   *   is modified to point to the new allocation. The chars are allocated as
   *   an array, so the caller must use <code>freeUTFMemory</code> when done.
   *   Like <code>DataOutput::writeUTF</code> the maximum length supported by
   *   this method is 64K; use <code>readAUTFHuge</code> to read strings of
   *   length larger than this.
   *
   * @param value output C string to hold the read characters; it is allocated
   *   by this method
   * @param len output parameter to hold the number of characters read from
   *   stream; not set if NULL
   */
  inline void readUTF(char** value, uint16_t* len = NULL) {
    uint16_t length;
    readInt(&length);
    checkBufferSize(length);
    uint16_t decodedLen = (uint16_t)getDecodedLength(m_buf, length);
    if (len != NULL) {
      *len = decodedLen;
    }
    char* str;
    GF_NEW(str, char[decodedLen + 1]);
    *value = str;
    for (uint16_t i = 0; i < decodedLen; i++) {
      decodeChar(str++);
    }
    *str = '\0';  // null terminate for c-string.
  }

  /**
   * Reads a java modified UTF-8 encoded string having maximum encoded length
   * of 64K without reading the length which must be passed as a parameter.
   * Allocates a c string buffer, and deserializes into it. Sets integer at
   * length to hold the length of the string. Value is modified to point to the
   * new allocation. The chars are allocated as an array, so the caller must
   * use freeUTFMemory when done.
   * If len == NULL, then the decoded string length is not set.
   */
  inline void readUTFNoLen(wchar_t** value, uint16_t decodedLen) {
    wchar_t* str;
    GF_NEW(str, wchar_t[decodedLen + 1]);
    *value = str;
    for (uint16_t i = 0; i < decodedLen; i++) {
      decodeChar(str++);
    }
    *str = L'\0';  // null terminate for c-string.
  }

  /**
   * Allocates a c string buffer, and reads a java modified UTF-8
   * encoded string from <code>DataInput</code> into it.
   * @remarks Sets integer at length to hold the strlen of the string. Value
   *   is modified to point to the new allocation. The chars are allocated as
   *   an array, so the caller must use <code>freeUTFMemory</code> when done.
   *   Use this instead of <code>readUTF</code> when reading a string of length
   *   greater than 64K.
   *
   * @param value output C string to hold the read characters; it is allocated
   *   by this method
   * @param len output parameter to hold the number of characters read from
   *   stream; not set if NULL
   */
  inline void readUTFHuge(char** value, uint32_t* len = NULL) {
    uint32_t length;
    readInt(&length);
    if (len != NULL) {
      *len = length;
    }
    char* str;
    GF_NEW(str, char[length + 1]);
    *value = str;
    for (uint32_t i = 0; i < length; i++) {
      int8_t item;
      read(&item);  // ignore this - should be higher order zero byte
      read(&item);
      *str = item;
      str++;
    }
    *str = '\0';  // null terminate for c-string.
  }

  /**
   * Allocates a wide-character string buffer, and reads a java
   * modified UTF-8 encoded string having maximum encoded length of 64K from
   * <code>DataInput</code> into it.
   * @remarks Sets integer at length to hold the strlen of the string. Value
   *   is modified to point to the new allocation. The chars are allocated as
   *   an array, so the caller must use <code>freeUTFMemory</code> when done.
   *   Like <code>DataOutput::writeUTF</code> the maximum length supported by
   *   this method is 64K; use <code>readAUTFHuge</code> to read strings of
   *   length larger than this.
   *
   * @param value output wide-character string to hold the read characters;
   *   it is allocated by this method
   * @param len output parameter to hold the number of characters read from
   *   stream; not set if NULL
   */
  inline void readUTF(wchar_t** value, uint16_t* len = NULL) {
    uint16_t length;
    readInt(&length);
    checkBufferSize(length);
    uint16_t decodedLen = (uint16_t)getDecodedLength(m_buf, length);
    if (len != NULL) {
      *len = decodedLen;
    }
    wchar_t* str;
    GF_NEW(str, wchar_t[decodedLen + 1]);
    *value = str;
    for (uint16_t i = 0; i < decodedLen; i++) {
      decodeChar(str++);
    }
    *str = L'\0';  // null terminate for c-string.
  }

  /**
   * Allocates a wide-character string buffer, and reads a java
   * modified UTF-8 encoded string from <code>DataInput</code> into it.
   * @remarks Sets integer at length to hold the strlen of the string. Value
   *   is modified to point to the new allocation. The chars are allocated as
   *   an array, so the caller must use <code>freeUTFMemory</code> when done.
   *   Use this instead of <code>readUTF</code> when reading a string of length
   *   greater than 64K.
   *
   * @param value output wide-character string to hold the read characters;
   *   it is allocated by this method
   * @param len output parameter to hold the number of characters read from
   *   stream; not set if NULL
   */
  inline void readUTFHuge(wchar_t** value, uint32_t* len = NULL) {
    uint32_t length;
    readInt(&length);
    if (len != NULL) {
      *len = length;
    }
    wchar_t* str;
    GF_NEW(str, wchar_t[length + 1]);
    *value = str;
    for (uint32_t i = 0; i < length; i++) {
      uint8_t hibyte;
      read(&hibyte);
      uint8_t lobyte;
      read(&lobyte);
      *str = (((uint16_t)hibyte) << 8) | (uint16_t)lobyte;
      str++;
    }
    *str = L'\0';  // null terminate for c-string.
  }

  /**
   * Read a <code>Serializable</code> object from the <code>DataInput</code>.
   * Null objects are handled.
   * This accepts an argument <code>throwOnError</code> that
   * specifies whether to check the type dynamically and throw a
   * <code>ClassCastException</code> when the cast fails.
   *
   * @param ptr The object to be read which is output by reference.
   *            The type of this must match the type of object that
   *            the application expects.
   * @param throwOnError Throw a <code>ClassCastException</code> when
   *                     the type of object does not match <code>ptr</code>.
   *                     Default is true when <code>GF_DEBUG_ASSERTS</code>
   *                     macro is set and false in normal case.
   * @throws ClassCastException When <code>dynCast</code> fails
   *                            for the given <code>ptr</code>.
   * @see dynCast
   * @see staticCast
   */
  template <class PTR>
  inline void readObject(SharedPtr<PTR>& ptr,
                         bool throwOnError = DINP_THROWONERROR_DEFAULT) {
    SerializablePtr sPtr;
    readObjectInternal(sPtr);
    if (throwOnError) {
      ptr = dynCast<SharedPtr<PTR> >(sPtr);
    } else {
      ptr = staticCast<SharedPtr<PTR> >(sPtr);
    }
  }

  inline bool readNativeBool() {
    int8_t typeId = 0;
    read(&typeId);

    bool val;
    readBoolean(&val);
    return val;
  }

  inline int32_t readNativeInt32() {
    int8_t typeId = 0;
    read(&typeId);

    int32_t val;
    readInt(&val);
    return val;
  }

  inline bool readNativeString(CacheableStringPtr& csPtr) {
    int8_t typeId = 0;
    read(&typeId);
    int64_t compId = typeId;
    if (compId == GemfireTypeIds::NullObj) {
      csPtr = NULLPTR;
    } else if (compId == GemfireTypeIds::CacheableNullString) {
      csPtr = CacheableStringPtr(dynamic_cast<CacheableString*>(
          CacheableString::createDeserializable()));
    } else if (compId == gemfire::GemfireTypeIds::CacheableASCIIString) {
      csPtr = CacheableStringPtr(dynamic_cast<CacheableString*>(
          CacheableString::createDeserializable()));
      csPtr.ptr()->fromData(*this);
    } else if (compId == gemfire::GemfireTypeIds::CacheableASCIIStringHuge) {
      csPtr = CacheableStringPtr(dynamic_cast<CacheableString*>(
          CacheableString::createDeserializableHuge()));
      csPtr.ptr()->fromData(*this);
    } else if (compId == gemfire::GemfireTypeIds::CacheableString) {
      csPtr = CacheableStringPtr(dynamic_cast<CacheableString*>(
          CacheableString::createUTFDeserializable()));
      csPtr.ptr()->fromData(*this);
    } else if (compId == gemfire::GemfireTypeIds::CacheableStringHuge) {
      csPtr = CacheableStringPtr(dynamic_cast<CacheableString*>(
          CacheableString::createUTFDeserializableHuge()));
      csPtr.ptr()->fromData(*this);
    } else {
      LOGDEBUG("In readNativeString something is wrong while expecting string");
      rewindCursor(1);
      csPtr = NULLPTR;
      return false;
    }
    return true;
  }

  inline void readDirectObject(SerializablePtr& ptr, int8_t typeId = -1) {
    readObjectInternal(ptr, typeId);
  }

  /**
   * Read a <code>Serializable</code> object from the <code>DataInput</code>.
   * Null objects are handled.
   */
  inline void readObject(SerializablePtr& ptr) { readObjectInternal(ptr); }

  inline void readObject(wchar_t* value) {
    uint16_t temp = 0;
    readInt(&temp);
    *value = (wchar_t)temp;
  }

  inline void readObject(bool* value) { readBoolean(value); }

  inline void readObject(int8_t* value) { read(value); }

  inline void readObject(int16_t* value) { readInt(value); }

  inline void readObject(int32_t* value) { readInt(value); }

  inline void readObject(int64_t* value) { readInt(value); }

  inline void readObject(float* value) { readFloat(value); }

  inline void readObject(double* value) { readDouble(value); }

  inline void readCharArray(char** value, int32_t& length) {
    int arrayLen = 0;
    readArrayLen(&arrayLen);
    length = arrayLen;
    char* objArray = NULL;
    if (arrayLen > 0) {
      objArray = new char[arrayLen];
      int i = 0;
      for (i = 0; i < arrayLen; i++) {
        char tmp = 0;
        readPdxChar(&tmp);
        objArray[i] = tmp;
      }
      *value = objArray;
    }
  }

  inline void readWideCharArray(wchar_t** value, int32_t& length) {
    readObject(value, length);
  }

  inline void readBooleanArray(bool** value, int32_t& length) {
    readObject(value, length);
  }

  inline void readByteArray(int8_t** value, int32_t& length) {
    readObject(value, length);
  }

  inline void readShortArray(int16_t** value, int32_t& length) {
    readObject(value, length);
  }

  inline void readIntArray(int32_t** value, int32_t& length) {
    readObject(value, length);
  }

  inline void readLongArray(int64_t** value, int32_t& length) {
    readObject(value, length);
  }

  inline void readFloatArray(float** value, int32_t& length) {
    readObject(value, length);
  }

  inline void readDoubleArray(double** value, int32_t& length) {
    readObject(value, length);
  }

  inline void readString(char** value) {
    int8_t typeId;
    read(&typeId);

    // Check for NULL String
    if (typeId == GemfireTypeIds::CacheableNullString) {
      *value = NULL;
      return;
    }
    /*
    if (typeId == GemfireTypeIds::CacheableString) {
      readUTF(value);
    } else {
      readUTFHuge(value);
    }
    */
    if (typeId == (int8_t)GemfireTypeIds::CacheableASCIIString ||
        typeId == (int8_t)GemfireTypeIds::CacheableString) {
      // readUTF( value);
      readASCII(value);
      // m_len = shortLen;
    } else if (typeId == (int8_t)GemfireTypeIds::CacheableASCIIStringHuge ||
               typeId == (int8_t)GemfireTypeIds::CacheableStringHuge) {
      // readUTFHuge( value);
      readASCIIHuge(value);
    } else {
      throw IllegalArgumentException(
          "DI readString error:: String type not supported ");
    }
  }

  inline void readWideString(wchar_t** value) {
    int8_t typeId;
    read(&typeId);

    // Check for NULL String
    if (typeId == GemfireTypeIds::CacheableNullString) {
      *value = NULL;
      return;
    }

    if (typeId == (int8_t)GemfireTypeIds::CacheableASCIIString ||
        typeId == (int8_t)GemfireTypeIds::CacheableString) {
      readUTF(value);
    } else if (typeId == (int8_t)GemfireTypeIds::CacheableASCIIStringHuge ||
               typeId == (int8_t)GemfireTypeIds::CacheableStringHuge) {
      readUTFHuge(value);
    } else {
      throw IllegalArgumentException(
          "DI readWideString error:: WideString type provided is not "
          "supported ");
    }
  }

  inline void readStringArray(char*** strArray, int32_t& length) {
    int32_t arrLen;
    readArrayLen(&arrLen);
    length = arrLen;
    if (arrLen == -1) {
      *strArray = NULL;
      return;
    } else {
      char** tmpArray;
      GF_NEW(tmpArray, char * [arrLen]);
      for (int i = 0; i < arrLen; i++) {
        readString(&tmpArray[i]);
      }
      *strArray = tmpArray;
    }
  }

  inline void readWideStringArray(wchar_t*** strArray, int32_t& length) {
    int32_t arrLen;
    readArrayLen(&arrLen);
    length = arrLen;
    if (arrLen == -1) {
      *strArray = NULL;
      return;
    } else {
      wchar_t** tmpArray;
      GF_NEW(tmpArray, wchar_t * [arrLen]);
      for (int i = 0; i < arrLen; i++) {
        readWideString(&tmpArray[i]);
      }
      *strArray = tmpArray;
    }
  }

  inline void readArrayOfByteArrays(int8_t*** arrayofBytearr,
                                    int32_t& arrayLength,
                                    int32_t** elementLength) {
    int32_t arrLen;
    readArrayLen(&arrLen);
    arrayLength = arrLen;

    if (arrLen == -1) {
      *arrayofBytearr = NULL;
      return;
    } else {
      int8_t** tmpArray;
      int32_t* tmpLengtharr;
      GF_NEW(tmpArray, int8_t * [arrLen]);
      GF_NEW(tmpLengtharr, int32_t[arrLen]);
      for (int i = 0; i < arrLen; i++) {
        readBytes(&tmpArray[i], &tmpLengtharr[i]);
      }
      *arrayofBytearr = tmpArray;
      *elementLength = tmpLengtharr;
    }
  }

  /**
   * Get the length required to represent a given UTF-8 encoded string
   * (created using {@link DataOutput::writeUTF} or
   * <code>java.io.DataOutput.writeUTF</code>) in wide-character format.
   *
   * @param value The UTF-8 encoded stream.
   * @param length The length of the stream to be read.
   * @return The length of the decoded string.
   * @see DataOutput::getEncodedLength
   */
  static int32_t getDecodedLength(const uint8_t* value, int32_t length) {
    const uint8_t* end = value + length;
    int32_t decodedLen = 0;
    while (value < end) {
      // get next byte unsigned
      int32_t b = *value++ & 0xff;
      int32_t k = b >> 5;
      // classify based on the high order 3 bits
      switch (k) {
        case 6: {
          value++;
          break;
        }
        case 7: {
          value += 2;
          break;
        }
        default:
          break;
      }
      decodedLen += 1;
    }
    if (value > end) decodedLen--;
    return decodedLen;
  }

  /** constructor given a pre-allocated byte array with size */
  DataInput(const uint8_t* m_buffer, int32_t len)
      : m_buf(m_buffer),
        m_bufHead(m_buffer),
        m_bufLength(len),
        m_poolName(NULL) {}

  /** destructor */
  ~DataInput() {}

  /**
   * Get the pointer to current buffer position. This should be treated
   * as readonly and modification of contents using this internal pointer
   * has undefined behavior.
   */
  inline const uint8_t* currentBufferPosition() const { return m_buf; }

  /** get the number of bytes read in the buffer */
  inline int32_t getBytesRead() const {
    return static_cast<int32_t>(m_buf - m_bufHead);
  }

  /** get the number of bytes remaining to be read in the buffer */
  inline int32_t getBytesRemaining() const {
    return (m_bufLength - getBytesRead());
  }

  /** advance the cursor by given offset */
  inline void advanceCursor(int32_t offset) { m_buf += offset; }

  /** rewind the cursor by given offset */
  inline void rewindCursor(int32_t offset) { m_buf -= offset; }

  /** reset the cursor to the start of buffer */
  inline void reset() { m_buf = m_bufHead; }

  inline void setBuffer() {
    m_buf = currentBufferPosition();
    m_bufLength = getBytesRemaining();
  }

  inline void resetPdx(int32_t offset) { m_buf = m_bufHead + offset; }

  inline int32_t getPdxBytes() const { return m_bufLength; }

  static uint8_t* getBufferCopy(const uint8_t* from, uint32_t length) {
    uint8_t* result;
    GF_NEW(result, uint8_t[length]);
    memcpy(result, from, length);

    return result;
  }

  inline void reset(int32_t offset) { m_buf = m_bufHead + offset; }

  uint8_t* getBufferCopyFrom(const uint8_t* from, uint32_t length) {
    uint8_t* result;
    GF_NEW(result, uint8_t[length]);
    memcpy(result, from, length);

    return result;
  }

  /*
   * This is for internal use
   */
  const char* getPoolName() { return m_poolName; }

  /*
   * This is for internal use
   */
  void setPoolName(const char* poolName) { m_poolName = poolName; }

 private:
  const uint8_t* m_buf;
  const uint8_t* m_bufHead;
  int32_t m_bufLength;
  const char* m_poolName;

  void readObjectInternal(SerializablePtr& ptr, int8_t typeId = -1);

  template <typename mType>
  void readObject(mType** value, int32_t& length) {
    int arrayLen;
    readArrayLen(&arrayLen);
    length = arrayLen;
    mType* objArray;
    if (arrayLen > 0) {
      objArray = new mType[arrayLen];
      int i = 0;
      for (i = 0; i < arrayLen; i++) {
        mType tmp = 0;
        readObject(&tmp);
        objArray[i] = tmp;  //*value[i] = tmp;
      }
      *value = objArray;
    }
  }

  inline void readPdxChar(char* value) {
    int16_t val = 0;
    readInt(&val);
    *value = (char)val;
  }

  inline void _checkBufferSize(int32_t size, int32_t line) {
    if ((m_bufLength - (m_buf - m_bufHead)) < size) {
      char exMsg[128];
      gf_sprintf(exMsg,
                 "DataInput: attempt to read beyond buffer at line %d: "
                 "available buffer size %d, attempted read of size %d ",
                 line, m_bufLength - (m_buf - m_bufHead), size);
      throw OutOfRangeException(exMsg);
    }
  }

  inline void decodeChar(char* str) {
    uint8_t bt = *(m_buf++);
    if (bt & 0x80) {
      if (bt & 0x20) {
        // three bytes.
        *str = (char)(((bt & 0x0f) << 12) | (((*m_buf++) & 0x3f) << 6));
        *str |= (char)((*m_buf++) & 0x3f);
      } else {
        // two bytes.
        *str = (char)(((bt & 0x1f) << 6) | ((*m_buf++) & 0x3f));
      }
    } else {
      // single byte...
      *str = bt;
    }
  }

  inline void decodeChar(wchar_t* str) {
    // get next byte unsigned
    int32_t b = *m_buf++ & 0xff;
    int32_t k = b >> 5;
    // classify based on the high order 3 bits
    switch (k) {
      case 6: {
        // two byte encoding
        // 110yyyyy 10xxxxxx
        // use low order 6 bits
        int32_t y = b & 0x1f;
        // use low order 6 bits of the next byte
        // It should have high order bits 10, which we don't check.
        int32_t x = *m_buf++ & 0x3f;
        // 00000yyy yyxxxxxx
        *str = (y << 6 | x);
        break;
      }
      case 7: {
        // three byte encoding
        // 1110zzzz 10yyyyyy 10xxxxxx
        // assert ( b & 0x10 )
        //     == 0 : "UTF8Decoder does not handle 32-bit characters";
        // use low order 4 bits
        int32_t z = b & 0x0f;
        // use low order 6 bits of the next byte
        // It should have high order bits 10, which we don't check.
        int32_t y = *m_buf++ & 0x3f;
        // use low order 6 bits of the next byte
        // It should have high order bits 10, which we don't check.
        int32_t x = *m_buf++ & 0x3f;
        // zzzzyyyy yyxxxxxx
        int32_t asint = (z << 12 | y << 6 | x);
        *str = asint;
        break;
      }
      default:
        // one byte encoding
        // 0xxxxxxx
        // use just low order 7 bits
        // 00000000 0xxxxxxx
        *str = (b & 0x7f);
        break;
    }
  }

  // disable other constructors and assignment
  DataInput();
  DataInput(const DataInput&);
  DataInput& operator=(const DataInput&);
};
}

#endif  // __GEMFIRE_DATAINPUT_H__
