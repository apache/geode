#ifndef _GEMFIRE_CACHEABLESTRING_HPP_
#define _GEMFIRE_CACHEABLESTRING_HPP_

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
#include "GemfireTypeIds.hpp"
#include "ExceptionTypes.hpp"

/** @file
 */

namespace gemfire {

#define GF_STRING (int8_t) GemfireTypeIds::CacheableASCIIString
#define GF_STRING_HUGE (int8_t) GemfireTypeIds::CacheableASCIIStringHuge
#define GF_WIDESTRING (int8_t) GemfireTypeIds::CacheableString
#define GF_WIDESTRING_HUGE (int8_t) GemfireTypeIds::CacheableStringHuge

/**
 * Implement a immutable C string wrapper that can serve as a distributable
 * key object for caching as well as being a string value.
 */
class CPPCACHE_EXPORT CacheableString : public CacheableKey {
 protected:
  void* m_str;
  int8_t m_type;
  uint32_t m_len;
  mutable int m_hashcode;

 public:
  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const;

  /**
   *@brief deserialize this object
   * Throw IllegalArgumentException if the packed CacheableString is not less
   * than 64K bytes.
   **/
  virtual Serializable* fromData(DataInput& input);

  /** creation function for strings */
  static Serializable* createDeserializable();

  /** creation function for strings > 64K length */
  static Serializable* createDeserializableHuge();

  /** creation function for wide strings */
  static Serializable* createUTFDeserializable();

  /** creation function for wide strings > 64K length in UTF8 encoding */
  static Serializable* createUTFDeserializableHuge();

  /**
   *@brief Return the classId of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and deserialize into.
   */
  virtual int32_t classId() const;

  /**
   * Return the typeId byte of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and deserialize into.
   *
   * For a <code>CacheableString</code> this shall return
   * <code>GemfireTypeIds::CacheableNullString</code> if the underlying
   * string is null, <code>GemfireTypeIds::CacheableASCIIString</code>
   * if the underlying string is a char*, and
   * <code>GemfireTypeIds::CacheableString</code> if it is a wchar_t*.
   * For strings larger than 64K it will return
   * <code>GemfireTypeIds::CacheableASCIIStringHuge</code> and
   * <code>GemfireTypeIds::CacheableStringHuge</code> for char* and wchar_t*
   * respectively.
   */
  virtual int8_t typeId() const;

  /** return true if this key matches other. */
  virtual bool operator==(const CacheableKey& other) const;

  /** return the hashcode for this key. */
  virtual uint32_t hashcode() const;

  /**
   * Factory method for creating an instance of CacheableString from
   * a null terminated C string optionally giving the length.
   *
   * This should be used only for ASCII strings.
   */
  static CacheableStringPtr create(const char* value, int32_t len = 0) {
    CacheableStringPtr str = NULLPTR;
    if (value != NULL) {
      str = new CacheableString();
      str->initString(value, len);
    }
    return str;
  }

  /**
   * Factory method for creating an instance of CacheableString from
   * a C string of given length by taking ownership of the string without
   * making a copy. The string should have been allocated using
   * the standard C++ new operator.
   *
   * This should be used only for ASCII strings.
   *
   * CAUTION: use this only when you really know what you are doing.
   */
  static CacheableStringPtr createNoCopy(char* value, int32_t len = 0) {
    CacheableStringPtr str = NULLPTR;
    if (value != NULL) {
      str = new CacheableString();
      str->initStringNoCopy(value, len);
    }
    return str;
  }

  /**
   * Factory method for creating an instance of CacheableString from a
   * wide-character null terminated C string optionally giving the length.
   *
   * This should be used for non-ASCII strings.
   */
  static CacheableStringPtr create(const wchar_t* value, int32_t len = 0) {
    CacheableStringPtr str = NULLPTR;
    if (value != NULL) {
      str = new CacheableString();
      str->initString(value, len);
    }
    return str;
  }

  /**
   * Factory method for creating an instance of CacheableString from a
   * wide-character C string of given length by taking ownership of the
   * string without making a copy. The string should have been allocated
   * using the standard C++ new operator.
   *
   * This should be used for non-ASCII strings.
   *
   * CAUTION: use this only when you really know what you are doing.
   */
  static CacheableStringPtr createNoCopy(wchar_t* value, int32_t len = 0) {
    CacheableStringPtr str = NULLPTR;
    if (value != NULL) {
      str = new CacheableString();
      str->initStringNoCopy(value, len);
    }
    return str;
  }

  /** Returns true if the underlying string is a normal C string. */
  inline bool isCString() const {
    return (m_type == GF_STRING || m_type == GF_STRING_HUGE);
  }

  /** Returns true if the underlying string is a wide-character string. */
  inline bool isWideString() const {
    return (m_type == GF_WIDESTRING || m_type == GF_WIDESTRING_HUGE);
  }

  /**
   * Return the string that backs this CacheableString as a char *. This
   * shall throw an exception if the underlying string is a wchar_t* --
   * the caller should use <code>typeId</code> to determine the actual type,
   * or <code>isWideString</code> to find whether this is a wide-character
   * string.
   *
   * @throws IllegalStateException if the underlying string is a wchar_t *
   */
  const char* asChar() const {
    if (isWideString()) {
      throw IllegalStateException(
          "CacheableString::asChar: the string is a "
          "wide character string; use asWChar() to obtain it.");
    }
    return (const char*)m_str;
  }

  /**
   * Return the string that backs this CacheableString as a wchar_t *. This
   * shall throw an exception if the underlying string is a char* --
   * the caller should use <code>typeId</code> to determine the actual type,
   * or <code>isWideString</code> to find whether this is indeed a
   * wide-character string.
   *
   * @throws IllegalStateException if the underlying string is a char *
   */
  const wchar_t* asWChar() const {
    if (isCString()) {
      throw IllegalStateException(
          "CacheableString::asWChar: the string is "
          "not a wide character string; use asChar() to obtain it.");
    }
    return (const wchar_t*)m_str;
  }

  /** Return the length of the contained string. */
  inline uint32_t length() const { return m_len; }

  /**
   * Display this object as c string. In this case, it returns the same
   * value as asChar() when underlying type is a char* and returns the same
   * value as asWChar() cast to char* when the underlying type is a wchar_t*.
   * To handle this correctly the user should find the actual type by calling
   * typeId() or isWideString() and cast to the correct type accordingly.
   * Note: this is a debugging API, not intended for getting the exact value
   * of the CacheableString. In a future release this may return a more
   * summary representation. This is historical. It is preferred that the
   * user call logString or asChar/asWChar, depending on the need.
   */
  const char* toString() { return (const char*)m_str; }

  virtual CacheableStringPtr toString() const {
    return CacheableStringPtr(this);
  }

  /** get the name of the class of this object for logging purpose */
  virtual const char* className() const { return "CacheableString"; }

  /** Destructor */
  virtual ~CacheableString();

  /** used to render as a string for logging. */
  virtual int32_t logString(char* buffer, int32_t maxLength) const;

  virtual uint32_t objectSize() const;

 protected:
  /** Private method to populate the <code>CacheableString</code>. */
  void copyString(const char* value, int32_t len);
  /** Private method to populate the <code>CacheableString</code>. */
  void copyString(const wchar_t* value, int32_t len);
  /** initialize the string, given a value and length. */
  void initString(const char* value, int32_t len);
  /**
   * Initialize the string without making a copy, given a C string
   * and length.
   */
  void initStringNoCopy(char* value, int32_t len);
  /** initialize the string, given a wide-char string and length. */
  void initString(const wchar_t* value, int32_t len);
  /**
   * Initialize the string without making a copy, given a wide-char string
   * and length.
   */
  void initStringNoCopy(wchar_t* value, int32_t len);
  /** Private method to get ASCII string for wide-string if possible. */
  char* getASCIIString(const wchar_t* value, int32_t& len, int32_t& encodedLen);
  /** Default constructor. */
  inline CacheableString(int8_t type = GF_STRING)
      : m_str(NULL), m_type(type), m_len(0), m_hashcode(0) {}

 private:
  // never implemented.
  void operator=(const CacheableString& other);
  CacheableString(const CacheableString& other);
};

/** overload of gemfire::createKeyArr to pass char* */
inline CacheableKeyPtr createKeyArr(const char* value) {
  return (value != NULL ? CacheableKeyPtr(CacheableString::create(value).ptr())
                        : NULLPTR);
}

/** overload of gemfire::createKeyArr to pass wchar_t* */
inline CacheableKeyPtr createKeyArr(const wchar_t* value) {
  return (value != NULL ? CacheableKeyPtr(CacheableString::create(value).ptr())
                        : NULLPTR);
}

/** overload of gemfire::createValueArr to pass char* */
inline CacheablePtr createValueArr(const char* value) {
  return (value != NULL ? CacheablePtr(CacheableString::create(value).ptr())
                        : NULLPTR);
}

/** overload of gemfire::createValueArr to pass wchar_t* */
inline CacheablePtr createValueArr(const wchar_t* value) {
  return (value != NULL ? CacheablePtr(CacheableString::create(value).ptr())
                        : NULLPTR);
}
}

#endif
