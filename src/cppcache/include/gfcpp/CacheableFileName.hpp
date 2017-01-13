#ifndef _GEMFIRE_CACHEABLEFILENAME_HPP_
#define _GEMFIRE_CACHEABLEFILENAME_HPP_

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

/** @file
 */

namespace gemfire {
/**
 * Implement an immutable wrapper for filenames that can serve as a
 * distributable filename object for caching as both key and value.
 */
class CPPCACHE_EXPORT CacheableFileName : public CacheableString {
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

  /**
   * @brief creation function for filenames.
   */
  static Serializable* createDeserializable() {
    return new CacheableFileName();
  }

  /**
   * Factory method for creating an instance of CacheableFileName from a
   * C string optionally given the length.
   */
  static CacheableFileNamePtr create(const char* value, int32_t len = 0) {
    CacheableFileNamePtr str = NULLPTR;
    if (value != NULL) {
      str = new CacheableFileName();
      str->initString(value, len);
    }
    return str;
  }

  /**
   * Factory method for creating an instance of CacheableFileName from a
   * wide-character C string optionally given the length.
   */
  static CacheableFileNamePtr create(const wchar_t* value, int32_t len = 0) {
    CacheableFileNamePtr str = NULLPTR;
    if (value != NULL) {
      str = new CacheableFileName();
      str->initString(value, len);
    }
    return str;
  }

  /** get the name of the class of this object for logging purpose */
  virtual const char* className() const { return "CacheableFileName"; }

  /** return the hashcode for this key. */
  virtual uint32_t hashcode() const;

 protected:
  /** Default constructor. */
  inline CacheableFileName() : CacheableString(), m_hashcode(0) {}

 private:
  // never implemented.
  void operator=(const CacheableFileName& other);
  CacheableFileName(const CacheableFileName& other);

 private:
  mutable int m_hashcode;
};
}

#endif
