#ifndef _GEMFIRE_CACHEABLEUNDEFINED_HPP_
#define _GEMFIRE_CACHEABLEUNDEFINED_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "Cacheable.hpp"

/** @file
 */

namespace gemfire {

/**
 * Encapsulate an undefined query result.
 */
class CPPCACHE_EXPORT CacheableUndefined : public Cacheable {
 public:
  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const;

  /**
   *@brief deserialize this object
   **/
  virtual Serializable* fromData(DataInput& input);

  /**
   * @brief creation function for undefined query result
   */
  inline static Serializable* createDeserializable() {
    return new CacheableUndefined();
  }

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
   * @brief Return the data serialization fixed ID size type for internal use.
   * @since GFE 5.7
   */
  virtual int8_t DSFID() const;

  /**
   * Factory method for creating the default instance of CacheableUndefined.
   */
  inline static CacheableUndefinedPtr create() {
    return CacheableUndefinedPtr(new CacheableUndefined());
  }

  virtual uint32_t objectSize() const;

 protected:
  /** Constructor, used for deserialization. */
  inline CacheableUndefined() {}

 private:
  // never implemented.
  CacheableUndefined& operator=(const CacheableUndefined& other);
  CacheableUndefined(const CacheableUndefined& other);
};
}

#endif  // _GEMFIRE_CACHEABLEUNDEFINED_HPP_
