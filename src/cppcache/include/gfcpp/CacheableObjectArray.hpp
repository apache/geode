#ifndef _GEMFIRE_CACHEABLEOBJECTARRAY_HPP_
#define _GEMFIRE_CACHEABLEOBJECTARRAY_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "VectorT.hpp"

/** @file
 */

namespace gemfire {

/**
 * Implement an immutable Vector of <code>Cacheable</code> objects
 * that can serve as a distributable object for caching.
 */
class CPPCACHE_EXPORT CacheableObjectArray : public Cacheable,
                                             public _VectorOfCacheable {
 public:
  typedef _VectorOfCacheable::Iterator Iterator;

  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const;

  /**
   *@brief deserialize this object
   **/
  virtual Serializable* fromData(DataInput& input);

  /**
   * @brief creation function for java Object[]
   */
  inline static Serializable* createDeserializable() {
    return new CacheableObjectArray();
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
   * Factory method for creating the default instance of CacheableObjectArray.
   */
  inline static CacheableObjectArrayPtr create() {
    return CacheableObjectArrayPtr(new CacheableObjectArray());
  }

  /**
   * Factory method for creating an instance of CacheableObjectArray with
   * given size.
   */
  inline static CacheableObjectArrayPtr create(int32_t n) {
    return CacheableObjectArrayPtr(new CacheableObjectArray(n));
  }

  virtual uint32_t objectSize() const;

 protected:
  /** Constructor, used for deserialization. */
  inline CacheableObjectArray() : _VectorOfCacheable() {}
  /** Create a vector with n elements allocated. */
  inline CacheableObjectArray(int32_t n) : _VectorOfCacheable(n) {}

 private:
  // never implemented.
  CacheableObjectArray& operator=(const CacheableObjectArray& other);
  CacheableObjectArray(const CacheableObjectArray& other);
};
}

#endif  // _GEMFIRE_CACHEABLEOBJECTARRAY_HPP_
