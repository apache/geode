#pragma once

#ifndef GEODE_GFCPP_CACHEABLEOBJECTARRAY_H_
#define GEODE_GFCPP_CACHEABLEOBJECTARRAY_H_

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

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "VectorT.hpp"

/** @file
 */

namespace apache {
namespace geode {
namespace client {

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
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CACHEABLEOBJECTARRAY_H_
