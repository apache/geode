#ifndef _GEMFIRE_CACHEABLEOBJECTPARTLIST_HPP_
#define _GEMFIRE_CACHEABLEOBJECTPARTLIST_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/Cacheable.hpp>
#include <gfcpp/VectorT.hpp>
#include <gfcpp/HashMapT.hpp>
#include "MapWithLock.hpp"

#include <vector>

/** @file
 */

namespace gemfire {
class ThinClientRegion;

/**
 * Implement an immutable list of object parts that encapsulates an object,
 * a raw byte array or a java exception object. Optionally can also store
 * the keys corresponding to those objects. This is used for reading the
 * results of a GET_ALL request or initial values of register interest.
 *
 * Also see GetAll.ObjectPartList on java side.
 *
 *
 */
class CacheableObjectPartList : public Cacheable {
 protected:
  const VectorOfCacheableKey* m_keys;
  uint32_t* m_keysOffset;
  HashMapOfCacheablePtr m_values;
  HashMapOfExceptionPtr m_exceptions;
  VectorOfCacheableKeyPtr m_resultKeys;
  ThinClientRegion* m_region;
  MapOfUpdateCounters* m_updateCountMap;
  int32_t m_destroyTracker;
  bool m_addToLocalCache;

  inline CacheableObjectPartList()
      : m_keys(NULL),
        m_keysOffset(NULL),
        m_values(NULLPTR),
        m_exceptions(NULLPTR),
        m_resultKeys(NULLPTR),
        m_region(NULL),
        m_updateCountMap(NULL),
        m_destroyTracker(0),
        m_addToLocalCache(false) {}

  // never implemented.
  CacheableObjectPartList& operator=(const CacheableObjectPartList& other);
  CacheableObjectPartList(const CacheableObjectPartList& other);

 public:
  /**
   * @brief constructor given the list of keys and a map of values,
   *        map of exceptions and region to populate with the values
   *        obtained in fromData
   */
  CacheableObjectPartList(const VectorOfCacheableKey* keys,
                          uint32_t* keysOffset,
                          const HashMapOfCacheablePtr& values,
                          const HashMapOfExceptionPtr& exceptions,
                          const VectorOfCacheableKeyPtr& resultKeys,
                          ThinClientRegion* region,
                          MapOfUpdateCounters* trackerMap,
                          int32_t destroyTracker, bool addToLocalCache)
      : m_keys(keys),
        m_keysOffset(keysOffset),
        m_values(values),
        m_exceptions(exceptions),
        m_resultKeys(resultKeys),
        m_region(region),
        m_updateCountMap(trackerMap),
        m_destroyTracker(destroyTracker),
        m_addToLocalCache(addToLocalCache) {}

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
  /*inline static Serializable* createDeserializable()
  {
    return new CacheableObjectPartList();
  }*/

  /**
   *@brief Return the classId byte of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and derserialize into.
   */
  virtual int32_t classId() const;

  /**
   *@brief return the typeId byte of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and derserialize into.
   */
  virtual int8_t typeId() const;

  /**
   * Return the data serializable fixed ID size type for internal use.
   * @since GFE 5.7
   */
  virtual int8_t DSFID() const;

  virtual uint32_t objectSize() const;
};

typedef SharedPtr<CacheableObjectPartList> CacheableObjectPartListPtr;
}

#endif  // _GEMFIRE_CACHEABLEOBJECTPARTLIST_HPP_
