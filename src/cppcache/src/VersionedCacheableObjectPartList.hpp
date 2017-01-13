#ifndef _GEMFIRE_VERSIONEDCACHEABLEOBJECTPARTLIST_HPP_
#define _GEMFIRE_VERSIONEDCACHEABLEOBJECTPARTLIST_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/*
#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/gf_types.hpp>
#include <gfcpp/DataOutput.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/Cacheable.hpp>
#include <gfcpp/VectorT.hpp>
#include <gfcpp/HashMapT.hpp>
#include "MapWithLock.hpp"
*/
#include "CacheableObjectPartList.hpp"
#include "VersionTag.hpp"
//#include "DiskVersionTag.hpp"
#include <ace/Task.h>
#include <vector>

/** @file
 */

namespace gemfire {
class ThinClientRegion;

/**
 * Implement an immutable list of object parts that encapsulates an object,
 * a raw byte array or a java exception object. Optionally can also store
 * the keys corresponding to those objects. This is used for reading the
 * results of a GET_ALL/PUTALL request or initial values of register interest.
 *
 * Also see GetAll.ObjectPartList on java side.
 *
 *
 */

class VersionedCacheableObjectPartList;
typedef SharedPtr<VersionedCacheableObjectPartList>
    VersionedCacheableObjectPartListPtr;

class VersionedCacheableObjectPartList : public CacheableObjectPartList {
 private:
  bool m_regionIsVersioned;
  bool m_serializeValues;
  bool m_hasTags;
  bool m_hasKeys;
  std::vector<VersionTagPtr> m_versionTags;
  std::vector<uint8> m_byteArray;
  uint16_t m_endpointMemId;
  VectorOfCacheableKeyPtr m_tempKeys;
  ACE_Recursive_Thread_Mutex& m_responseLock;

  static const uint8_t FLAG_NULL_TAG;
  static const uint8_t FLAG_FULL_TAG;
  static const uint8_t FLAG_TAG_WITH_NEW_ID;
  static const uint8_t FLAG_TAG_WITH_NUMBER_ID;

  void readObjectPart(int32_t index, DataInput& input, CacheableKeyPtr keyPtr);
  // never implemented.
  VersionedCacheableObjectPartList& operator=(
      const VersionedCacheableObjectPartList& other);
  VersionedCacheableObjectPartList(
      const VersionedCacheableObjectPartList& other);
  /*inline VersionedCacheableObjectPartList() : m_responseLock()
  {
        m_regionIsVersioned = false;
        m_serializeValues = false;
        m_endpointMemId = 0;
        GF_NEW(m_tempKeys, VectorOfCacheableKey);
  }*/

 public:
  VersionedCacheableObjectPartList(const VectorOfCacheableKey* keys,
                                   uint32_t* keysOffset,
                                   const HashMapOfCacheablePtr& values,
                                   const HashMapOfExceptionPtr& exceptions,
                                   const VectorOfCacheableKeyPtr& resultKeys,
                                   ThinClientRegion* region,
                                   MapOfUpdateCounters* trackerMap,
                                   int32_t destroyTracker, bool addToLocalCache,
                                   uint16_t m_dsmemId,
                                   ACE_Recursive_Thread_Mutex& responseLock)
      : CacheableObjectPartList(keys, keysOffset, values, exceptions,
                                resultKeys, region, trackerMap, destroyTracker,
                                addToLocalCache),
        m_responseLock(responseLock) {
    m_regionIsVersioned = false;
    m_serializeValues = false;
    m_endpointMemId = m_dsmemId;
    GF_NEW(m_tempKeys, VectorOfCacheableKey);
    m_hasTags = false;
    m_hasKeys = false;
  }

  VersionedCacheableObjectPartList(VectorOfCacheableKey* keys,
                                   int32_t totalMapSize,
                                   ACE_Recursive_Thread_Mutex& responseLock)
      : m_responseLock(responseLock) {
    m_regionIsVersioned = false;
    m_serializeValues = false;
    m_hasTags = false;
    m_endpointMemId = 0;
    m_versionTags.resize(totalMapSize);
    this->m_hasKeys = false;
    this->m_tempKeys = VectorOfCacheableKeyPtr(keys);
  }

  VersionedCacheableObjectPartList(VectorOfCacheableKey* keys,
                                   ACE_Recursive_Thread_Mutex& responseLock)
      : m_responseLock(responseLock) {
    m_regionIsVersioned = false;
    m_serializeValues = false;
    m_hasTags = false;
    m_endpointMemId = 0;
    this->m_hasKeys = false;
    this->m_tempKeys = VectorOfCacheableKeyPtr(keys);
  }

  VersionedCacheableObjectPartList(ACE_Recursive_Thread_Mutex& responseLock)
      : m_responseLock(responseLock) {
    m_regionIsVersioned = false;
    m_serializeValues = false;
    m_hasTags = false;
    m_endpointMemId = 0;
    this->m_hasKeys = false;
  }

  /*inline VersionedCacheableObjectPartList(bool serializeValues)
  {
      m_serializeValues = serializeValues;
    GF_NEW(m_tempKeys, VectorOfCacheableKey);

  }*/

  inline uint16_t getEndpointMemId() { return m_endpointMemId; }

  std::vector<VersionTagPtr>& getVersionedTagptr() { return m_versionTags; }

  void setVersionedTagptr(std::vector<VersionTagPtr>& versionTags) {
    m_versionTags = versionTags;
    m_hasTags = (m_versionTags.size() > 0);
  }

  int getVersionedTagsize() const { return (int)(m_versionTags.size()); }

  VectorOfCacheableKeyPtr getSucceededKeys() { return m_tempKeys; }

  inline VersionedCacheableObjectPartList(
      uint16_t endpointMemId, ACE_Recursive_Thread_Mutex& responseLock)
      : m_responseLock(responseLock) {
    m_regionIsVersioned = false;
    m_serializeValues = false;
    m_endpointMemId = endpointMemId;
    GF_NEW(m_tempKeys, VectorOfCacheableKey);
    m_hasTags = false;
    m_hasKeys = false;
  }

  void addAll(VersionedCacheableObjectPartListPtr other) {
    // LOGDEBUG("DEBUG:: COPL.addAll called");
    // ACE_Guard< ACE_Recursive_Thread_Mutex > guard( this->m_responseLock );
    if (other->m_tempKeys != NULLPTR) {
      if (this->m_tempKeys == NULLPTR) {
        this->m_tempKeys = new VectorOfCacheableKey();
        this->m_hasKeys = true;
        int size = other->m_tempKeys->size();
        for (int i = 0; i < size; i++) {
          this->m_tempKeys->push_back(other->m_tempKeys->at(i));
        }
      } else {
        if (this->m_tempKeys != NULLPTR) {
          if (!this->m_hasKeys) {
            LOGDEBUG(" VCOPL::addAll m_hasKeys should be true here");
            this->m_hasKeys = true;
          }
          int size = other->m_tempKeys->size();
          for (int i = 0; i < size; i++) {
            this->m_tempKeys->push_back(other->m_tempKeys->at(i));
          }
        }
      }
    }

    // set m_regionIsVersioned
    this->m_regionIsVersioned |= other->m_regionIsVersioned;
    size_t size = other->m_versionTags.size();
    LOGDEBUG(" VCOPL::addAll other->m_versionTags.size() = %d ", size);
    // Append m_versionTags
    if (size > 0) {
      for (size_t i = 0; i < size; i++) {
        this->m_versionTags.push_back(other->m_versionTags[i]);
      }
      m_hasTags = true;
    }
  }

  int size() {
    if (this->m_hasKeys) {
      return this->m_tempKeys->size();
    } else if (this->m_hasTags) {
      return (int)this->m_versionTags.size();
    } else {
      LOGDEBUG(
          "DEBUG:: Should not call VCOPL.size() if hasKeys and hasTags both "
          "are false.!!");
    }
    return -1;
  }

  void addAllKeys(VectorOfCacheableKeyPtr keySet) {
    if (!this->m_hasKeys) {
      this->m_hasKeys = true;
      this->m_tempKeys = new VectorOfCacheableKey(*keySet);
    } else {
      for (int i = 0; i < keySet->size(); i++) {
        this->m_tempKeys->push_back(keySet->at(i));
      }
    }
  }

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
    return new VersionedCacheableObjectPartList();
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
}

#endif  // _GEMFIRE_VERSIONEDCACHEABLEOBJECTPARTLIST_HPP_
