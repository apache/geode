/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef CACHEDDESERIALIZABLEHELPER_HPP_
#define CACHEDDESERIALIZABLEHELPER_HPP_

#include <gfcpp/Cacheable.hpp>
#include <gfcpp/DataInput.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include "GemfireTypeIdsImpl.hpp"

namespace gemfire {
/**
 * Helper class to deserialize bytes received from GFE in the form of
 * PREFER_BYTES_CACHED_DESERIALIZABLE = -65, or
 * VM_CACHED_DESERIALIZABLE = -64.
 *
 *
 */
class CachedDeserializableHelper : public Cacheable,
                                   private NonCopyable,
                                   private NonAssignable {
 private:
  int8_t m_typeId;
  SerializablePtr m_intermediate;

  // default constructor disabled
  CachedDeserializableHelper();
  CachedDeserializableHelper(int8_t typeId) : m_typeId(typeId) {}

 public:
  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const {
    throw IllegalStateException(
        "CachedDeserializableHelper::toData should never have been called.");
  }

  /**
   *@brief deserialize this object
   **/
  virtual Serializable* fromData(DataInput& input) {
    int32_t arrayLen;
    input.readArrayLen(&arrayLen);
    input.readObject(m_intermediate);
    return m_intermediate.ptr();
  }

  /**
   * @brief creation function
   */
  inline static Serializable* createForPreferBytesDeserializable() {
    return new CachedDeserializableHelper(
        GemfireTypeIdsImpl::PreferBytesCachedDeserializable);
  }

  /**
   * @brief creation function
   */
  inline static Serializable* createForVmCachedDeserializable() {
    return new CachedDeserializableHelper(
        GemfireTypeIdsImpl::VmCachedDeserializable);
  }

  /**
   *@brief return the typeId byte of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and derserialize into.
   */
  virtual int8_t typeId() const { return m_typeId; }

  /**
   * Return the data serializable fixed ID size type for internal use.
   * @since GFE 5.7
   */
  virtual int8_t DSFID() const { return GemfireTypeIdsImpl::FixedIDByte; }

  int32_t classId() const { return 0; }
};

typedef SharedPtr<CachedDeserializableHelper> CachedDeserializableHelperPtr;

}  // end namespace gemfire;

#endif  // CACHEDDESERIALIZABLEHELPER_HPP_
