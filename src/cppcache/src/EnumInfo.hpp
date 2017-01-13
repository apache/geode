/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef ENUM_INFO_HPP
#define ENUM_INFO_HPP

#include <gfcpp/GemfireTypeIds.hpp>
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/CacheableKey.hpp>

namespace gemfire {

class CPPCACHE_EXPORT EnumInfo : public CacheableKey {
 private:
  CacheableStringPtr m_enumClassName;
  CacheableStringPtr m_enumName;
  int32_t m_ordinal;

 public:
  ~EnumInfo();
  EnumInfo();
  EnumInfo(const char* enumClassName, const char* enumName, int32_t m_ordinal);
  static Serializable* createDeserializable() { return new EnumInfo(); }
  virtual void toData(DataOutput& output) const;
  virtual Serializable* fromData(DataInput& input);
  virtual uint32_t objectSize() const {
    uint32_t size = sizeof(EnumInfo);
    size += sizeof(int32_t);
    size += m_enumClassName->objectSize();
    size += m_enumName->objectSize();
    return size;
  }
  virtual int32_t classId() const { return 0; }
  virtual int8_t typeId() const { return GemfireTypeIds::EnumInfo; }
  virtual CacheableStringPtr toString() const {
    return CacheableString::create("EnumInfo");
  }
  virtual bool operator==(const CacheableKey& other) const;
  virtual uint32_t hashcode() const;

  virtual int8_t DSFID() const;
  CacheableStringPtr getEnumClassName() const { return m_enumClassName; }
  CacheableStringPtr getEnumName() const { return m_enumName; }
  int32_t getEnumOrdinal() const { return m_ordinal; }
};
typedef SharedPtr<EnumInfo> EnumInfoPtr;
}

#endif  // ENUM_INFO_HPP
