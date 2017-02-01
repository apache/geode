#pragma once

#ifndef GEODE_GFCPP_CACHEABLEENUM_H_
#define GEODE_GFCPP_CACHEABLEENUM_H_

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

#include "CacheableKey.hpp"
#include "CacheableString.hpp"

namespace apache {
namespace geode {
namespace client {

/**
* Since C++ enums cannot be directly passed as a parameter to PdxWriter's
* writeObject and PdxReader's readObject api
* wrap C++ enum in to a immutable wrapper CacheableEnum class type by specifying
* enum class name, enum value name and its ordinal.
* C++ enum allows explicit setting of ordinal number, but it is up to the user
* to map java enumName with that of C++ enumName.
* Currently this wrapper only works as part of PdxSerializable member object and
* cannot be directly used in Region operations.
*
* @see PdxWriter#writeObject
* @see PdxReader#readObject
*/

class CPPCACHE_EXPORT CacheableEnum : public CacheableKey {
 private:
  CacheableStringPtr m_enumClassName;
  CacheableStringPtr m_enumName;
  int32_t m_ordinal;
  mutable int32_t m_hashcode;

 public:
  /** Destructor */
  ~CacheableEnum();

  /**
  * @brief creation function for enum.
  */
  static Serializable* createDeserializable() { return new CacheableEnum(); }
  /**
  * @brief serialize this object
  **/
  virtual void toData(DataOutput& output) const;

  /**
  * @brief deserialize this object
  **/
  virtual Serializable* fromData(DataInput& input);

  /** @return the size of the object in bytes */
  virtual uint32_t objectSize() const {
    uint32_t size = sizeof(CacheableEnum);
    size += static_cast<uint32_t>(sizeof(int32_t));
    size += m_enumClassName->objectSize();
    size += m_enumName->objectSize();
    return size;
  }

  /**
  * @brief Return the classId of the instance being serialized.
  * This is used by deserialization to determine what instance
  * type to create and deserialize into.
  */
  virtual int32_t classId() const { return 0; }

  /**
  * @brief return the typeId byte of the instance being serialized.
  * This is used by deserialization to determine what instance
  * type to create and deserialize into.
  */
  virtual int8_t typeId() const {
    // return 0;
    return static_cast<int8_t>(GeodeTypeIds::CacheableEnum);
  }

  /**
  * Display this object as c string.
  */
  virtual CacheableStringPtr toString() const {
    return CacheableString::create("CacheableEnum");
  }

  /**
  * Factory method for creating an instance of CacheableEnum.
  * @param className the name of the enum class that maps to the java enum type.
  * @param enumName the name of the enum constant that maps to the java enum
  * type.
  * @param ordinal the ordinal value of the enum constant that maps to the java
  * enum type.
  * @return a {@link CacheableEnum} representing C++ enum.
  */
  static CacheableEnumPtr create(const char* enumClassName,
                                 const char* enumName, int32_t ordinal) {
    CacheableEnumPtr str(new CacheableEnum(enumClassName, enumName, ordinal));
    return str;
  }

  /**@return enum class name. */
  const char* getEnumClassName() const { return m_enumClassName->asChar(); }

  /**@return enum name. */
  const char* getEnumName() const { return m_enumName->asChar(); }

  /**@return enum ordinal. */
  int32_t getEnumOrdinal() const { return m_ordinal; }

  /** @return the hashcode for this key. */
  virtual uint32_t hashcode() const;

  /** @return true if this key matches other. */
  virtual bool operator==(const CacheableKey& other) const;

 protected:
  CacheableEnum();
  CacheableEnum(const char* enumClassName, const char* enumName,
                int32_t ordinal);

 private:
  // never implemented.
  void operator=(const CacheableEnum& other);
  CacheableEnum(const CacheableEnum& other);
};
}  // namespace client
}  // namespace geode
}  // namespace apache


#endif // GEODE_GFCPP_CACHEABLEENUM_H_
