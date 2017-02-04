#pragma once

#ifndef GEODE_GFCPP_PDXSERIALIZABLE_H_
#define GEODE_GFCPP_PDXSERIALIZABLE_H_

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

namespace apache {
namespace geode {
namespace client {

typedef PdxSerializable* (*PdxTypeFactoryMethod)();

class CPPCACHE_EXPORT PdxSerializable : public CacheableKey {
 public:
  PdxSerializable();
  virtual ~PdxSerializable();

  // for virtual overloads bring base toData/fromData in scope otherwise
  // child classes won't be able to override as desired
  // Solaris compiler gives "hides the virtual function" warnings when
  // compiling child classes while other compilers silently
  // accept but will cause problems with overloaded calls (in this case
  //   no implicit conversion from PdxWriterPtr to DataOutput etc exists
  //   so no imminent danger)
  // see
  // http://www.oracle.com/technetwork/server-storage/solarisstudio/documentation/cplusplus-faq-355066.html#Coding1
  // using Serializable::toData;
  // using Serializable::fromData;

  /**
   *@brief serialize this object in geode PDX format
   *@param PdxWriter to serialize the PDX object
   **/
  virtual void toData(PdxWriterPtr output) /*const*/ = 0;

  /**
   *@brief Deserialize this object
   *@param PdxReader to Deserialize the PDX object
   **/
  virtual void fromData(PdxReaderPtr input) = 0;

  /**
   *@brief return the typeId byte of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and deserialize into.
   *
   * Note that this should not be overridden by custom implementations
   * and is reserved only for builtin types.
   */
  virtual int8_t typeId() const;

  /** return true if this key matches other. */
  virtual bool operator==(const CacheableKey& other) const;

  /** return the hashcode for this key. */
  virtual uint32_t hashcode() const;

  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const;

  /**
   *@brief deserialize this object, typical implementation should return
   * the 'this' pointer.
   **/
  virtual Serializable* fromData(DataInput& input);

  /**
   *@brief return the classId of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and derserialize into.
   */
  virtual int32_t classId() const { return 0x10; }

  /**
   * Display this object as 'string', which depends on the implementation in
   * the subclasses.
   * The default implementation renders the classname.
   *
   * The return value may be a temporary, so the caller has to ensure that
   * the SharedPtr count does not go down to zero by storing the result
   * in a variable or otherwise.
   */
  virtual CacheableStringPtr toString() const;

  /**
   * Get the Type for the Object. Equivalent to the C# Type->GetType() API.
   */
  virtual const char* getClassName() const = 0;
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_PDXSERIALIZABLE_H_
