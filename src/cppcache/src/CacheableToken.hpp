#pragma once

#ifndef GEODE_CACHEABLETOKEN_H_
#define GEODE_CACHEABLETOKEN_H_

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

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/Cacheable.hpp>

namespace apache {
namespace geode {
namespace client {

class CPPCACHE_EXPORT CacheableToken;
typedef SharedPtr<CacheableToken> CacheableTokenPtr;

/** Implement a non-mutable int64_t wrapper that can serve as a distributable
 * key object for cacheing as well as being a 64 bit value. */
class CPPCACHE_EXPORT CacheableToken : public Cacheable {
 private:
  enum TokenType { NOT_USED = 0, INVALID, DESTROYED, OVERFLOWED, TOMBSTONE };

  TokenType m_value;

  static CacheableTokenPtr* invalidToken;
  static CacheableTokenPtr* destroyedToken;
  static CacheableTokenPtr* overflowedToken;
  static CacheableTokenPtr* tombstoneToken;

 public:
  inline static CacheableTokenPtr& invalid() { return *invalidToken; }

  inline static CacheableTokenPtr& destroyed() { return *destroyedToken; }

  inline static CacheableTokenPtr& overflowed() { return *overflowedToken; }
  inline static CacheableTokenPtr& tombstone() { return *tombstoneToken; }
  /**
   *@brief serialize this object
   **/
  virtual void toData(DataOutput& output) const;

  /**
   *@brief deserialize this object
   **/
  virtual Serializable* fromData(DataInput& input);

  /**
   * @brief creation function for strings.
   */
  static Serializable* createDeserializable();

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

  virtual ~CacheableToken();

  inline bool isInvalid() { return m_value == INVALID; }

  inline bool isDestroyed() { return m_value == DESTROYED; }

  inline bool isOverflowed() { return m_value == OVERFLOWED; }

  inline bool isTombstone() { return m_value == TOMBSTONE; }

  static bool isToken(const CacheablePtr& ptr) {
    return (*invalidToken == ptr) || (*destroyedToken == ptr) ||
           (*overflowedToken == ptr) || (*tombstoneToken == ptr);
  }

  static bool isInvalid(const CacheablePtr& ptr) {
    return *invalidToken == ptr;
  }

  static bool isDestroyed(const CacheablePtr& ptr) {
    return *destroyedToken == ptr;
  }

  static bool isOverflowed(const CacheablePtr& ptr) {
    return *overflowedToken == ptr;
  }

  static bool isTombstone(const CacheablePtr& ptr) {
    return *tombstoneToken == ptr;
  }
  static void init();

  /**
   * Display this object as 'string', which depend on the implementation in
   * the subclasses. The default implementation renders the classname.
   * This returns constant strings of the form "CacheableToken::INVALID".
   */
  virtual CacheableStringPtr toString() const;

  virtual uint32_t objectSize() const;

 protected:
  CacheableToken(TokenType value);
  CacheableToken();  // used for deserialization.

 private:
  // never implemented.
  void operator=(const CacheableToken& other);
  CacheableToken(const CacheableToken& other);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_CACHEABLETOKEN_H_
