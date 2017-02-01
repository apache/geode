#pragma once

#ifndef GEODE_GFCPP_CACHEABLEKEY_H_
#define GEODE_GFCPP_CACHEABLEKEY_H_

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
#include "Cacheable.hpp"

/**
 * @file
 */

namespace apache {
namespace geode {
namespace client {

/** Represents a cacheable key */
class CPPCACHE_EXPORT CacheableKey : public Cacheable {
 protected:
  /** Constructor */
  CacheableKey() : Cacheable() {}

  /** Destructor */
  virtual ~CacheableKey() {}

 public:
  /** return true if this key matches other. */
  virtual bool operator==(const CacheableKey& other) const = 0;

  /** return the hashcode for this key. */
  virtual uint32_t hashcode() const = 0;

  /** Copy the string form of a key into a char* buffer for logging purposes.
   *
   * Implementations should only generate a string as long as maxLength chars,
   * and return the number of chars written. buffer is expected to be large
   * enough to hold at least maxLength chars.
   *
   * The default implementation renders the classname and instance address.
   */
  virtual int32_t logString(char* buffer, int32_t maxLength) const;

  /**
   * Factory method that creates the key type that matches the type of value.
   *
   * For customer defined derivations of CacheableKey, the method
   * apache::geode::client::createKey may be overloaded. For pointer types (e.g.
   * char*)
   * the method apache::geode::client::createKeyArr may be overloaded.
   */
  template <class PRIM>
  inline static CacheableKeyPtr create(const PRIM value);

 private:
  // Never defined.
  CacheableKey(const CacheableKey& other);
  void operator=(const CacheableKey& other);
};

template <class TKEY>
inline CacheableKeyPtr createKey(const SharedPtr<TKEY>& value);

template <typename TKEY>
inline CacheableKeyPtr createKey(const TKEY* value);
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_CACHEABLEKEY_H_
