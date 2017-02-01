#pragma once

#ifndef GEODE_GFCPP_HASHFUNCTION_H_
#define GEODE_GFCPP_HASHFUNCTION_H_

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
#include "SharedPtr.hpp"
#include "CacheableKey.hpp"

/** @file
 */

namespace apache {
namespace geode {
namespace client {

/** typedef for the hash function used by the hashing schemes. */
typedef int32_t (*Hasher)(const SharedBasePtr&);

/** typedef for the hashing key equality function. */
typedef bool (*EqualTo)(const SharedBasePtr&, const SharedBasePtr&);

class HashSB {
 private:
  Hasher m_hashFn;

  // Never defined.
  HashSB();

 public:
  HashSB(const Hasher hashFn) : m_hashFn(hashFn) {}

  int32_t operator()(const SharedBasePtr& p) const { return m_hashFn(p); }
};

class EqualToSB {
 private:
  EqualTo m_equalFn;

  // Never defined.
  EqualToSB();

 public:
  EqualToSB(const EqualTo equalFn) : m_equalFn(equalFn) {}

  bool operator()(const SharedBasePtr& x, const SharedBasePtr& y) const {
    return m_equalFn(x, y);
  }
};

template <typename TKEY>
inline int32_t hashFunction(const TKEY& k) {
  return k->hashcode();
}

template <typename TKEY>
inline bool equalToFunction(const TKEY& x, const TKEY& y) {
  return (*x.ptr() == *y.ptr());
}
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_HASHFUNCTION_H_
