#ifndef _GEMFIRE_HASHFUNCTION_HPP_
#define _GEMFIRE_HASHFUNCTION_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "SharedPtr.hpp"
#include "CacheableKey.hpp"

/** @file
 */

namespace gemfire {

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
}

#endif
