#ifndef __GEMFIRE_CACHEABLEKEY_H__
#define __GEMFIRE_CACHEABLEKEY_H__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "gf_types.hpp"
#include "Cacheable.hpp"

/**
 * @file
 */

namespace gemfire {

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
   * gemfire::createKey may be overloaded. For pointer types (e.g. char*)
   * the method gemfire::createKeyArr may be overloaded.
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
}

#endif
