#ifndef _GEMFIRE_HASHMAPT_HPP_
#define _GEMFIRE_HASHMAPT_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "HashMapOfSharedBase.hpp"
#include "Cacheable.hpp"
#include "CacheableKey.hpp"
#include "Exception.hpp"

/** @file
*/

namespace gemfire {

/** HashMap of <code>TKEY</code> to <code>TVAL</code>. */
template <typename TKEY, typename TVAL>
class HashMapT {
 private:
  HashMapOfSharedBase m_map;

 public:
  class Iterator {
   private:
    HashMapOfSharedBase::Iterator m_iter;

    inline Iterator(const HashMapOfSharedBase::Iterator& iter) : m_iter(iter) {}

    // Never defined.
    Iterator();

   public:
    inline const TKEY first() const { return staticCast<TKEY>(m_iter.first()); }

    inline const TVAL second() const {
      return staticCast<TVAL>(m_iter.second());
    }

    inline Iterator& operator++() {
      ++m_iter;
      return *this;
    }

    inline void operator++(int) { m_iter++; }

    inline bool operator==(const Iterator& other) const {
      return (m_iter == other.m_iter);
    }

    inline bool operator!=(const Iterator& other) const {
      return (m_iter != other.m_iter);
    }

    friend class HashMapT;
  };

  static int32_t hasher(const SharedBasePtr& p) {
    return gemfire::hashFunction<TKEY>(staticCast<TKEY>(p));
  }

  static bool equal_to(const SharedBasePtr& x, const SharedBasePtr& y) {
    return gemfire::equalToFunction<TKEY>(staticCast<TKEY>(x),
                                          staticCast<TKEY>(y));
  }

  /** Returns the size of the hash map. */
  inline int32_t size() const { return static_cast<int32_t>(m_map.size()); }

  /** Returns the largest possible size of the hash map. */
  inline int32_t max_size() const {
    return static_cast<int32_t>(m_map.max_size());
  }

  /** true if the hash map's size is 0. */
  inline bool empty() const { return m_map.empty(); }

  /** Returns the number of buckets used by the hash map. */
  inline int32_t bucket_count() const {
    return static_cast<int32_t>(m_map.bucket_count());
  }

  /** Increases the bucket count to at least n. */
  inline void resize(int32_t n) { m_map.resize(n); }

  /** Swaps the contents of two hash maps. */
  inline void swap(HashMapT& other) { m_map.swap(other.m_map); }

  /** Inserts the <k, v> pair into the hash map,
   * when k does not exist in the hash map.
   */
  inline bool insert(const TKEY& k, const TVAL& v) {
    return m_map.insert(k, v);
  }

  /** Updates a value whose key must exist. */
  inline void update(const TKEY& k, const TVAL& v) { m_map[k] = v; }

  /** Erases the element whose key is k. */
  inline int32_t erase(const TKEY& k) { return m_map.erase(k); }

  /** Erases all of the elements. */
  inline void clear() { m_map.clear(); }

  /** Check if a given key k exists in the hash map. */
  inline bool contains(const TKEY& k) const { return m_map.contains(k); }

  /** Finds an element whose key is k. */
  inline Iterator find(const TKEY& k) const { return Iterator(m_map.find(k)); }

  /** Counts the number of elements whose key is k. */
  int32_t count(const SharedBasePtr& k) const { return m_map.count(k); }

  /** Returns a copy of the object that is associated
   * with a particular key.
   */
  inline TVAL operator[](const TKEY& k) { return staticCast<TVAL>(m_map[k]); }

  /** Get an iterator pointing to the start of hash_map. */
  inline Iterator begin() const { return Iterator(m_map.begin()); }

  /** Get an iterator pointing to the end of hash_map. */
  inline Iterator end() const { return Iterator(m_map.end()); }

  /** Assignment operator. */
  inline HashMapT& operator=(const HashMapT& other) {
    m_map = other.m_map;
    return *this;
  }

  /** Creates an empty hash map with hash function
   * hasher<TKEY> and equal to function equal_to<TKEY>.
   */
  inline HashMapT() : m_map(hasher, equal_to) {}

  /** Creates an empty hash map with at least n buckets and
   * hash function hasher<TKEY> and equal to function equal_to<TKEY>.
   */
  inline HashMapT(int32_t n) : m_map(n, hasher, equal_to) {}

  /** Copy constructor. */
  inline HashMapT(const HashMapT& other) : m_map(other.m_map) {}

  /** Destructor: the destructor of m_map would do required stuff. */
  inline ~HashMapT() {}
};

typedef HashMapT<CacheableKeyPtr, CacheablePtr> _HashMapOfCacheable;
typedef HashMapT<CacheableKeyPtr, ExceptionPtr> _HashMapOfException;

/**
 * A map of <code>CacheableKey</code> objects to <code>Cacheable</code>
 * that also extends <code>SharedBase</code> for smart pointers.
 */
class CPPCACHE_EXPORT HashMapOfCacheable : public _HashMapOfCacheable,
                                           public SharedBase {
 public:
  /** Iterator class for the hash map. */
  typedef _HashMapOfCacheable::Iterator Iterator;

  /** Creates an empty hash map. */
  inline HashMapOfCacheable() : _HashMapOfCacheable() {}

  /** Creates an empty hash map with at least n buckets. */
  inline HashMapOfCacheable(int32_t n) : _HashMapOfCacheable(n) {}

  /** Copy constructor. */
  inline HashMapOfCacheable(const HashMapOfCacheable& other)
      : _HashMapOfCacheable(other) {}

 private:
  const HashMapOfCacheable& operator=(const HashMapOfCacheable&);
};

/**
 * A map of <code>CacheableKey</code> objects to <code>Exception</code>
 * that also extends <code>SharedBase</code> for smart pointers.
 */
class CPPCACHE_EXPORT HashMapOfException : public _HashMapOfException,
                                           public SharedBase {
 public:
  /** Iterator class for the hash map. */
  typedef _HashMapOfException::Iterator Iterator;

  /** Creates an empty hash map. */
  inline HashMapOfException() : _HashMapOfException() {}

  /** Creates an empty hash map with at least n buckets. */
  inline HashMapOfException(int32_t n) : _HashMapOfException(n) {}

  /** Copy constructor. */
  inline HashMapOfException(const HashMapOfException& other)
      : _HashMapOfException(other) {}

 private:
  const HashMapOfException& operator=(const HashMapOfException&);
};

typedef SharedPtr<HashMapOfCacheable> HashMapOfCacheablePtr;
typedef SharedPtr<HashMapOfException> HashMapOfExceptionPtr;
}

#endif
