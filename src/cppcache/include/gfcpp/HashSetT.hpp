#ifndef _GEMFIRE_HASHSETT_HPP_
#define _GEMFIRE_HASHSETT_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "HashSetOfSharedBase.hpp"
#include "CacheableKey.hpp"

/** @file
*/

namespace gemfire {

/** HashSet of <code>TKEY</code>. */
template <typename TKEY>
class HashSetT {
 private:
  HashSetOfSharedBase m_set;

 public:
  /** Interface of an iterator for <code>HashSetT</code>.*/
  class Iterator {
   private:
    HashSetOfSharedBase::Iterator m_iter;

    inline Iterator(const HashSetOfSharedBase::Iterator& iter) : m_iter(iter) {}

    // Never defined.
    Iterator();

   public:
    inline const TKEY operator*() const { return staticCast<TKEY>(*m_iter); }

    inline bool isEnd() const { return m_iter.isEnd(); }

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

    inline void reset() { m_iter.reset(); }

    friend class HashSetT;
  };

  inline static int32_t hasher(const SharedBasePtr& p) {
    return gemfire::hashFunction<TKEY>(staticCast<TKEY>(p));
  }

  inline static bool equal_to(const SharedBasePtr& x, const SharedBasePtr& y) {
    return gemfire::equalToFunction<TKEY>(staticCast<TKEY>(x),
                                          staticCast<TKEY>(y));
  }

  /** Returns the size of the hash set. */
  inline int32_t size() const { return m_set.size(); }

  /** Returns the largest possible size of the hash set. */
  inline int32_t max_size() const { return m_set.max_size(); }

  /** true if the hash set's size is 0. */
  inline bool empty() const { return m_set.empty(); }

  /** Returns the number of buckets used by the hash set. */
  inline int32_t bucket_count() const { return m_set.bucket_count(); }

  /** Increases the bucket count to at least n. */
  inline void resize(int32_t n) { m_set.resize(n); }

  /** Swaps the contents of two hash sets. */
  inline void swap(HashSetT& other) { m_set.swap(other.m_set); }

  /** Inserts the key k into the hash set,
   * when k does not exist in the hash set.
   */
  inline bool insert(const TKEY& k) { return m_set.insert(k); }

  /** Erases the element whose key is k. */
  inline int32_t erase(const TKEY& k) { return m_set.erase(k); }

  /** Erases all of the elements. */
  inline void clear() { m_set.clear(); }

  /** Check if a given key k exists in the hash set. */
  inline bool contains(const TKEY& k) const { return m_set.contains(k); }

  /** Counts the number of elements whose key is k. */
  int32_t count(const TKEY& k) const { return m_set.count(k); }

  /** Get an iterator pointing to the start of hash_set. */
  inline Iterator begin() const { return Iterator(m_set.begin()); }

  /** Get an iterator pointing to the end of hash_set. */
  inline Iterator end() const { return Iterator(m_set.end()); }

  /** Assignment operator. */
  inline HashSetT& operator=(const HashSetT& other) {
    m_set = other.m_set;
    return *this;
  }

  /** Creates an empty hash set with hash function
   * hasher<TKEY> and equal to function equal_to<TKEY>.
   */
  inline HashSetT() : m_set(hasher, equal_to) {}

  /** Creates an empty hash set with at least n buckets and
   * hash function hasher<TKEY> and equal to function equal_to<TKEY>.
   */
  inline HashSetT(int32_t n) : m_set(n, hasher, equal_to) {}

  /** Copy constructor. */
  inline HashSetT(const HashSetT& other) : m_set(other.m_set) {}

  /** Destructor: the destructor of m_set would do required stuff. */
  inline ~HashSetT() {}
};

typedef HashSetT<CacheableKeyPtr> _HashSetOfCacheableKey;

/**
 * A hash set of <code>CacheableKey</code> objects that also extends
 * <code>SharedBase</code> for smart pointers.
 */
class CPPCACHE_EXPORT HashSetOfCacheableKey : public _HashSetOfCacheableKey,
                                              public SharedBase {
 public:
  /** Iterator class for the hash set. */
  typedef _HashSetOfCacheableKey::Iterator Iterator;

  /** Create an empty HashSet. */
  inline HashSetOfCacheableKey() : _HashSetOfCacheableKey() {}

  /** Creates an empty hash set with at least n buckets. */
  inline HashSetOfCacheableKey(int32_t n) : _HashSetOfCacheableKey(n) {}

  /** Copy constructor. */
  inline HashSetOfCacheableKey(const HashSetOfCacheableKey& other)
      : _HashSetOfCacheableKey(other) {}

 private:
  const HashSetOfCacheableKey& operator=(const HashSetOfCacheableKey&);
};

typedef SharedPtr<HashSetOfCacheableKey> HashSetOfCacheableKeyPtr;
}

#endif
