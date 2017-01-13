
#ifndef _GEMFIRE_VECTORT_HPP_
#define _GEMFIRE_VECTORT_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "VectorOfSharedBase.hpp"
#include "Cacheable.hpp"
#include "CacheableKey.hpp"

/** @file
*/

namespace gemfire {

/** Vector template type class */
template <class PTR_TYPE>
class VectorT {
 private:
  VectorOfSharedBase m_vector;

 public:
  /** Interface of an iterator for <code>VectorT</code>.*/
  class Iterator {
   private:
    VectorOfSharedBase::Iterator m_iter;

    inline Iterator(const VectorOfSharedBase::Iterator& iter) : m_iter(iter) {}

    // Never defined.
    Iterator();

   public:
    inline const PTR_TYPE operator*() const {
      return staticCast<PTR_TYPE>(*m_iter);
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

    friend class VectorT;
  };

  /** return the size of the vector. */
  inline int32_t size() const { return static_cast<int32_t>(m_vector.size()); }

  /** synonym for size. */
  inline int32_t length() const {
    return static_cast<int32_t>(m_vector.size());
  }

  /** return the largest possible size of the vector. */
  inline int32_t max_size() const {
    return static_cast<int32_t>(m_vector.max_size());
  }

  /** return the number of elements allocated for this vector. */
  inline int32_t capacity() const {
    return static_cast<int32_t>(m_vector.capacity());
  }

  /** return true if the vector's size is 0. */
  inline bool empty() const { return m_vector.empty(); }

  /** Return the n'th element */
  inline PTR_TYPE operator[](int32_t n) {
    return staticCast<PTR_TYPE>(m_vector[n]);
  }

  /** Return the n'th element */
  inline const PTR_TYPE operator[](int32_t n) const {
    return staticCast<PTR_TYPE>(m_vector[n]);
  }

  /** Return the n'th element with bounds checking. */
  inline PTR_TYPE at(int32_t n) { return staticCast<PTR_TYPE>(m_vector.at(n)); }

  /** Return the n'th element with bounds checking. */
  inline const PTR_TYPE at(int32_t n) const {
    return staticCast<PTR_TYPE>(m_vector.at(n));
  }

  /** Get an iterator pointing to the start of vector. */
  inline Iterator begin() const { return Iterator(m_vector.begin()); }

  /** Get an iterator pointing to the end of vector. */
  inline Iterator end() const { return Iterator(m_vector.end()); }

  /** Create an empty vector. */
  inline VectorT() : m_vector() {}

  /** Create a vector with n elements allocated */
  inline VectorT(int32_t n) : m_vector(n) {}

  /** Create a vector with n copies of t */
  inline VectorT(int32_t n, const PTR_TYPE& t) : m_vector(n, t) {}

  /** copy constructor */
  inline VectorT(const VectorT& other) : m_vector(other.m_vector) {}

  /** destructor, sets all SharedPtr elements to NULLPTR */
  inline ~VectorT() {
    // destructor of m_vector field does all the work.
  }

  /** assignment operator */
  inline VectorT& operator=(const VectorT& other) {
    m_vector = other.m_vector;
    return *this;
  }

  /** reallocate a vector to hold n elements. */
  inline void reserve(int32_t n) { m_vector.reserve(n); }

  /** returns the first element. */
  inline PTR_TYPE front() { return staticCast<PTR_TYPE>(m_vector.front()); }

  /** returns the first element. */
  inline const PTR_TYPE front() const {
    return staticCast<PTR_TYPE>(m_vector.front());
  }

  /** returns the last element. */
  inline PTR_TYPE back() { return staticCast<PTR_TYPE>(m_vector.back()); }

  /** returns the last element. */
  inline const PTR_TYPE back() const {
    return staticCast<PTR_TYPE>(m_vector.back());
  }

  /** insert a new element at the end. */
  inline void push_back(const PTR_TYPE& e) { m_vector.push_back(e); }

  /** removes the last element. */
  inline void pop_back() { m_vector.pop_back(); }

  /** swaps the contents of two vectors. */
  inline void swap(VectorT& other) { m_vector.swap(other.m_vector); }

  /** erases all elements. */
  inline void clear() { m_vector.clear(); }

  /** inserts or erases elements at the end such that size becomes n.
   *  Not to be confused with reserve which simply allocates the space,
   *  resize fills the space with active elements. */
  inline void resize(int32_t n, const PTR_TYPE& t = NULLPTR) {
    m_vector.resize(n, t);
  }

  /** insert object at the given position. */
  inline void insert(int32_t index, const PTR_TYPE& t) {
    m_vector.insert(index, t);
  }

  /** Removes the object at the specified index from a vector. */
  inline void erase(int32_t index) { m_vector.erase(index); }
};

typedef VectorT<CacheablePtr> _VectorOfCacheable;
typedef VectorT<CacheableKeyPtr> _VectorOfCacheableKey;
typedef VectorT<RegionEntryPtr> VectorOfRegionEntry;
typedef VectorT<RegionPtr> VectorOfRegion;
typedef VectorT<CacheableStringPtr> VectorOfCacheableString;
typedef VectorT<CqListenerPtr> VectorOfCqListener;
typedef VectorT<CqQueryPtr> VectorOfCqQuery;

/**
 * A vector of <code>Cacheable</code> objects that also extends
 * <code>SharedBase</code> for smart pointers.
 */
class CPPCACHE_EXPORT VectorOfCacheable : public _VectorOfCacheable,
                                          public SharedBase {
 public:
  /** Iterator class for the vector. */
  typedef _VectorOfCacheable::Iterator Iterator;

  /** Create an empty vector. */
  inline VectorOfCacheable() : _VectorOfCacheable() {}

  /** Create a vector with n elements allocated. */
  inline VectorOfCacheable(int32_t n) : _VectorOfCacheable(n) {}

  /** Create a vector with n copies of t. */
  inline VectorOfCacheable(int32_t n, const CacheablePtr& t)
      : _VectorOfCacheable(n, t) {}

  /** Copy constructor. */
  inline VectorOfCacheable(const VectorOfCacheable& other)
      : _VectorOfCacheable(other) {}

 private:
  const VectorOfCacheable& operator=(const VectorOfCacheable&);
};

/**
 * A vector of <code>CacheableKey</code> objects that also extends
 * <code>SharedBase</code> for smart pointers.
 */
class CPPCACHE_EXPORT VectorOfCacheableKey : public _VectorOfCacheableKey,
                                             public SharedBase {
 public:
  /** Iterator class for the vector. */
  typedef _VectorOfCacheableKey::Iterator Iterator;

  /** Create an empty vector. */
  inline VectorOfCacheableKey() : _VectorOfCacheableKey() {}

  /** Create a vector with n elements allocated */
  inline VectorOfCacheableKey(int32_t n) : _VectorOfCacheableKey(n) {}

  /** Create a vector with n copies of t */
  inline VectorOfCacheableKey(int32_t n, const CacheableKeyPtr& t)
      : _VectorOfCacheableKey(n, t) {}

  /** Copy constructor */
  inline VectorOfCacheableKey(const VectorOfCacheableKey& other)
      : _VectorOfCacheableKey(other) {}

 private:
  const VectorOfCacheableKey& operator=(const VectorOfCacheableKey&);
};

typedef SharedPtr<VectorOfCacheable> VectorOfCacheablePtr;
typedef SharedPtr<VectorOfCacheableKey> VectorOfCacheableKeyPtr;
}

#endif
