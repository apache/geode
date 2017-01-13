#ifndef _GEMFIRE_HASHMAPOFSHAREDBASE_HPP_
#define _GEMFIRE_HASHMAPOFSHAREDBASE_HPP_

/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "gfcpp_globals.hpp"
#include "SharedPtr.hpp"
#include "HashFunction.hpp"
#ifdef BUILD_CPPCACHE
#include <unordered_map>
#endif

/** @file
 */

namespace gemfire {

#ifdef BUILD_CPPCACHE
typedef std::unordered_map<SharedBasePtr, SharedBasePtr, HashSB, EqualToSB>
    HMofSBP;
typedef HMofSBP::const_iterator HMofSBPIterator;
#else
class HMofSBP;
class HMofSBPIterator;
#endif

/** Represents a HashMap of <code>SharedBase</code>
 */
class CPPCACHE_EXPORT HashMapOfSharedBase {
 private:
  HMofSBP* m_stdHashMap;

  // Never defined.
  HashMapOfSharedBase();

 public:
  /** Interface of an iterator for <code>HashMapOfSharedBase</code>.*/
  class CPPCACHE_EXPORT Iterator {
   private:
    HMofSBPIterator* m_iter;

    Iterator(const HMofSBPIterator& iter);

    // Never defined
    Iterator();

   public:
    Iterator(const Iterator& other);

    const SharedBasePtr first() const;

    const SharedBasePtr second() const;

    Iterator& operator++();

    void operator++(int);

    bool operator==(const Iterator& other) const;

    bool operator!=(const Iterator& other) const;

    ~Iterator();

    friend class HashMapOfSharedBase;

   private:
    const Iterator& operator=(const Iterator&);
  };

  /** Returns the size of the hash_map. */
  int32_t size() const;

  /** Returns the largest possible size of the hash_map. */
  int32_t max_size() const;

  /** true if the hash_map's size is 0. */
  bool empty() const;

  /** Returns the number of buckets used by the hash_map. */
  int32_t bucket_count() const;

  /** Increases the bucket count to at least n. */
  void resize(int32_t n);

  /** Swaps the contents of two hash_maps. */
  void swap(HashMapOfSharedBase& other);

  /** Inserts the <k, v> pair into the hash_map,
   * when k does not exist in the hash_map.
   */
  bool insert(const SharedBasePtr& k, const SharedBasePtr& v);

  /** Erases the element whose key is k. */
  int32_t erase(const SharedBasePtr& k);

  /** Erases all of the elements. */
  void clear();

  /** Check if a given key k exists in the hash_map. */
  bool contains(const SharedBasePtr& k) const;

  /** Finds an element whose key is k. */
  Iterator find(const SharedBasePtr& k) const;

  /** Counts the number of elements whose key is k. */
  int32_t count(const SharedBasePtr& k) const;

  /** Returns a reference to the object that is associated
   * with a particular key.
   */
  SharedBasePtr& operator[](const SharedBasePtr& k);

  /** Get an iterator pointing to the start of hash_map. */
  Iterator begin() const;

  /** Get an iterator pointing to the end of hash_map. */
  Iterator end() const;

  /** Assignment operator. */
  HashMapOfSharedBase& operator=(const HashMapOfSharedBase& other);

  /** Creates an empty hash_map using h as the hash function
   * and k as the key equal function.
   */
  HashMapOfSharedBase(const Hasher h, const EqualTo k);

  /** Creates an empty hash_map with at least n buckets,
   * using h as the hash function and k as the key equal function.
   */
  HashMapOfSharedBase(int32_t n, const Hasher h, const EqualTo k);

  /** Copy constructor. */
  HashMapOfSharedBase(const HashMapOfSharedBase& other);

  /** Destructor, sets all SharedPtr elements to NULLPTR. */
  ~HashMapOfSharedBase();
};
}

#endif
