#ifndef _GEMFIRE_HASHSETOFSHAREDBASE_HPP_
#define _GEMFIRE_HASHSETOFSHAREDBASE_HPP_

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
#include "HashFunction.hpp"
#ifdef BUILD_CPPCACHE
#include <unordered_set>
#endif

/** @file
 */

namespace gemfire {

#ifdef BUILD_CPPCACHE
typedef std::unordered_set<SharedBasePtr, HashSB, EqualToSB> HSofSBP;
typedef HSofSBP::const_iterator HSofSBPIterator;
#else
class HSofSBP;
class HSofSBPIterator;
#endif

/** Represents a HashSet of <code>SharedBase</code>
 */
class CPPCACHE_EXPORT HashSetOfSharedBase {
 private:
  HSofSBP* m_stdHashSet;

  // Never defined.
  HashSetOfSharedBase();

 public:
  /** Interface of an iterator for <code>HashSetOfSharedBase</code>.*/
  class CPPCACHE_EXPORT Iterator {
   private:
    const HashSetOfSharedBase& m_set;
    HSofSBPIterator* m_iter;

    Iterator(const HSofSBPIterator& iter, const HashSetOfSharedBase& set);

    // Never defined.
    Iterator();

   public:
    Iterator(const Iterator& other);

    const SharedBasePtr operator*() const;

    bool isEnd() const;

    Iterator& operator++();

    void operator++(int);

    bool operator==(const Iterator& other) const;

    bool operator!=(const Iterator& other) const;

    void reset();

    ~Iterator();

    friend class HashSetOfSharedBase;

   private:
    const Iterator& operator=(const Iterator&);
  };

  /** Returns the size of the hash_set. */
  int32_t size() const;

  /** Returns the largest possible size of the hash_set. */
  int32_t max_size() const;

  /** true if the hash_set's size is 0. */
  bool empty() const;

  /** Returns the number of buckets used by the hash_set. */
  int32_t bucket_count() const;

  /** Increases the bucket count to at least n. */
  void resize(int32_t n);

  /** Swaps the contents of two hash_sets. */
  void swap(HashSetOfSharedBase& other);

  /** Inserts the key k into the hash_set. */
  bool insert(const SharedBasePtr& k);

  /** Erases the element whose key is k. */
  int32_t erase(const SharedBasePtr& k);

  /** Erases all of the elements. */
  void clear();

  /** Check if a given key k exists in the hash_set. */
  bool contains(const SharedBasePtr& k) const;

  /** Counts the number of elements whose key is k. */
  int32_t count(const SharedBasePtr& k) const;

  /** Get an iterator pointing to the start of hash_set. */
  Iterator begin() const;

  /** Get an iterator pointing to the end of hash_set. */
  Iterator end() const;

  /** Assignment operator. */
  HashSetOfSharedBase& operator=(const HashSetOfSharedBase& other);

  /** Creates an empty hash_set using h as the hash function
   * and k as the key equal function.
   */
  HashSetOfSharedBase(const Hasher h, const EqualTo k);

  /** Creates an empty hash_set with at least n buckets,
   * using h as the hash function and k as the key equal function.
   */
  HashSetOfSharedBase(int32_t n, const Hasher h, const EqualTo k);

  /** Copy constructor. */
  HashSetOfSharedBase(const HashSetOfSharedBase& other);

  /** Destructor, sets all SharedPtr elements to NULLPTR. */
  ~HashSetOfSharedBase();
};
}  // namespace gemfire

#endif
