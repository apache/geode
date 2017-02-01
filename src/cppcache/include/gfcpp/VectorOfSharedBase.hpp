#pragma once

#ifndef GEODE_GFCPP_VECTOROFSHAREDBASE_H_
#define GEODE_GFCPP_VECTOROFSHAREDBASE_H_

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
#ifdef BUILD_CPPCACHE
#include <vector>
#endif

/** @file
*/

namespace apache {
namespace geode {
namespace client {

#ifdef BUILD_CPPCACHE
typedef std::vector<apache::geode::client::SharedBasePtr> VofSBP;
typedef VofSBP::const_iterator VofSBPIterator;
#else
class VofSBP;
class VofSBPIterator;
#endif

/** Represents a vector of <code>apache::geode::client::SharedBasePtr</code>
*/
class CPPCACHE_EXPORT VectorOfSharedBase {
 private:
  VofSBP* m_stdvector;

 public:
  /** Interface of an iterator for <code>VectorOfSharedBase</code>.*/
  class CPPCACHE_EXPORT Iterator {
   private:
    VofSBPIterator* m_iter;

    Iterator(const VofSBPIterator& iter);
    // Never defined.
    Iterator();

   private:
    const Iterator& operator=(const Iterator&);

   public:
    Iterator(const Iterator& other);

    const SharedBasePtr operator*() const;

    Iterator& operator++();

    void operator++(int);

    bool operator==(const Iterator& other) const;

    bool operator!=(const Iterator& other) const;

    ~Iterator();

    friend class VectorOfSharedBase;
  };

  /** return the size of the vector. */
  int32_t size() const;

  /** return the largest possible size of the vector. */
  int32_t max_size() const;

  /** return the number of elements allocated for this vector. */
  int32_t capacity() const;

  /** return true if the vector's size is 0. */
  bool empty() const;

  /** Return the n'th element */
  SharedBasePtr& operator[](int32_t n);

  /** Return the n'th element */
  const SharedBasePtr& operator[](int32_t n) const;

  /** Return the n'th element with bounds checking. */
  SharedBasePtr& at(int32_t n);

  /** Return the n'th element with bounds checking. */
  SharedBasePtr& at(int32_t n) const;

  /** Get an iterator pointing to the start of vector. */
  Iterator begin() const;

  /** Get an iterator pointing to the end of vector. */
  Iterator end() const;

  /** Create an empty vector. */
  VectorOfSharedBase();

  /** Create a vector with n elements allocated */
  VectorOfSharedBase(int32_t n);

  /** Create a vector with n copies of t */
  VectorOfSharedBase(int32_t n, const SharedBasePtr& t);

  /** copy constructor */
  VectorOfSharedBase(const VectorOfSharedBase& other);

  /** destructor, sets all SharedPtr elements to NULLPTR */
  ~VectorOfSharedBase();

  /** assignment operator */
  VectorOfSharedBase& operator=(const VectorOfSharedBase& other);

  /** reallocate a vector to hold n elements. */
  void reserve(int32_t n);

  /** returns the first element. */
  SharedBasePtr& front();

  /** returns the first element. */
  const SharedBasePtr& front() const;

  /** returns the last element. */
  SharedBasePtr& back();

  /** returns the last element. */
  const SharedBasePtr& back() const;

  /** insert a new element at the end. */
  void push_back(const SharedBasePtr& e);

  /** removes the last element. */
  void pop_back();

  /** swaps the contents of two vectors. */
  void swap(VectorOfSharedBase& other);

  /** erases all elements. */
  void clear();

  /** inserts or erases elements at the end such that size becomes n.
   *  Not to be confused with reserve which simply allocates the space,
   *  resize fills the space with active elements. */
  void resize(int32_t n, const SharedBasePtr& t = NULLPTR);

  /** insert object at the given index. */
  void insert(int32_t index, const SharedBasePtr& t);

  /** removes the object at the specified index from a vector
  */
  void erase(int32_t index);
};
}  // namespace client
}  // namespace geode
}  // namespace apache

#endif // GEODE_GFCPP_VECTOROFSHAREDBASE_H_
