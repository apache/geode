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

#include <gfcpp/HashMapOfSharedBase.hpp>

namespace apache {
namespace geode {
namespace client {

// Iterator methods

HashMapOfSharedBase::Iterator::Iterator(const HMofSBPIterator& iter) {
  m_iter = new HMofSBPIterator(iter);
}

HashMapOfSharedBase::Iterator::Iterator(
    const HashMapOfSharedBase::Iterator& other) {
  m_iter = new HMofSBPIterator(*(other.m_iter));
}

const SharedBasePtr HashMapOfSharedBase::Iterator::first() const {
  return (*m_iter)->first;
}

const SharedBasePtr HashMapOfSharedBase::Iterator::second() const {
  return (*m_iter)->second;
}

HashMapOfSharedBase::Iterator& HashMapOfSharedBase::Iterator::operator++() {
  ++(*m_iter);
  return *this;
}

void HashMapOfSharedBase::Iterator::operator++(int) { ++(*m_iter); }

bool HashMapOfSharedBase::Iterator::operator==(
    const HashMapOfSharedBase::Iterator& other) const {
  return (*m_iter == *other.m_iter);
}

bool HashMapOfSharedBase::Iterator::operator!=(
    const HashMapOfSharedBase::Iterator& other) const {
  return (*m_iter != *other.m_iter);
}

HashMapOfSharedBase::Iterator::~Iterator() { delete m_iter; }

// HashMap methods

int32_t HashMapOfSharedBase::size() const {
  return static_cast<int32_t>(m_stdHashMap->size());
}

int32_t HashMapOfSharedBase::max_size() const {
  return static_cast<int32_t>(m_stdHashMap->max_size());
}

bool HashMapOfSharedBase::empty() const { return m_stdHashMap->empty(); }

int32_t HashMapOfSharedBase::bucket_count() const {
  return static_cast<int32_t>(m_stdHashMap->bucket_count());
}

void HashMapOfSharedBase::resize(int32_t n) { m_stdHashMap->rehash(n); }

void HashMapOfSharedBase::swap(HashMapOfSharedBase& other) {
  m_stdHashMap->swap(*(other.m_stdHashMap));
}

bool HashMapOfSharedBase::insert(const SharedBasePtr& k,
                                 const SharedBasePtr& v) {
  std::pair<HMofSBP::iterator, bool> result =
      m_stdHashMap->insert(HMofSBP::value_type(k, v));
  return result.second;
}

int32_t HashMapOfSharedBase::erase(const SharedBasePtr& k) {
  return static_cast<int32_t>(m_stdHashMap->erase(k));
}

void HashMapOfSharedBase::clear() { m_stdHashMap->clear(); }

bool HashMapOfSharedBase::contains(const SharedBasePtr& k) const {
  HMofSBP::const_iterator iter = m_stdHashMap->find(k);
  return (iter != m_stdHashMap->end());
}

HashMapOfSharedBase::Iterator HashMapOfSharedBase::find(
    const SharedBasePtr& k) const {
  return Iterator(m_stdHashMap->find(k));
}

int32_t HashMapOfSharedBase::count(const SharedBasePtr& k) const {
  return static_cast<int32_t>(m_stdHashMap->count(k));
}

SharedBasePtr& HashMapOfSharedBase::operator[](const SharedBasePtr& k) {
  return (*m_stdHashMap)[k];
}

HashMapOfSharedBase::Iterator HashMapOfSharedBase::begin() const {
  return HashMapOfSharedBase::Iterator(m_stdHashMap->begin());
}

HashMapOfSharedBase::Iterator HashMapOfSharedBase::end() const {
  return HashMapOfSharedBase::Iterator(m_stdHashMap->end());
}

HashMapOfSharedBase& HashMapOfSharedBase::operator=(
    const HashMapOfSharedBase& other) {
  *m_stdHashMap = *(other.m_stdHashMap);
  return *this;
}

HashMapOfSharedBase::HashMapOfSharedBase(const Hasher h, const EqualTo k) {
  HashSB hSB(h);
  EqualToSB eqSB(k);
  m_stdHashMap = new HMofSBP(100, hSB, eqSB);
}

HashMapOfSharedBase::HashMapOfSharedBase(int32_t n, const Hasher h,
                                         const EqualTo k) {
  HashSB hSB(h);
  EqualToSB eqSB(k);
  m_stdHashMap = new HMofSBP(n, hSB, eqSB);
}

HashMapOfSharedBase::HashMapOfSharedBase(const HashMapOfSharedBase& other) {
  /*
  Note: The line below marked ORIGINAL doesn't compile on newer compilers.
  For example, on Microsoft Visual 2013 or later, it generates error C2248
  because the HashSB() and
  EqualToSB() constructors are private.

  Until we refactor this class (HashMapOfSharedBase) to include HashSB and
  EqualToSB functionality this line
  is replaced with the code marked NEW below.
  */

  // *** ORIGINAL ***
  // m_stdHashMap = new HMofSBP(*(other.m_stdHashMap));

  // *** NEW ***
  HashSB hSB((other.m_stdHashMap)->hash_function());
  EqualToSB eqSB((other.m_stdHashMap)->key_eq());
  size_t size = other.size();
  m_stdHashMap = new HMofSBP(size, hSB, eqSB);
  *m_stdHashMap = *(other.m_stdHashMap);
}

HashMapOfSharedBase::~HashMapOfSharedBase() {
  m_stdHashMap->clear();
  delete m_stdHashMap;
}
}  // namespace client
}  // namespace geode
}  // namespace apache
