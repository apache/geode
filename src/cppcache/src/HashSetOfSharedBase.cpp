/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/HashSetOfSharedBase.hpp>

namespace gemfire {

// Iterator methods

HashSetOfSharedBase::Iterator::Iterator(const HSofSBPIterator& iter,
                                        const HashSetOfSharedBase& set)
    : m_set(set) {
  m_iter = new HSofSBPIterator(iter);
}

HashSetOfSharedBase::Iterator::Iterator(
    const HashSetOfSharedBase::Iterator& other)
    : m_set(other.m_set) {
  m_iter = new HSofSBPIterator(*(other.m_iter));
}

const SharedBasePtr HashSetOfSharedBase::Iterator::operator*() const {
  return *(*m_iter);
}

bool HashSetOfSharedBase::Iterator::isEnd() const {
  return (*m_iter == m_set.m_stdHashSet->end());
}

HashSetOfSharedBase::Iterator& HashSetOfSharedBase::Iterator::operator++() {
  ++(*m_iter);
  return *this;
}

void HashSetOfSharedBase::Iterator::operator++(int) { ++(*m_iter); }

bool HashSetOfSharedBase::Iterator::operator==(
    const HashSetOfSharedBase::Iterator& other) const {
  return (*m_iter == *other.m_iter);
}

bool HashSetOfSharedBase::Iterator::operator!=(
    const HashSetOfSharedBase::Iterator& other) const {
  return (*m_iter != *other.m_iter);
}

void HashSetOfSharedBase::Iterator::reset() {
  *m_iter = m_set.m_stdHashSet->begin();
}

HashSetOfSharedBase::Iterator::~Iterator() { delete m_iter; }

// HashSet methods

int32_t HashSetOfSharedBase::size() const {
  return static_cast<int32_t>(m_stdHashSet->size());
}

int32_t HashSetOfSharedBase::max_size() const {
  return static_cast<int32_t>(m_stdHashSet->max_size());
}

bool HashSetOfSharedBase::empty() const { return m_stdHashSet->empty(); }

int32_t HashSetOfSharedBase::bucket_count() const {
  return static_cast<int32_t>(m_stdHashSet->bucket_count());
}

void HashSetOfSharedBase::resize(int32_t n) { m_stdHashSet->rehash(n); }

void HashSetOfSharedBase::swap(HashSetOfSharedBase& other) {
  m_stdHashSet->swap(*(other.m_stdHashSet));
}

bool HashSetOfSharedBase::insert(const SharedBasePtr& k) {
  std::pair<HSofSBP::iterator, bool> result = m_stdHashSet->insert(k);
  return result.second;
}

int32_t HashSetOfSharedBase::erase(const SharedBasePtr& k) {
  return static_cast<int32_t>(m_stdHashSet->erase(k));
}

void HashSetOfSharedBase::clear() { m_stdHashSet->clear(); }

bool HashSetOfSharedBase::contains(const SharedBasePtr& k) const {
  HSofSBP::const_iterator iter = m_stdHashSet->find(k);
  return (iter != m_stdHashSet->end());
}

int32_t HashSetOfSharedBase::count(const SharedBasePtr& k) const {
  return static_cast<int32_t>(m_stdHashSet->count(k));
}

HashSetOfSharedBase::Iterator HashSetOfSharedBase::begin() const {
  return Iterator(m_stdHashSet->begin(), *this);
}

HashSetOfSharedBase::Iterator HashSetOfSharedBase::end() const {
  return Iterator(m_stdHashSet->end(), *this);
}

HashSetOfSharedBase& HashSetOfSharedBase::operator=(
    const HashSetOfSharedBase& other) {
  *m_stdHashSet = *(other.m_stdHashSet);
  return *this;
}

HashSetOfSharedBase::HashSetOfSharedBase(const Hasher h, const EqualTo k) {
  HashSB hSB(h);
  EqualToSB eqSB(k);
  m_stdHashSet = new HSofSBP(100, hSB, eqSB);
}

HashSetOfSharedBase::HashSetOfSharedBase(int32_t n, const Hasher h,
                                         const EqualTo k) {
  HashSB hSB(h);
  EqualToSB eqSB(k);
  m_stdHashSet = new HSofSBP(n, hSB, eqSB);
}

HashSetOfSharedBase::HashSetOfSharedBase(const HashSetOfSharedBase& other) {
  /*
     Note: The line below marked ORIGINAL doesn't compile on newer compilers.
     For example, on Microsoft Visual 2013 or later, it generates error C2248
     because the HashSB() and
     EqualToSB() constructors are private.

     Until we refactor this class (HashSetOfSharedBase) to include HashSB and
     EqualToSB functionality this line
     is replaced with the code marked NEW below.
  */

  // *** ORIGINAL ***
  // m_stdHashSet = new HSofSBP(*(other.m_stdHashSet));

  // *** NEW ***
  HashSB hSB((other.m_stdHashSet)->hash_function());
  EqualToSB eqSB((other.m_stdHashSet)->key_eq());
  size_t size = other.size();
  m_stdHashSet = new HSofSBP(size, hSB, eqSB);
  *m_stdHashSet = *(other.m_stdHashSet);
}

HashSetOfSharedBase::~HashSetOfSharedBase() {
  m_stdHashSet->clear();
  delete m_stdHashSet;
}
}  // namespace gemfire
