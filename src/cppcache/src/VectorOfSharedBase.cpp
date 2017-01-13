/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <gfcpp/gfcpp_globals.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/VectorOfSharedBase.hpp>

#include <vector>
#include <stdexcept>

namespace gemfire {

// Iterator methods

VectorOfSharedBase::Iterator::Iterator(const VofSBPIterator& iter) {
  m_iter = new VofSBPIterator(iter);
}

VectorOfSharedBase::Iterator::Iterator(
    const VectorOfSharedBase::Iterator& other) {
  m_iter = new VofSBPIterator(*(other.m_iter));
}

const SharedBasePtr VectorOfSharedBase::Iterator::operator*() const {
  return *(*m_iter);
}

VectorOfSharedBase::Iterator& VectorOfSharedBase::Iterator::operator++() {
  ++(*m_iter);
  return *this;
}

void VectorOfSharedBase::Iterator::operator++(int) { ++(*m_iter); }

bool VectorOfSharedBase::Iterator::operator==(
    const VectorOfSharedBase::Iterator& other) const {
  return (*m_iter == *other.m_iter);
}

bool VectorOfSharedBase::Iterator::operator!=(
    const VectorOfSharedBase::Iterator& other) const {
  return (*m_iter != *other.m_iter);
}

VectorOfSharedBase::Iterator::~Iterator() { delete m_iter; }

// Vector methods

/** return the size of the vector. */
int32_t VectorOfSharedBase::size() const {
  return static_cast<int32_t>(m_stdvector->size());
}

/** return the largest possible size of the vector. */
int32_t VectorOfSharedBase::max_size() const {
  return static_cast<int32_t>(m_stdvector->max_size());
}

/** return the number of elements allocated for this vector. */
int32_t VectorOfSharedBase::capacity() const {
  return static_cast<int32_t>(m_stdvector->capacity());
}

/** return true if the vector's size is 0. */
bool VectorOfSharedBase::empty() const { return m_stdvector->empty(); }

/** Return the n'th element */
SharedBasePtr& VectorOfSharedBase::operator[](int32_t n) {
  return m_stdvector->operator[](n);
}

/** Return the n'th element */
const SharedBasePtr& VectorOfSharedBase::operator[](int32_t n) const {
  return m_stdvector->operator[](n);
}

/** Return the n'th element with bounds checkin. */
SharedBasePtr& VectorOfSharedBase::at(int32_t n) {
  try {
    return m_stdvector->at(n);
  } catch (const std::out_of_range& ex) {
    throw gemfire::OutOfRangeException(ex.what());
  }
}

/** Return the n'th element with bounds checkin. */
SharedBasePtr& VectorOfSharedBase::at(int32_t n) const {
  try {
    return m_stdvector->at(n);
  } catch (const std::out_of_range& ex) {
    throw gemfire::OutOfRangeException(ex.what());
  }
}

/** Get an iterator pointing to the start of vector. */
VectorOfSharedBase::Iterator VectorOfSharedBase::begin() const {
  return Iterator(m_stdvector->begin());
}

/** Get an iterator pointing to the end of vector. */
VectorOfSharedBase::Iterator VectorOfSharedBase::end() const {
  return Iterator(m_stdvector->end());
}

/** Create an empty vector. */
VectorOfSharedBase::VectorOfSharedBase() : m_stdvector(NULL) {
  m_stdvector = new VofSBP();
}

/** Create a vector with n elements allocated */
VectorOfSharedBase::VectorOfSharedBase(int32_t n) : m_stdvector(NULL) {
  m_stdvector = new VofSBP(n);
}

/** Create a vector with n copies of t */
VectorOfSharedBase::VectorOfSharedBase(int32_t n, const SharedBasePtr& t)
    : m_stdvector(NULL) {
  m_stdvector = new VofSBP(n, t);
}

/** copy constructor */
VectorOfSharedBase::VectorOfSharedBase(const VectorOfSharedBase& other)
    : m_stdvector(NULL) {
  m_stdvector = new VofSBP(*(other.m_stdvector));
}

/** destructor, sets all SharedPtr elements to NULL */
VectorOfSharedBase::~VectorOfSharedBase() {
  m_stdvector->clear();
  delete m_stdvector;
}

/** assignment operator */
VectorOfSharedBase& VectorOfSharedBase::operator=(
    const VectorOfSharedBase& other) {
  *m_stdvector = *(other.m_stdvector);
  return *this;
}

/** reallocate a vector to hold n elements. */
void VectorOfSharedBase::reserve(int32_t n) { m_stdvector->reserve(n); }

/** returns the first element. */
SharedBasePtr& VectorOfSharedBase::front() { return m_stdvector->front(); }

/** returns the first element. */
const SharedBasePtr& VectorOfSharedBase::front() const {
  return m_stdvector->front();
}

/** returns the last element. */
SharedBasePtr& VectorOfSharedBase::back() { return m_stdvector->back(); }

/** returns the last element. */
const SharedBasePtr& VectorOfSharedBase::back() const {
  return m_stdvector->back();
}

/** insert a new element at the end. */
void VectorOfSharedBase::push_back(const SharedBasePtr& e) {
  m_stdvector->push_back(e);
}

/** removes the last element. */
void VectorOfSharedBase::pop_back() { m_stdvector->pop_back(); }

/** swaps the contents of two vectors. */
void VectorOfSharedBase::swap(VectorOfSharedBase& other) {
  m_stdvector->swap(*(other.m_stdvector));
}

/** erases all elements. */
void VectorOfSharedBase::clear() { m_stdvector->clear(); }

/** inserts or erases elements at the end such that size becomes n.
 *  Not to be confused with reserve which simply allocates the space,
 *  resize fills the space with active elements. */
void VectorOfSharedBase::resize(int32_t n, const SharedBasePtr& t) {
  m_stdvector->resize(n, t);
}

void VectorOfSharedBase::insert(int32_t index, const SharedBasePtr& t) {
  if (index < static_cast<int32_t>(m_stdvector->size())) {
    m_stdvector->insert(m_stdvector->begin() + index, t);
  }
}

void VectorOfSharedBase::erase(int32_t index) {
  if (index < static_cast<int32_t>(m_stdvector->size())) {
    m_stdvector->erase(m_stdvector->begin() + index);
  }
}
}  // namespace gemfire
