/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "ResultSetImpl.hpp"

/**
 * @file
 */

using namespace gemfire;

ResultSetImpl::ResultSetImpl(const CacheableVectorPtr& response)
    : m_resultSetVector(response)
// UNUSED , m_nextIndex(0)
{}

bool ResultSetImpl::isModifiable() const { return false; }

int32_t ResultSetImpl::size() const {
  return static_cast<int32_t>(m_resultSetVector->size());
}

const SerializablePtr ResultSetImpl::operator[](int32_t index) const {
  if (index >= m_resultSetVector->size()) {
    throw IllegalArgumentException("index out of bounds");
  }
  return m_resultSetVector->operator[](index);
}

SelectResultsIterator ResultSetImpl::getIterator() {
  return SelectResultsIterator(m_resultSetVector, SelectResultsPtr(this));
}

SelectResults::Iterator ResultSetImpl::begin() const {
  return m_resultSetVector->begin();
}

SelectResults::Iterator ResultSetImpl::end() const {
  return m_resultSetVector->end();
}

ResultSetImpl::~ResultSetImpl() {}
