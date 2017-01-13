/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

/**
 * @file
 */

#include <gfcpp/SelectResultsIterator.hpp>

namespace gemfire {

SelectResultsIterator::SelectResultsIterator(const CacheableVectorPtr& vectorSR,
                                             SelectResultsPtr srp)
    : m_vectorSR(vectorSR), m_nextIndex(0), m_srp(srp) {}

bool SelectResultsIterator::hasNext() const {
  return m_nextIndex < m_vectorSR->size();
}

const SerializablePtr SelectResultsIterator::next() {
  if (!hasNext()) return NULLPTR;

  return m_vectorSR->operator[](m_nextIndex++);
}

bool SelectResultsIterator::moveNext() {
  if (hasNext()) {
    m_nextIndex++;
    return true;
  } else {
    return false;
  }
}

const SerializablePtr SelectResultsIterator::current() const {
  if (m_nextIndex == 0 || m_nextIndex > m_vectorSR->size()) return NULLPTR;

  return m_vectorSR->operator[](m_nextIndex - 1);
}

void SelectResultsIterator::reset() { m_nextIndex = 0; }

}  // namespace gemfire
