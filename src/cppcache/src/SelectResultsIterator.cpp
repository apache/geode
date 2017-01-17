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
