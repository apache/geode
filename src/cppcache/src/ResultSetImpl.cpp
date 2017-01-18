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

#include "ResultSetImpl.hpp"

/**
 * @file
 */

using namespace apache::geode::client;

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
