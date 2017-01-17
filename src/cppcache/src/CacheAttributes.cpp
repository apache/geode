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

#include <Utils.hpp>
#include <string.h>
#include <stdlib.h>
#include <gfcpp/GemfireTypeIds.hpp>
#include <gfcpp/CacheAttributes.hpp>

using namespace gemfire;
CacheAttributes::CacheAttributes()
    : m_redundancyLevel(0), m_endpoints(NULL), m_cacheMode(false) {}

CacheAttributes::CacheAttributes(const CacheAttributes& rhs)
    : m_redundancyLevel(rhs.m_redundancyLevel) {
  copyStringAttribute(m_endpoints, rhs.m_endpoints);
  m_cacheMode = rhs.m_cacheMode;
}

CacheAttributes::~CacheAttributes() { GF_SAFE_DELETE_ARRAY(m_endpoints); }

int CacheAttributes::getRedundancyLevel() { return m_redundancyLevel; }

char* CacheAttributes::getEndpoints() { return m_endpoints; }

/** Return true if all the attributes are equal to those of other. */
bool CacheAttributes::operator==(const CacheAttributes& other) const {
  if (m_redundancyLevel != other.m_redundancyLevel) return false;
  if (0 != compareStringAttribute(m_endpoints, other.m_endpoints)) return false;

  return true;
}

int32_t CacheAttributes::compareStringAttribute(char* attributeA,
                                                char* attributeB) const {
  if (attributeA == NULL && attributeB == NULL) {
    return 0;
  } else if (attributeA == NULL && attributeB != NULL) {
    return -1;
  } else if (attributeA != NULL && attributeB == NULL) {
    return -1;
  }
  return (strcmp(attributeA, attributeB));
}

/** Return true if any of the attributes are not equal to those of other. */
bool CacheAttributes::operator!=(const CacheAttributes& other) const {
  return !(*this == other);
}

void CacheAttributes::copyStringAttribute(char*& lhs, const char* rhs) {
  if (lhs != NULL) {
    delete[] lhs;
  }
  if (rhs == NULL) {
    lhs = NULL;
  } else {
    size_t len = strlen(rhs) + 1;
    lhs = new char[len];
    memcpy(lhs, rhs, len);
  }
}
