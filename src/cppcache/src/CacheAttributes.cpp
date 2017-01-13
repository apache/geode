/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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
