/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include "Cache.hpp"
#include "ExpirationAttributes.hpp"
#include "impl/Utils.hpp"
#include "DistributedSystem.hpp"
#include <stdlib.h>
#include <string.h>
#include "CacheAttributesFactory.hpp"

using namespace gemfire;

CacheAttributesFactory::CacheAttributesFactory()
  : m_cacheAttributes( new CacheAttributes( ) )
{
}

CacheAttributesFactory::~CacheAttributesFactory()
{
}

void CacheAttributesFactory::setRedundancyLevel(int redundancyLevel)
{
  m_cacheAttributes->m_redundancyLevel = redundancyLevel;
  m_cacheAttributes->m_cacheMode=true;
}

void CacheAttributesFactory::setEndpoints(const char* endpoints)
{
  if ( m_cacheAttributes->m_endpoints != NULL ) {
    delete [] m_cacheAttributes->m_endpoints;
  }
  if ( endpoints == NULL ) {
    m_cacheAttributes->m_endpoints = NULL;
  } else {
    size_t len = strlen( endpoints );
    m_cacheAttributes->m_endpoints = new char[ len + 1 ];
    memcpy( m_cacheAttributes->m_endpoints, endpoints, len + 1 );
  }
}

CacheAttributesPtr CacheAttributesFactory::createCacheAttributes()
{
  return m_cacheAttributes;
}

