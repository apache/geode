/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
 * @file TestCacheLoader.cpp
 * @since   1.0
 * @version 1.0
 * @see
*/

#include "TestCacheLoader.hpp"

// ----------------------------------------------------------------------------
TestCacheLoader::TestCacheLoader()
 :m_bInvoked(false)
{
}

/*
TestCacheLoader::TestCacheLoader( const CacheLoader& rhs)
: CacheLoader(rhs), m_bInvoked(false)
{
}
*/
// ----------------------------------------------------------------------------

TestCacheLoader::~TestCacheLoader(void)
{
}

// ----------------------------------------------------------------------------

CacheablePtr TestCacheLoader::load(
  const RegionPtr& region,
  const CacheableKeyPtr& key,
  const UserDataPtr& aCallbackArgument)
{
  m_bInvoked = true;
  printf( "CacheLoader.load : %s\n", printEvent(region, key,
        aCallbackArgument).c_str());
  CacheablePtr value = NULLPTR;
  try {
    value = region->get(key, aCallbackArgument);
  } catch(Exception& ex) {
      fprintf(stderr, "Exception in TestCacheCallback::printEvent [%s]\n", ex.getMessage());
  }
  if (value != NULLPTR) {
     printf( "Loader found value: ");
     std::string formatValue = printEntryValue(value);
     printf( "%s\n",formatValue.c_str());
  } else {
     printf( " Loader did not find a value");
  }

  return value;
}

// ----------------------------------------------------------------------------

void TestCacheLoader::close( const RegionPtr& region )
{
  m_bInvoked = true;
}

// ----------------------------------------------------------------------------
