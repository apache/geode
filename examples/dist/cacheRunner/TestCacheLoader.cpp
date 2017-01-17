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
