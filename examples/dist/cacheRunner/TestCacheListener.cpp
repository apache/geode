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
 * @file TestCacheListener.cpp
 * @since   1.0
 * @version 1.0
 * @see
*/

#include "TestCacheListener.hpp"

// ----------------------------------------------------------------------------

TestCacheListener::TestCacheListener(void)
: m_bInvoked(false)
{
}

// ----------------------------------------------------------------------------

TestCacheListener::~TestCacheListener(void)
{
}

// ----------------------------------------------------------------------------

inline void TestCacheListener::afterCreate(
  const EntryEvent& event)
{
  m_bInvoked = true;

  afterCreate2(event);
}

// ----------------------------------------------------------------------------

inline void TestCacheListener::afterCreate2(const EntryEvent& event)
{
  printf( "TestCacheListener.afterCreate : %s\n", printEvent(event).c_str());
 
}
// ----------------------------------------------------------------------------


inline void TestCacheListener::afterUpdate(
 const EntryEvent& event)
{
  m_bInvoked = true;
  afterUpdate2(event);
}

// ----------------------------------------------------------------------------

inline void TestCacheListener::afterUpdate2(
 const EntryEvent& event)
{
  printf( "TestCacheListener.afterUpdate : %s\n", 
  printEvent(event).c_str());

}


// ----------------------------------------------------------------------------


inline void TestCacheListener::afterInvalidate(
 const EntryEvent& event)
{
  m_bInvoked = true;
  afterInvalidate2(event);
}

// ----------------------------------------------------------------------------

inline void TestCacheListener::afterInvalidate2(
 const EntryEvent& event)
{
  printf( "TestCacheListener.afterInvalidate : %s\n", 
  printEvent(event).c_str());

}

// ----------------------------------------------------------------------------


inline void TestCacheListener::afterDestroy(
  const EntryEvent& event) 
{
  m_bInvoked = true;

  afterDestroy2(event);
}

// ----------------------------------------------------------------------------

inline void TestCacheListener::afterDestroy2(
  const EntryEvent& event) 
{
  printf( "TestCacheListener.afterDestroy : %s\n", 
  printEvent(event).c_str());

}


// ----------------------------------------------------------------------------


inline void TestCacheListener::afterRegionInvalidate(
  const RegionEvent& event)
{
  m_bInvoked = true;

  afterRegionInvalidate2(event);
}

// ----------------------------------------------------------------------------

inline void TestCacheListener::afterRegionInvalidate2(
  const RegionEvent& event)
{
  printf( "TestCacheListener.afterRegionInvalidate : %s\n", 
  printEvent(event).c_str());

}


// ----------------------------------------------------------------------------


inline void TestCacheListener::afterRegionDestroy(
  const RegionEvent& event)
{
  m_bInvoked = true;

  afterRegionDestroy2(event);
}

// ----------------------------------------------------------------------------

inline void TestCacheListener::afterRegionDestroy2(
const RegionEvent& event)
{
  printf( "TestCacheListener.afterRegionDestroy : %s\n", 
  printEvent(event).c_str());  

}


// ----------------------------------------------------------------------------

inline void TestCacheListener::close(const RegionPtr& region)
{
  m_bInvoked = true;
  printf( "TestCacheListener.close\n");
}

// ----------------------------------------------------------------------------
