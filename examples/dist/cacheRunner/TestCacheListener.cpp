/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
