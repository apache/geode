/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/** 
 * @file TestCacheWriter.cpp
 * @since   1.0
 * @version 1.0
 * @see
*/

#include "TestCacheWriter.hpp"

// ----------------------------------------------------------------------------

TestCacheWriter::TestCacheWriter(void)
: m_bInvoked(false)
{
}

// ----------------------------------------------------------------------------

TestCacheWriter::~TestCacheWriter(void)
{
}

// ----------------------------------------------------------------------------
/**
  * Called before an entry is updated. The entry update is initiated by a <code>put</code>
  * or a <code>get</code> that causes the loader to update an existing entry.
  * The entry previously existed in the cache where the operation was
  * initiated, although the old value may have been null. The entry being
  * updated may or may not exist in the local cache where the CacheWriter is
  * installed.
  *
  * @param boolValues indicate the origin of Event
  * @param rptr indicate the region the event is for
  * @param callbackArg user data
  * @param key the key to the target object of the event
  * @param newValue the new value of the target object of the event
  * @param oldValue the old value of the target object of the event
  *
  * This function does not throw any exception.
  *
  * @see Region::put
  * @see Region::get
  */
// ----------------------------------------------------------------------------

bool TestCacheWriter::beforeUpdate( const EntryEvent& event )
{
   m_bInvoked = true;

   beforeUpdate2( event );
   return m_bInvoked;
}

// ----------------------------------------------------------------------------

void TestCacheWriter::beforeUpdate2( const EntryEvent& event )
{
  printf( "TestCacheWriter.beforeUpdate : %s\n", 
  printEvent( event ).c_str() );
}
  

// ----------------------------------------------------------------------------
/** Called before an entry is created. Entry creation is initiated by a
  * <code>create</code>, a <code>put</code>, or a <code>get</code>.
  * The <code>CacheWriter</code> can determine whether this value comes from a
  * <code>get</code> or not from {@link EntryEvent::isLoad}. The entry being
  * created may already exist in the local cache where this <code>CacheWriter</code>
  * is installed, but it does not yet exist in the cache where the operation was initiated.
  * @param boolValues indicate the origin of Event
  * @param rptr indicate the region the event is for
  * @param callbackArg user data
  * @param key the key to the target object of the event
  * @param newValue the new value of the target object of the event
  * @param oldValue the old value of the target object of the event
  *
  * This function does not throw any exception.
  *
  * @see Region::create
  * @see Region::put
  * @see Region::get
  */
// ----------------------------------------------------------------------------

bool TestCacheWriter::beforeCreate( const EntryEvent& event )
{
  m_bInvoked = true;

  beforeCreate2( event );
  return m_bInvoked;
}

// ----------------------------------------------------------------------------

//inline ErrorCodes::Codes TestCacheWriter::beforeCreate2( const EntryEvent& event )
inline void TestCacheWriter::beforeCreate2( const EntryEvent& event )
{
   printf( "TestCacheWriter.beforeCreate : %s\n", 
   printEvent( event ).c_str() );

}
  
// ----------------------------------------------------------------------------
/*@brief called before this region is invalidated
  * @param boolValues indicate the origin of Event
  * @param rptr indicate the region the event is for
  * @param callbackArg user data
  * @param key the key to the target object of the event
  * @param newValue the new value of the target object of the event
  * @param oldValue the old value of the target object of the event
  *
  * This function does not throw any exception.
  * @see Region::invalidate
  */
// ----------------------------------------------------------------------------

void TestCacheWriter::beforeInvalidate( const EntryEvent& event )
{
  m_bInvoked = true;

  beforeInvalidate2( event );
}

// ----------------------------------------------------------------------------

//inline ErrorCodes::Codes TestCacheWriter::beforeInvalidate2( const EntryEvent& event )
inline void TestCacheWriter::beforeInvalidate2( const EntryEvent& event )
{
  printf( "TestCacheWriter.beforeInvalidate : %s\n", 
  printEvent( event ).c_str() );
}

// ----------------------------------------------------------------------------
/**
  * Called before an entry is destroyed. The entry being destroyed may or may
  * not exist in the local cache where the CacheWriter is installed. This method
  * is <em>not</em> called as a result of expiration or {@link Region::localDestroyRegion}.
  *
  * @param boolValues indicate the origin of Event
  * @param rptr indicate the region the event is for
  * @param callbackArg user data
  * @param key the key to the target object of the event
  * @param newValue the new value of the target object of the event
  * @param oldValue the old value of the target object of the event
  * This function does not throw any exception.
  *
  * @see Region::destroy
  */
// ----------------------------------------------------------------------------

bool TestCacheWriter::beforeDestroy( const EntryEvent& event )
{
  m_bInvoked = true;

  beforeDestroy2( event );
  return m_bInvoked;
}

// ----------------------------------------------------------------------------

void TestCacheWriter::beforeDestroy2( const EntryEvent& event )
{
  printf( "TestCacheWriter.beforeDestroy : %s\n", 
  printEvent( event ).c_str() );
}

// ----------------------------------------------------------------------------


/**
  * Called before a region is destroyed. The <code>CacheWriter</code>
  * will not additionally be called for each entry that is destroyed
  * in the region as a result of a region destroy. If the region's
  * subregions have <code>CacheWriter</code>s installed, then they
  * will be called for the cascading subregion destroys.
  * This method is <em>not</em> called as a result of
  * expiration or {@link Region::localDestroyRegion}.  However, the
  * <code>close</code> method is invoked regardless of whether a
  * region is destroyed locally.  A non-local region destroy results
  * in an invocation of beforeRegionDestroy followed by an
  * invocation of {@link CacheCallback::close}.
  *<p>
  * WARNING: This method should not destroy or create any regions itself, or a
  * deadlock will occur.
  *
  * @param boolValues indicate the origin of Event
  * @param rptr indicate region the event is for
  * @param callbackArg user data
  *
  * This function does not throw any exception.
  * @see Region::invalidateRegion
  */
// ----------------------------------------------------------------------------

void TestCacheWriter::beforeRegionInvalidate( const RegionEvent& event )
{
  m_bInvoked = true;

  beforeRegionInvalidate2( event );
}

// ----------------------------------------------------------------------------

void TestCacheWriter::beforeRegionInvalidate2( const RegionEvent& event )
{
  printf( "TestCacheWriter.beforeRegionInvalidate : %s\n", 
  printEvent( event ).c_str() );
}

// ----------------------------------------------------------------------------
/*@brief called before this region is destroyed
  * @param boolValues indicate the origin of Event
  * @param rptr indicate region the event is for
  * @param callbackArg user data
  *
  * This function does not throw any exception.
  * @see Region::destroyRegion
  */
// ----------------------------------------------------------------------------

bool TestCacheWriter::beforeRegionDestroy( const RegionEvent& event )
{
  m_bInvoked = true;

  beforeRegionDestroy2( event );
  return m_bInvoked;
}

// ----------------------------------------------------------------------------

void TestCacheWriter::beforeRegionDestroy2( const RegionEvent& event )
{
  printf( "TestCacheWriter.beforeRegionDestroy : %s\n", 
    printEvent( event ).c_str() );
}

// ----------------------------------------------------------------------------

void TestCacheWriter::close( const RegionPtr& region )
{
  m_bInvoked = true;
  printf( "TestCacheWriter.close\n");
}

