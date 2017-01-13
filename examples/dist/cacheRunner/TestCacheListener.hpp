/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/** 
 * @file TestCacheListener.hpp
 * @since   1.0
 * @version 1.0
 * @see
*/

#ifndef __TEST_CACHE_LISTENER_HPP__
#define __TEST_CACHE_LISTENER_HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/CacheListener.hpp>

#include "TestCacheCallback.hpp"

using namespace gemfire;

/**
 * A <code>CacheListener</code> used in testing.  Its callback methods
 * are implemented to thrown {link UnsupportedOperationException}
 * unless the user overrides the "2" methods.
 *
 * @see #wasInvoked
 *
 * 
 *
 * @since 3.0
 */

/**
  * @class TestCacheListener
  *
  * @brief An example CacheListener plug-in
  */ 
class TestCacheListener : virtual public TestCacheCallback, virtual public CacheListener
{
public:
  TestCacheListener(void);
  ~TestCacheListener(void);

  /** Handles the event of a new key being added to a region. The entry
   * did not previously exist in this region in the local cache (even with a null value).
   * 
   * @param boolValues indicate the origin of Event
   * @param rptr indicate region the event is for
   * @param callbackArg user data
   * @param key the key to the target object of the event
   * @param newValue the new value of the target object of the event
   * @param oldValue the old value of the target object of the event
   * This function does not throw any exception.
   * @see Region::create
   * @see Region::put
   * @see Region::get
   */
  virtual void afterCreate( const EntryEvent& event );

  virtual void afterCreate2( const EntryEvent& event );
  
  /** Handles the event of an entry's value being modified in a region. This entry
   * previously existed in this region in the local cache, but its previous value
   * may have been null.
   *
   * @param boolValues indicate the origin of Event
   * @param rptr indicate the region the event is for
   * @param callbackArg user data
   * @param key the key to the target object of the event
   * @param newValue the new value of the target object of the event
   * @param oldValue the old value of the target object of the event
   * This function does not throw any exception.
   * @see Region::put
   */
  virtual void afterUpdate( const EntryEvent& event );

  virtual void afterUpdate2( const EntryEvent& event );

  /**
   * Handles the event of an entry's value being invalidated.
   *
   * @param boolValues indicate the origin of Event
   * @param rptr indicate the region the event is for
   * @param callbackArg user data
   * @param key the key to the target object of the event
   * @param newValue the new value of the target object of the event
   * @param oldValue the old value of the target object of the event
   * This function does not throw an exception.
   * @see Region::invalidate
   */
  virtual void afterInvalidate( const EntryEvent& event );

  virtual void afterInvalidate2( const EntryEvent& event );
  
  /**
   * Handles the event of an entry being destroyed.
   *
   * @param boolValues indicate the origin of Event
   * @param rptr indicate the region the event is for
   * @param callbackArg user data
   * @param key the key to the target object of the event
   * @param newValue the new value of the target object of the event
   * @param oldValue the old value of the target object of the event
   * This function does not throw an exception.
   * @see Region::destroy
   */
  virtual void afterDestroy( const EntryEvent& event );

  virtual void afterDestroy2( const EntryEvent& event );

  /** Handles the event of a region being invalidated. Events are not
   * invoked for each individual value that is invalidated as a result of
   * the region being invalidated. Each subregion, however, gets its own
   * <code>regionInvalidated</code> event invoked on its listener.
   * @param boolValues indicate the origin of Event
   * @param rptr indicate the region the event is for
   * @param callbackArg user data
   * This function does not throw any exception
   * @see Region::invalidateRegion
   */
  virtual void afterRegionInvalidate( const RegionEvent& event );

  virtual void afterRegionInvalidate2( const RegionEvent& event );
  
  /** Handles the event of a region being destroyed. Events are not invoked for
   * each individual entry that is destroyed as a result of the region being
   * destroyed. Each subregion, however, gets its own <code>afterRegionDestroyed</code> event
   * invoked on its listener.
   * @param boolValues indicate the origin of Event
   * @param rptr indicate the region the event is for
   * @param callbackArg user data
   * This function does not throw any exception.
   * @see Region::destroyRegion
   */
  virtual void afterRegionDestroy( const RegionEvent& event );

  virtual void afterRegionDestroy2( const RegionEvent& event );
  
  virtual void close(const RegionPtr& region);
  
  /**
    * Returns wether or not one of this <code>CacheListener</code>
    * methods was invoked.  Before returning, the <code>invoked</code>
    * flag is cleared.
    */
  bool wasInvoked( ) 
  {
    bool bInvoked = m_bInvoked;
    m_bInvoked = false;
    return bInvoked;
  }

  private:
    bool  m_bInvoked;
};

// ----------------------------------------------------------------------------

#endif // __TEST_CACHE_LISTENER_HPP__
