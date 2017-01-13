/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/** 
 * @file TestCacheWriter.hpp
 * @since   1.0
 * @version 1.0
 * @see
*/

#ifndef __TEST_CACHE_WRITER_HPP__
#define __TEST_CACHE_WRITER_HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/CacheWriter.hpp>

#include "TestCacheCallback.hpp"

using namespace gemfire;

/**
 * A <code>CacheWriter</code> used in testing.  Its callback methods
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
  * @class TestCacheWriter
  *
  * @brief An example CacheWriter plug-in
  */ 
class TestCacheWriter : virtual public TestCacheCallback, virtual public CacheWriter
{
public:
  TestCacheWriter(void);
  ~TestCacheWriter(void);

  /**
   * Called before an entry is updated. The entry update is initiated by a <code>put</code>
   * or a <code>get</code> that causes the loader to update an existing entry.
   * The entry previously existed in the cache where the operation was
   * initiated, although the old value may have been null. The entry being
   * updated may or may not exist in the local cache where the CacheWriter is
   * installed.
   *
   * @param eventCode indicate the type of Event
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
  virtual bool beforeUpdate( const EntryEvent& event );

  virtual void beforeUpdate2( const EntryEvent& event );

  /** Called before an entry is created. Entry creation is initiated by a
   * <code>create</code>, a <code>put</code>, or a <code>get</code>.
   * The <code>CacheWriter</code> can determine whether this value comes from a
   * <code>get</code> or not from {link EntryEvent::isLoad}. The entry being
   * created may already exist in the local cache where this <code>CacheWriter</code>
   * is installed, but it does not yet exist in the cache where the operation was initiated.
   * @param eventCode indicate the type of Event
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
  virtual bool beforeCreate( const EntryEvent& event );

  virtual void beforeCreate2( const EntryEvent& event );
  
  /*@brief called before this region is invalidated
   * @param eventCode indicate the type of Event
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
  virtual void beforeInvalidate( const EntryEvent& event );

  virtual void beforeInvalidate2( const EntryEvent& event );
  
  /**
   * Called before an entry is destroyed. The entry being destroyed may or may
   * not exist in the local cache where the CacheWriter is installed. This method
   * is <em>not</em> called as a result of expiration or {link Region::localDestroyRegion}.
   *
   * @param eventCode indicate the type of Event
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
  virtual bool beforeDestroy( const EntryEvent& event );

  virtual void beforeDestroy2( const EntryEvent& event );
  
  /**
   * Called before a region is destroyed. The <code>CacheWriter</code>
   * will not additionally be called for each entry that is destroyed
   * in the region as a result of a region destroy. If the region's
   * subregions have <code>CacheWriter</code>s installed, then they
   * will be called for the cascading subregion destroys.
   * This method is <em>not</em> called as a result of
   * expiration or {link Region::localDestroyRegion}.  However, the
   * <code>close</code> method is invoked regardless of whether a
   * region is destroyed locally.  A non-local region destroy results
   * in an invocation of beforeRegionDestroy followed by an
   * invocation of {link CacheCallback::close}.
   *<p>
   * WARNING: This method should not destroy or create any regions itself, or a
   * deadlock will occur.
   *
   * @param eventCode indicate the type of Event
   * @param boolValues indicate the origin of Event
   * @param rptr indicate region the event is for
   * @param callbackArg user data
   *
   * This function does not throw any exception.
   * @see Region::invalidateRegion
   */
  virtual void beforeRegionInvalidate( const RegionEvent& event );

  virtual void beforeRegionInvalidate2( const RegionEvent& event );
  
  /*@brief called before this region is destroyed
   * @param eventCode indicate the type of Event
   * @param boolValues indicate the origin of Event
   * @param rptr indicate region the event is for
   * @param callbackArg user data
   *
   * This function does not throw any exception.
   * @see Region::destroyRegion
   */
  virtual bool beforeRegionDestroy( const RegionEvent& event );

  virtual void beforeRegionDestroy2( const RegionEvent& event );
  
  virtual void close( const RegionPtr& region );
  
 
  /**
    * Returns wether or not one of this <code>CacheWriter</code>
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

#endif // __TEST_CACHE_WRITER_HPP__

// ----------------------------------------------------------------------------
