/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.  
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/** 
 * @file TestCacheCallback.hpp
 * @since   1.0
 * @version 1.0
 * @see
*/

#ifndef __TEST_CACHE_CALLBACK_HPP__
#define __TEST_CACHE_CALLBACK_HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/EntryEvent.hpp>
 #include <gfcpp/RegionEvent.hpp>

#include <string>

using namespace gemfire;

/**
 * An abstract superclass of implementation of GemFire cache callbacks
 * that are used for testing.
 *
 * @see #wasInvoked
 *
 * 
 *
 * @since 3.0
 */

/**
 * @class TestCacheCallback
 *
 * @brief An abstract superclass of implementation of GemFire cache callbacks
 * that are used for testing.
 */ 
class TestCacheCallback
{
public:
  TestCacheCallback(void);
  virtual ~TestCacheCallback(void);

public: // CacheCallback virtuals
  virtual void close( const RegionPtr& region );
  
public:
  /**
   * Returns whether or not one of this <code>CacheListener</code>
   * methods was invoked.  Before returning, the <code>invoked</code>
   * flag is cleared.
   */
  bool wasInvoked( );
  
  /**
   * This method will do nothing.  Note that it will not throw an
   * exception.
   */
  //virtual int close2( const RegionPtr& region );
  
  /**
   * Returns a description of the given <code>CacheEvent</code>.
   */
  std::string printEvent( const EntryEvent& event );

  /**
   * Returns a description of the given <code>CacheEvent</code>.
   */
  std::string printEvent( const RegionEvent& event );

  /**
   * Returns a description of the given load request.
   */
  std::string printEvent(
    const RegionPtr& rp, 
    const CacheableKeyPtr& key, 
    const UserDataPtr& aCallbackArgument);
  std::string printEntryValue(CacheablePtr& value);
  std::string getBoolValues( bool isRemote );

private:
  /** Was this callback invoked? */
  bool        m_bInvoked;
};

// ----------------------------------------------------------------------------

#endif // __TEST_CACHE_CALLBACK_HPP__

// ----------------------------------------------------------------------------
