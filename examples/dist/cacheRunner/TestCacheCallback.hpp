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
