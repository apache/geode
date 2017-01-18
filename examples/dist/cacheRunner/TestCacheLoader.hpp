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
 * @file TestCacheLoader.hpp
 * @since   1.0
 * @version 1.0
 * @see
*/

#ifndef __TEST_CACHE_LOADER_HPP__
#define __TEST_CACHE_LOADER_HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/CacheLoader.hpp>

#include "TestCacheCallback.hpp"

using namespace apache::geode::client;

/**
 * A <code>CacheLoader</code> used in testing.  Users should override
 * the "2" method.
 *
 * @see #wasInvoked
 * @see TestCacheWriter
 *
 * 
 *
 * @since 3.0
 */

/**
  * @class TestCacheLoader
  *
  * @brief An example CacheLoader plug-in
  */ 
class TestCacheLoader : virtual public TestCacheCallback, virtual public CacheLoader
{
public:
  TestCacheLoader();
  //TestCacheLoader( const CacheLoader& rhs);
  
  ~TestCacheLoader( );

  /**Loads a value. Application writers should implement this
   * method to customize the loading of a value. This method is called
   * by the caching service when the requested value is not in the cache.
   * Any exception (including an unchecked exception) thrown by this
   * method is propagated back to and thrown by the invocation of
   * {link Region::get} that triggered this load.
   *
   * @param rp a Region Pointer for which this is called.
   * @param key the key for the cacheable
   * @param cptr the value to be loaded
   * @param aCallbackArgument a LoaderHelper object that is passed in from cache service
   * and provides access to the key, region, argument, and <code>netSearch</code>.
   * @return the value supplied for this key, or null if no value can be
   * supplied. If this load is invoked as part of a {link LoaderHelper::netSearch},
   * returning null will cause GemFire to invoke the next loader it finds
   * in the system (if there is one).  If every available loader returns
   * a null value, {link Region::get} will return null.
   *
   *This function does not throw any exception.
   *@see Region::get .
   */
  virtual CacheablePtr load(
    const RegionPtr& region, 
    const CacheableKeyPtr& key, 
    const UserDataPtr& aCallbackArgument);
  
  virtual void close( const RegionPtr& region );

  /**
    * Returns wether or not one of this <code>CacheLoader</code>
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

#endif // __TEST_CACHE_LOADER_HPP__
