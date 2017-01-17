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

#pragma once

#include "gf_defs.hpp"
#include "IRegionService.hpp"
#include "DistributedSystem.hpp"
#include "CacheTransactionManager.hpp"
using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {
      /// <summary>
      /// GemFireCache represents the singleton cache that must be created
      /// in order to connect to Gemfire server.
      /// </summary>
      /// <remarks>
      /// Caches are obtained from Crest methods on the
      /// <see cref="CacheFactory.Create"/> class.
      /// <para>
      /// When a cache is created a <see cref="DistributedSystem" />
      /// must be specified.
      /// </para><para>
      /// When a cache will no longer be used, call <see cref="Cache.Close" />.
      /// Once it <see cref="Cache.IsClosed" /> any attempt to use it
      /// will cause a <c>CacheClosedException</c> to be thrown.
      /// </para><para>
      /// A cache can have multiple root regions, each with a different name.
      /// </para>
      /// </remarks>
      public interface class IGemFireCache : IRegionService
      {
      public:        
        
        /// <summary>
        /// Returns the name of this cache.
        /// </summary>
        /// <remarks>
        /// This method does not throw
        /// <c>CacheClosedException</c> if the cache is closed.
        /// </remarks>
        /// <returns>the string name of this cache</returns>
        property String^ Name
        {
          String^ get( );
        }

        /// <summary>
        /// Initializes the cache from an XML file.
        /// </summary>
        /// <param name="cacheXml">pathname of a <c>cache.xml</c> file</param>
        void InitializeDeclarativeCache( String^ cacheXml );

        /// <summary>
        /// Returns the distributed system used to
        /// <see cref="CacheFactory.Create" /> this cache.
        /// </summary>
        /// <remarks>
        /// This method does not throw
        /// <c>CacheClosedException</c> if the cache is closed.
        /// </remarks>
        property DistributedSystem^ DistributedSystem
        {
          GemStone::GemFire::Cache::Generic::DistributedSystem^ get( );
        } 

        /// <summary>
        /// Returns the cache transaction manager of
        /// <see cref="CacheFactory.Create" /> this cache.
        /// </summary>
        property GemStone::GemFire::Cache::Generic::CacheTransactionManager^ CacheTransactionManager
        {
          GemStone::GemFire::Cache::Generic::CacheTransactionManager^ get( );
        }

				///<summary>
				/// Returns whether Cache saves unread fields for Pdx types.
				///</summary>
				bool GetPdxIgnoreUnreadFields();

        ///<summary>
        /// Returns whether { @link PdxInstance} is preferred for PDX types instead of .NET object.
        ///</summary>
        bool GetPdxReadSerialized();
      };
      } // end namespace Generic      
    }
  }
}
