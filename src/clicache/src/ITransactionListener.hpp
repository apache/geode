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
#ifdef CSTX_COMMENTED
#pragma once

#include "gf_defs.hpp"
#include "TransactionEvent.hpp"


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      /// <summary>
      /// An application plug-in that can be installed for getting events for 
      /// cache transactions. 
      /// </summary>
      /// <seealso cref="CacheTransactionManager.AddListener" />
      generic<class TKey, class TValue>
      public interface class ITransactionListener
      {
      public:
        
        /// <summary>
        /// Called after a successful commit of a transaction.
        /// </summary>
        /// <param name="te">the transaction event</param>
        /// <seealso cref="CacheTransactionManager.Commit" />
        void AfterCommit(TransactionEvent<TKey, TValue>^ te);

	      /// <summary>
        /// Called after an unsuccessful commit operation.
        /// </summary>
        /// <param name="te">the transaction event</param>
        /// <seealso cref="CacheTransactionManager.Commit" />
        void AfterFailedCommit(TransactionEvent<TKey, TValue>^ te);

	      /// <summary>
        /// Called after an explicit rollback of a transaction.
        /// </summary>
        /// <param name="te">the transaction event</param>
        /// <seealso cref="CacheTransactionManager.Commit" />
        /// <seealso cref="CacheTransactionManager.Rollback" />
        void AfterRollback(TransactionEvent<TKey, TValue>^ te);
    
        /// <summary>
        /// alled when the region containing this callback is closed or destroyed, when
        /// the cache is closed, or when a callback is removed from a region
        /// using an <code>AttributesMutator</code>.
	      /// Implementations should cleanup any external
	      /// resources such as database connections. Any runtime exceptions this method
	      /// throws will be logged.
	      /// It is possible for this method to be called multiple times on a single
	      /// callback instance, so implementations must be tolerant of this.
	      /// </summary>
        /// <seealso cref="Cache.Close" />
        /// <seealso cref="Region.Close" />
        /// <seealso cref="Region.LocalDestroyRegion" />
        /// <seealso cref="Region.DestroyRegion" />
        void Close();
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

#endif