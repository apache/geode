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
      /// A callback that is allowed to veto a transaction. Only one TransactionWriter can exist
      /// per cache, and only one TransactionWriter will be fired in the
      /// entire distributed system for each transaction.
      /// This writer can be used to update a backend data source before the Geode 
      /// cache is updated during commit. If the backend update fails, the implementer 
      /// can throw a {<c>TransactionWriterException</c>} to veto the transaction.
      /// </summary>
      /// <seealso cref="CacheTransactionManager.SetWriter" />
      generic<class TKey, class TValue>
      public interface class ITransactionWriter
      {
      public:
        /// <summary>
        /// Called before the transaction has finished committing, but after conflict checking.
        /// Provides an opportunity for implementors to cause transaction abort by throwing a
        /// TransactionWriterException
        /// </summary>
        /// <param name="te">the transaction event</param>
        /// <exception cref="TransactionWriterException">
        /// in the event that the transaction should be rolled back
        /// </exception>
        /// <seealso cref="CacheTransactionManager.Commit" />
        void BeforeCommit(TransactionEvent<TKey, TValue>^ te);

      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

#endif
