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
#include "ITransactionListener.hpp"


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      /// <summary>
      /// Utility class that implements all methods in <c>ITransactionListener</c>
      /// with empty implementations. Applications can subclass this class
      /// and only override the methods for the events of interest.
      /// </summary>
      generic<class TKey, class TValue>
      public ref class TransactionListenerAdapter
        : public Apache::Geode::Client::ITransactionListener<TKey, TValue>
      {
      public:
        virtual void AfterCommit(Apache::Geode::Client::TransactionEvent<TKey, TValue>^ te)
        {
        }

	      virtual void AfterFailedCommit(Apache::Geode::Client::TransactionEvent<TKey, TValue>^ te)
        {
        }

	      virtual void AfterRollback(Apache::Geode::Client::TransactionEvent<TKey, TValue>^ te)
        {
        }
    
        virtual void Close()
        {
        }
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

#endif