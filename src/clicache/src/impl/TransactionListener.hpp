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



//#include "../gf_includes.hpp"
#include "../TransactionListenerAdapter.hpp"
#include "../ITransactionListener.hpp"
#include "../TransactionEvent.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Contains the generic listener object. Inherits from non generic listener interface.
      /// This class is used to hide the generic implementation from cpp and at the same time
      /// forward the calls to the generic objects
      /// </summary>
      generic<class TKey, class TValue>
			public ref class TransactionListenerGeneric : GemStone::GemFire::Cache::TransactionListenerAdapter
      {
        private:

					GemStone::GemFire::Cache::Generic::ITransactionListener<TKey, TValue>^ m_listener;

        public:

          void SetTransactionListener(GemStone::GemFire::Cache::Generic::ITransactionListener<TKey, TValue>^ listener)
          {
            m_listener = listener;
          }

					virtual void AfterCommit(GemStone::GemFire::Cache::TransactionEvent^ event) override 
          {
            GemStone::GemFire::Cache::Generic::TransactionEvent<TKey, TValue> gevent(GetNativePtr<apache::geode::client::TransactionEvent>(event));
            m_listener->AfterCommit(%gevent);
            
          }

          virtual void AfterFailedCommit(GemStone::GemFire::Cache::TransactionEvent^ event) override 
          {
            GemStone::GemFire::Cache::Generic::TransactionEvent<TKey, TValue> gevent(GetNativePtr<apache::geode::client::TransactionEvent>(event));
            m_listener->AfterFailedCommit(%gevent);
          }

          virtual void AfterRollback(GemStone::GemFire::Cache::TransactionEvent^ event) override 
          {
            GemStone::GemFire::Cache::Generic::TransactionEvent<TKey, TValue> gevent(GetNativePtr<apache::geode::client::TransactionEvent>(event));
            m_listener->AfterRollback(%gevent);
          }

          virtual void Close()  override 
          {
            m_listener->Close();
          }
      };
    }
    }
  }
}
#endif