/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
            GemStone::GemFire::Cache::Generic::TransactionEvent<TKey, TValue> gevent(GetNativePtr<gemfire::TransactionEvent>(event));
            m_listener->AfterCommit(%gevent);
            
          }

          virtual void AfterFailedCommit(GemStone::GemFire::Cache::TransactionEvent^ event) override 
          {
            GemStone::GemFire::Cache::Generic::TransactionEvent<TKey, TValue> gevent(GetNativePtr<gemfire::TransactionEvent>(event));
            m_listener->AfterFailedCommit(%gevent);
          }

          virtual void AfterRollback(GemStone::GemFire::Cache::TransactionEvent^ event) override 
          {
            GemStone::GemFire::Cache::Generic::TransactionEvent<TKey, TValue> gevent(GetNativePtr<gemfire::TransactionEvent>(event));
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