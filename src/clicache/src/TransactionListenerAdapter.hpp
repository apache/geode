/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifdef CSTX_COMMENTED
#pragma once

#include "gf_defs.hpp"
#include "ITransactionListener.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Utility class that implements all methods in <c>ITransactionListener</c>
      /// with empty implementations. Applications can subclass this class
      /// and only override the methods for the events of interest.
      /// </summary>
      generic<class TKey, class TValue>
      public ref class TransactionListenerAdapter
        : public GemStone::GemFire::Cache::Generic::ITransactionListener<TKey, TValue>
      {
      public:
        virtual void AfterCommit(GemStone::GemFire::Cache::Generic::TransactionEvent<TKey, TValue>^ te)
        {
        }

	      virtual void AfterFailedCommit(GemStone::GemFire::Cache::Generic::TransactionEvent<TKey, TValue>^ te)
        {
        }

	      virtual void AfterRollback(GemStone::GemFire::Cache::Generic::TransactionEvent<TKey, TValue>^ te)
        {
        }
    
        virtual void Close()
        {
        }
      };

    }
  }
}
 } //namespace 
#endif