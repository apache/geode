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
#include "ITransactionWriter.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Utility class that implements all methods in <c>ITransactionWriter</c>
      /// with empty implementations.
      /// </summary>
      generic<class TKey, class TValue>
      public ref class TransactionWriterAdapter
        : public GemStone::GemFire::Cache::Generic::ITransactionWriter<TKey, TValue>
      {
      public:
        virtual void BeforeCommit(TransactionEvent<TKey, TValue>^ te)
        {
        }
      };

    }
  }
}
 } //namespace 
#endif