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
#include "../TransactionWriterAdapter.hpp"
#include "../ITransactionWriter.hpp"
#include "SafeConvert.hpp"

using namespace System;
using namespace GemStone::GemFire::Cache;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Contains the generic writer object. Inherits from non generic writer interface.
      /// This class is used to hide the generic implementation from cpp and at the same time
      /// forward the calls to the generic objects
      /// </summary>
      generic<class TKey, class TValue>
			public ref class TransactionWriterGeneric : GemStone::GemFire::Cache::TransactionWriterAdapter
      {
        private:

          GemStone::GemFire::Cache::Generic::ITransactionWriter<TKey, TValue>^ m_writer;

        public:

          void SetTransactionWriter(GemStone::GemFire::Cache::Generic::ITransactionWriter<TKey, TValue>^ writer)
          {
            m_writer = writer;
          }

          virtual void BeforeCommit(GemStone::GemFire::Cache::TransactionEvent^ te) override
          {
            GemStone::GemFire::Cache::Generic::TransactionEvent<TKey, TValue> gevent(GetNativePtr<gemfire::TransactionEvent>(te));
            m_writer->BeforeCommit(%gevent);
          }

      };
    }
    }
  }
}
#endif