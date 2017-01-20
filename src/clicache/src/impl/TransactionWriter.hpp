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
#include "../TransactionWriterAdapter.hpp"
#include "../ITransactionWriter.hpp"
#include "SafeConvert.hpp"

using namespace System;
using namespace Apache::Geode::Client;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {

      /// <summary>
      /// Contains the generic writer object. Inherits from non generic writer interface.
      /// This class is used to hide the generic implementation from cpp and at the same time
      /// forward the calls to the generic objects
      /// </summary>
      generic<class TKey, class TValue>
			public ref class TransactionWriterGeneric : Apache::Geode::Client::TransactionWriterAdapter
      {
        private:

          Apache::Geode::Client::Generic::ITransactionWriter<TKey, TValue>^ m_writer;

        public:

          void SetTransactionWriter(Apache::Geode::Client::Generic::ITransactionWriter<TKey, TValue>^ writer)
          {
            m_writer = writer;
          }

          virtual void BeforeCommit(Apache::Geode::Client::TransactionEvent^ te) override
          {
            Apache::Geode::Client::Generic::TransactionEvent<TKey, TValue> gevent(GetNativePtr<apache::geode::client::TransactionEvent>(te));
            m_writer->BeforeCommit(%gevent);
          }

      };
    }
    }
  }
}
#endif