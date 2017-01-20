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

#include "../gf_defs.hpp"
#include <vcclr.h>
#include <cppcache/TransactionWriter.hpp>
#include "../ITransactionWriter.hpp"

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
interface class ITransactionWriter;
    }
  }
}

namespace apache {
  namespace geode {
    namespace client {

      /// <summary>
      /// Wraps the managed <see cref="Apache.Geode.Client.ITransactionWriter" />
      /// object and implements the native <c>apache::geode::client::TransactionWriter</c> interface.
      /// </summary>
      class ManagedTransactionWriterGeneric
        : public apache::geode::client::TransactionWriter
      {
      public:

        /// <summary>
        /// Constructor to initialize with the provided managed object.
        /// </summary>
        /// <param name="userptr">
        /// The managed object.
        /// </param>
        inline ManagedTransactionWriterGeneric(Object^ userptr )
          : m_userptr( userptr ) { }

        static apache::geode::client::TransactionWriter* create( const char* assemblyPath,
          const char* factoryFunctionName );

        virtual ~ManagedTransactionWriterGeneric( ) { }

        virtual void beforeCommit(apache::geode::client::TransactionEventPtr& te);

        inline Apache::Geode::Client::ITransactionWriter^ ptr( ) const
        {
          return m_managedptr;
        }

        inline void setptr( Apache::Geode::Client::ITransactionWriter^ managedptr )
        {
          m_managedptr = managedptr;
        }

        inline Object^ userptr( ) const
        {
          return m_userptr;
        }

      private:

        /// <summary>
        /// Using gcroot to hold the managed delegate pointer (since it cannot be stored directly).
        /// Note: not using auto_gcroot since it will result in 'Dispose' of the ITransactionWriter
        /// to be called which is not what is desired when this object is destroyed. Normally this
        /// managed object may be created by the user and will be handled automatically by the GC.
        /// </summary>
        gcroot<Apache::Geode::Client::ITransactionWriter^> m_managedptr;

        gcroot<Object^> m_userptr;
      };

    }  // namespace client
  }  // namespace geode
}  // namespace apache
#endif