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

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/CqAttributesMutator.hpp>
#include "impl/NativeWrapper.hpp"


using namespace System;
using namespace System::Collections::Generic;
using namespace System::Threading;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      generic<class TKey, class TResult>
	  interface class ICqListener;

      generic<class TKey, class TResult>
      private ref class CqListenerHelper sealed{
        public:
        static IDictionary<Client::ICqListener<TKey, TResult>^, IntPtr>^
          m_ManagedVsUnManagedCqLstrDict = gcnew 
          Dictionary<Client::ICqListener<TKey, TResult>^, IntPtr>();

        static ReaderWriterLock^ g_readerWriterLock = gcnew ReaderWriterLock();
      };

      /// <summary>
      /// Supports modification of certain cq attributes after the cq
      /// has been created.
      /// </summary>
      generic<class TKey, class TResult>
      public ref class CqAttributesMutator sealed
        : public Internal::SBWrap<apache::geode::client::CqAttributesMutator>
      {
      public:

        /// <summary>
        /// Adds the CqListener for the cq.
        /// </summary>
        /// <param name="cqListener">
        /// user-defined cq listener, or null for no cache listener
        /// </param>
        void AddCqListener( Client::ICqListener<TKey, TResult>^ cqListener );

        /// <summary>
        /// Remove a CqListener for the cq.
        /// </summary>
        
        void RemoveCqListener(Client::ICqListener<TKey, TResult>^ aListener);


        /// <summary>
	/// Initialize with an array of listeners
        /// </summary>
        
        void SetCqListeners(array<Client::ICqListener<TKey, TResult>^>^ newListeners);


      internal:
        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static Client::CqAttributesMutator<TKey, TResult>^ Create( apache::geode::client::CqAttributesMutator* nativeptr )
        {
          if (nativeptr == nullptr)
          {
            return nullptr;
          }
          return gcnew Client::CqAttributesMutator<TKey, TResult>( nativeptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqAttributesMutator<TKey, TResult>( apache::geode::client::CqAttributesMutator* nativeptr )
          : SBWrap( nativeptr ) { }
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

