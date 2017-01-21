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
#include <gfcpp/CqAttributesFactory.hpp>
//#include "impl/NativeWrapper.hpp"
#include "impl/SafeConvert.hpp"

#include "CqAttributes.hpp"

using namespace System;
using namespace System::Collections::Generic;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      /*
      generic<class TKey, class TValue>
      ref class CqAttributes;
      */
      generic<class TKey, class TResult>
      interface class ICqListener;

      /// <summary>
      /// Creates instances of <c>CqAttributes</c>.
      /// </summary>
      /// <seealso cref="CqAttributes" />
      generic<class TKey, class TResult>
      public ref class CqAttributesFactory sealed
        : public Internal::UMWrap<apache::geode::client::CqAttributesFactory>
      {
      public:

        /// <summary>
        /// Creates a new instance of <c>CqAttributesFactory</c> ready
        /// to create a <c>CqAttributes</c> with default settings.
        /// </summary>
        inline CqAttributesFactory( )
          : UMWrap( new apache::geode::client::CqAttributesFactory( ), true )
        { }

        inline CqAttributesFactory(Client::CqAttributes<TKey, TResult>^ cqAttributes )
          : UMWrap( new apache::geode::client::CqAttributesFactory(apache::geode::client::CqAttributesPtr(GetNativePtrFromSBWrapGeneric<apache::geode::client::CqAttributes>(cqAttributes ))), true )
        { }

        // ATTRIBUTES

        /// <summary>
        /// add a cqListener 
        /// </summary>
        void AddCqListener(Client::ICqListener<TKey, TResult>^ cqListener);

        /// <summary>
        /// Initialize with an array of listeners
        /// </summary>
        void InitCqListeners( array<Client::ICqListener<TKey, TResult>^>^ cqListeners );

        // FACTORY METHOD

        /// <summary>
        /// Creates a <c>CqAttributes</c> with the current settings.
        /// </summary>
        Client::CqAttributes<TKey, TResult>^ Create( );
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

