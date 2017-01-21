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
#include <gfcpp/CqEvent.hpp>
#include "CqQuery.hpp"
#include "CqOperation.hpp"
//#include "impl/NativeWrapper.hpp"

#include "ICqEvent.hpp"
#include "ICacheableKey.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

			interface class IGFSerializable;
      //interface class ICqEvent;
      //interface class ICacheableKey;

      /// <summary>
      /// This class encapsulates events that occur for cq.
      /// </summary>
      generic<class TKey, class TResult>
      public ref class CqEvent sealed
        : public Internal::UMWrap<apache::geode::client::CqEvent>
      {
      public:


        /// <summary>
        /// Return the cqquery this event occurred in.
        /// </summary>
	      CqQuery<TKey, TResult>^ getCq();

        /// <summary>
        /// Get the operation on the base operation that triggered this event.
        /// </summary>
       CqOperationType getBaseOperation();

        /// <summary>
        /// Get the operation on the query operation that triggered this event.
        /// </summary>
       CqOperationType getQueryOperation();

        /// <summary>
        /// Get the key relating to the event.
        /// In case of REGION_CLEAR and REGION_INVALIDATE operation, the key will be null.
        /// </summary>
        TKey /*Generic::ICacheableKey^*/ getKey( );

        /// <summary>
        /// Get the new value of the modification.
        /// If there is no new value returns null, this will happen during delete
        /// operation.
        /// </summary>
        /*Object^*/ TResult getNewValue( );

        array< Byte >^ getDeltaValue( );

      internal:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqEvent( const apache::geode::client::CqEvent* nativeptr )
          : UMWrap( const_cast<apache::geode::client::CqEvent*>( nativeptr ), false ) { }
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

