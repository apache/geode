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
#include "ICqListener.hpp"
//#include "impl/NativeWrapper.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

  generic<class TKey, class TResult>
	ref class CqEvent;
	//interface class ICqListener;

      /// <summary>
      /// This class wraps the native C++ <c>apache::geode::client::Serializable</c> objects
      /// as managed <see cref="../../IGFSerializable" /> objects.
      /// </summary>
      generic<class TKey, class TResult>    
      public ref class CqListener
        : public Internal::SBWrap<apache::geode::client::CqListener>, public ICqListener<TKey, TResult>
      {
      public:

        /// <summary>
        /// Invoke on an event
        /// </summary>
	virtual void OnEvent( CqEvent<TKey, TResult>^ ev) 
	{
	}

        /// <summary>
        /// Invoke on an error
        /// </summary>
	virtual void OnError( CqEvent<TKey, TResult>^ ev) 
	{
	}

        /// <summary>
        /// Invoke on close
        /// </summary>
	virtual void Close()
	{
	}

      internal:

        /// <summary>
        /// Default constructor.
        /// </summary>
        inline CqListener<TKey, TResult>( )
          : SBWrap( ) { }

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqListener<TKey, TResult>( apache::geode::client::CqListener* nativeptr )
          : SBWrap( nativeptr ) { }

        /// <summary>
        /// Used to assign the native Serializable pointer to a new object.
        /// </summary>
        /// <remarks>
        /// Note the order of preserveSB() and releaseSB(). This handles the
        /// corner case when <c>m_nativeptr</c> is same as <c>nativeptr</c>.
        /// </remarks>
        inline void AssignSP( apache::geode::client::CqListener* nativeptr )
        {
          AssignPtr( nativeptr );
        }

        /// <summary>
        /// Used to assign the native CqListener pointer to a new object.
        /// </summary>
        inline void SetSP( apache::geode::client::CqListener* nativeptr )
        {
          if ( nativeptr != nullptr ) {
            nativeptr->preserveSB( );
          }
          _SetNativePtr( nativeptr );
        }

      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

