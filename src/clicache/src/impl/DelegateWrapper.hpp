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

#include "../gf_defs.hpp"
#include "../Serializable.hpp"
#include "ManagedCacheableKey.hpp"
#include "SafeConvert.hpp"
#include "../Log.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      /// <summary>
      /// Template class to wrap a managed <see cref="TypeFactoryMethod" />
      /// delegate that returns an <see cref="IGFSerializable" /> object. It contains
      /// a method that converts the managed object gotten by invoking the
      /// delegate to the native <c>apache::geode::client::Serializable</c> object
      /// (using the provided wrapper class constructor).
      /// </summary>
      /// <remarks>
      /// This class is to enable interopibility between the managed and unmanaged
      /// worlds when registering types.
      /// In the managed world a user would register a managed type by providing
      /// a factory delegate returning an object of that type. However, the
      /// native implementation requires a factory function that returns an
      /// object implementing <c>apache::geode::client::Serializable</c>. Normally this would not
      /// be possible since we require to dynamically generate a new function
      /// for a given delegate.
      ///
      /// Fortunately in the managed world the delegates contain an implicit
      /// 'this' pointer. Thus we can have a universal delegate that contains
      /// the given managed delegate (in the 'this' pointer) and returns the
      /// native <c>apache::geode::client::Serializable</c> object. Additionally marshalling
      /// services provide <c>Marshal.GetFunctionPointerForDelegate</c> which gives
      /// a function pointer for a delegate which completes the conversion.
      /// </remarks>
      ref class DelegateWrapperGeneric
      {
      public:

        /// <summary>
        /// Constructor to wrap the given managed delegate.
        /// </summary>
        inline DelegateWrapperGeneric( TypeFactoryMethodGeneric^ typeDelegate )
          : m_delegate( typeDelegate ) { }

        /// <summary>
        /// Returns the native <c>apache::geode::client::Serializable</c> object by invoking the
        /// managed delegate provided in the constructor.
        /// </summary>
        /// <returns>
        /// Native <c>apache::geode::client::Serializable</c> object after invoking the managed
        /// delegate and wrapping inside a <c>ManagedCacheableKey</c> object.
        /// </returns>
        apache::geode::client::Serializable* NativeDelegateGeneric( )
        {
          IGFSerializable^ tempObj = m_delegate( );
          IGFDelta^ tempDelta =
            dynamic_cast<IGFDelta^>(tempObj);
          if( tempDelta != nullptr )
          {
            if(!SafeConvertClassGeneric::isAppDomainEnabled)
              return new apache::geode::client::ManagedCacheableDeltaGeneric( tempDelta );
            else
              return new apache::geode::client::ManagedCacheableDeltaBytesGeneric( tempDelta, false );
          }
          else if(!SafeConvertClassGeneric::isAppDomainEnabled)
            return new apache::geode::client::ManagedCacheableKeyGeneric( tempObj);
          else
            return new apache::geode::client::ManagedCacheableKeyBytesGeneric( tempObj, false);
        }


      private:

        TypeFactoryMethodGeneric^ m_delegate;
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache
