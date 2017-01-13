/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "../gf_defs.hpp"
#include "../Serializable.hpp"
#include "ManagedCacheableKey.hpp"
#include "SafeConvert.hpp"
#include "../Log.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Template class to wrap a managed <see cref="TypeFactoryMethod" />
      /// delegate that returns an <see cref="IGFSerializable" /> object. It contains
      /// a method that converts the managed object gotten by invoking the
      /// delegate to the native <c>gemfire::Serializable</c> object
      /// (using the provided wrapper class constructor).
      /// </summary>
      /// <remarks>
      /// This class is to enable interopibility between the managed and unmanaged
      /// worlds when registering types.
      /// In the managed world a user would register a managed type by providing
      /// a factory delegate returning an object of that type. However, the
      /// native implementation requires a factory function that returns an
      /// object implementing <c>gemfire::Serializable</c>. Normally this would not
      /// be possible since we require to dynamically generate a new function
      /// for a given delegate.
      ///
      /// Fortunately in the managed world the delegates contain an implicit
      /// 'this' pointer. Thus we can have a universal delegate that contains
      /// the given managed delegate (in the 'this' pointer) and returns the
      /// native <c>gemfire::Serializable</c> object. Additionally marshalling
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
        /// Returns the native <c>gemfire::Serializable</c> object by invoking the
        /// managed delegate provided in the constructor.
        /// </summary>
        /// <returns>
        /// Native <c>gemfire::Serializable</c> object after invoking the managed
        /// delegate and wrapping inside a <c>ManagedCacheableKey</c> object.
        /// </returns>
        gemfire::Serializable* NativeDelegateGeneric( )
        {
          IGFSerializable^ tempObj = m_delegate( );
          IGFDelta^ tempDelta =
            dynamic_cast<IGFDelta^>(tempObj);
          if( tempDelta != nullptr )
          {
            if(!SafeConvertClassGeneric::isAppDomainEnabled)
              return new gemfire::ManagedCacheableDeltaGeneric( tempDelta );
            else
              return new gemfire::ManagedCacheableDeltaBytesGeneric( tempDelta, false );
          }
          else if(!SafeConvertClassGeneric::isAppDomainEnabled)
            return new gemfire::ManagedCacheableKeyGeneric( tempObj);
          else
            return new gemfire::ManagedCacheableKeyBytesGeneric( tempObj, false);
        }


      private:

        TypeFactoryMethodGeneric^ m_delegate;
      };

    }
  }
}
}