/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/CqAttributesFactory.hpp>
//#include "impl/NativeWrapper.hpp"
#include "impl/SafeConvert.hpp"

#include "CqAttributes.hpp"

using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
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
        : public Internal::UMWrap<gemfire::CqAttributesFactory>
      {
      public:

        /// <summary>
        /// Creates a new instance of <c>CqAttributesFactory</c> ready
        /// to create a <c>CqAttributes</c> with default settings.
        /// </summary>
        inline CqAttributesFactory( )
          : UMWrap( new gemfire::CqAttributesFactory( ), true )
        { }

        inline CqAttributesFactory(Generic::CqAttributes<TKey, TResult>^ cqAttributes )
          : UMWrap( new gemfire::CqAttributesFactory(gemfire::CqAttributesPtr(GetNativePtrFromSBWrapGeneric<gemfire::CqAttributes>(cqAttributes ))), true )
        { }

        // ATTRIBUTES

        /// <summary>
        /// add a cqListener 
        /// </summary>
        void AddCqListener(Generic::ICqListener<TKey, TResult>^ cqListener);

        /// <summary>
        /// Initialize with an array of listeners
        /// </summary>
        void InitCqListeners( array<Generic::ICqListener<TKey, TResult>^>^ cqListeners );

        // FACTORY METHOD

        /// <summary>
        /// Creates a <c>CqAttributes</c> with the current settings.
        /// </summary>
        Generic::CqAttributes<TKey, TResult>^ Create( );
      };

    }
  }
}
 } //namespace 
