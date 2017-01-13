/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/CqAttributes.hpp>
#include "impl/NativeWrapper.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      generic<class TKey, class TResult>
      interface class ICqListener;

      /// <summary>
      /// Defines attributes for configuring a cq.
      /// </summary>
      generic<class TKey, class TResult>
      public ref class CqAttributes sealed
        : public Internal::SBWrap<gemfire::CqAttributes>
      {
      public:

        /// <summary>
        /// get all listeners in this attributes
        /// </summary>
        virtual array<Generic::ICqListener<TKey, TResult>^>^ getCqListeners();

      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static CqAttributes<TKey, TResult>^ Create( gemfire::CqAttributes* nativeptr )
        {
          if (nativeptr == nullptr)
          {
            return nullptr;
          }
          return gcnew CqAttributes( nativeptr );
        }


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqAttributes( gemfire::CqAttributes* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
 } //namespace 
