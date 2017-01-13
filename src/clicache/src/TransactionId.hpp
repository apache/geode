/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/
#pragma once

#include "gf_defs.hpp"
#include <gfcpp/TransactionId.hpp>
#include "impl/NativeWrapper.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache 
    {
      namespace Generic
      {
        /// <summary>
        /// This class encapsulates Id of a transaction.
        /// </summary>
        public ref class TransactionId sealed
          : public Internal::SBWrap<gemfire::TransactionId>
        {
        internal:

          inline static TransactionId^ Create( gemfire::TransactionId* nativeptr )
          {
            return ( nativeptr != nullptr ?
              gcnew TransactionId( nativeptr ) : nullptr );
          }

        private:

          /// <summary>
          /// Private constructor to wrap a native object pointer
          /// </summary>
          /// <param name="nativeptr">The native object pointer</param>
          inline TransactionId( gemfire::TransactionId* nativeptr )
            : SBWrap( nativeptr ) { }
        };
      }

    }
  }
} //namespace 
