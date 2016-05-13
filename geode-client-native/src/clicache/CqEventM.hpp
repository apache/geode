/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/CqEvent.hpp"
#include "CqQueryM.hpp"
#include "CqOperationM.hpp"
#include "impl/NativeWrapper.hpp"

#include "ICqEvent.hpp"
#include "ICacheableKey.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      interface class IGFSerializable;
      //interface class ICqEvent;
      //interface class ICacheableKey;

      /// <summary>
      /// This class encapsulates events that occur for cq.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class CqEvent sealed
        : public Internal::UMWrap<gemfire::CqEvent>
      {
      public:


        /// <summary>
        /// Return the cqquery this event occurred in.
        /// </summary>
	CqQuery^ getCq();

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
       ICacheableKey^ getKey( );

        /// <summary>
        /// Get the new value of the modification.
        /// If there is no new value returns null, this will happen during delete
        /// operation.
        /// </summary>
       IGFSerializable^ getNewValue( );

       array< Byte >^ getDeltaValue( );

      internal:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqEvent( const gemfire::CqEvent* nativeptr )
          : UMWrap( const_cast<gemfire::CqEvent*>( nativeptr ), false ) { }
      };

    }
  }
}
