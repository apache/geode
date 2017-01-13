/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/CqState.hpp>
#include "impl/NativeWrapper.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Enumerated type for cq state
      /// @nativeclient
      /// For Native Clients:
      /// @endnativeclient      
      /// </summary>
      public enum class CqStateType
      {
        STOPPED = 0,
        RUNNING,
        CLOSED,
        CLOSING,
        INVALID
      };


      /// <summary>
      /// Static class containing convenience methods for <c>CqState</c>.
      /// </summary>
      public ref class CqState sealed
        : public Internal::UMWrap<gemfire::CqState>
      {
      public:

        /// <summary>
        /// Returns the state in string form.
        /// </summary>
        virtual String^ ToString( ) override;

        /// <summary>
        /// Returns true if the CQ is in Running state.
        /// </summary>
        bool IsRunning(); 

        /// <summary>
        /// Returns true if the CQ is in Stopped state.
	/// </summary>
        bool IsStopped();

        /// <summary>
        /// Returns true if the CQ is in Closed state.
        /// </summary>
        bool IsClosed(); 

        /// <summary>
        /// Returns true if the CQ is in Closing state.
	/// </summary>
        bool IsClosing();
	void SetState(CqStateType state);
	CqStateType GetState();

        internal:

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CqState( gemfire::CqState* nativeptr )
		            : UMWrap( nativeptr, false ) { }

      };

    }
  }
}
 } //namespace 
