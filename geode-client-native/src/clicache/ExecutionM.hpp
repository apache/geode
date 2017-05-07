/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/Execution.hpp"
#include "impl/NativeWrapper.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      interface class IGFSerializable;
      interface class IResultCollector;
      ref class ResultCollector;

      /// <summary>
      /// This class encapsulates events that occur for cq.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class Execution sealed
        : public Internal::SBWrap<gemfire::Execution>
      {
      public:
        /// <summary>
		/// Add a routing object, 
        /// Return self.
        /// </summary>
		Execution^ WithFilter(array<IGFSerializable^>^ routingObj);

        /// <summary>
		/// Add an argument, 
        /// Return self.
        /// </summary>
		Execution^ WithArgs(IGFSerializable^ args);

        /// <summary>
		/// Add a result collector, 
        /// Return self.
        /// </summary>
		Execution^ WithCollector(IResultCollector^ rc);

        /// <summary>
        /// Execute a function, 
        /// Return resultCollector.
		/// </summary>
		/// <param name="func"> The name of the function to be executed. </param>
		/// <param name="getResult"> Indicating if results are expected. </param>
		/// <param name="timeout"> Value to wait for the operation to finish before timing out.</param>
		/// <param name="isHA"> Whether the given function is HA. </param>
		/// <param name="optimizeForWrite"> Whether the function should be optmized for write operation. </param>
        /// <deprecated>
        /// parameters hasResult, isHA and optimizeForWrite are deprecated as of NativeClient 3.6, use of these parameters is ignored.
        /// </deprecated>
        IResultCollector^ Execute(String^ func, Boolean getResult, UInt32 timeout, Boolean isHA, Boolean optimizeForWrite);

        /// <summary>
        /// Execute a function, 
		/// Return resultCollector.
        /// </summary>
		/// <param name="func"> The name of the function to be executed. </param>
		/// <param name="getResult"> Indicating if results are expected. </param>
		/// <param name="timeout"> Value to wait for the operation to finish before timing out.</param>
		/// <param name="isHA"> Whether the given function is HA. </param>
        IResultCollector^ Execute(String^ func, Boolean getResult, UInt32 timeout, Boolean isHA);

        /// <summary>
        /// Execute a function, 
        /// Return resultCollector.
        /// </summary>
		/// <param name="getResult"> Indicating if results are expected. </param>
		/// <param name="timeout"> Value to wait for the operation to finish before timing out.</param> 
        IResultCollector^ Execute(String^ func, Boolean getResult, UInt32 timeout);
        
        /// <summary>
		/// Execute a function, 
        /// Return resultCollector.
        /// </summary>
		/// <param name="getResult"> Indicating if results are expected. </param>
        
        IResultCollector^ Execute(String^ func, Boolean getResult);

        /// <summary>
        /// Execute a function, 
        /// Return resultCollector.
        /// </summary>
        IResultCollector^ Execute(String^ func);

      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static Execution^ Create( gemfire::Execution* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew Execution( nativeptr ) : nullptr );
	}

        /// <summary>
        /// Private constructor to wrap a native object pointer.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Execution( gemfire::Execution* nativeptr )
          : SBWrap( nativeptr ) { }
      };

    }
  }
}
