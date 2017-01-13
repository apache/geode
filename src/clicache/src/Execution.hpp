/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/Execution.hpp>
#include "impl/NativeWrapper.hpp"
//#include "impl/ResultCollectorProxy.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache {
      //interface class IGFSerializable;
      namespace Generic
    {
      generic<class TResult>
      interface class IResultCollector;

      generic<class TResult>
      ref class ResultCollector;

      /// <summary>
      /// This class encapsulates events that occur for cq.
      /// </summary>
      generic<class TResult>
      public ref class Execution sealed
        : public Internal::SBWrap<gemfire::Execution>
      {
      public:
        /// <summary>
		/// Add a routing object, 
        /// Return self.
        /// </summary>
		generic<class TFilter>
    Execution<TResult>^ WithFilter(System::Collections::Generic::ICollection<TFilter>^ routingObj);

        /// <summary>
		/// Add an argument, 
        /// Return self.
        /// </summary>
    generic<class TArgs>
		Execution<TResult>^ WithArgs(TArgs args);

        /// <summary>
		/// Add a result collector, 
        /// Return self.
        /// </summary>
		Execution<TResult>^ WithCollector(IResultCollector<TResult>^ rc);

        /// <summary>
        /// Execute a function, 
        /// Return resultCollector.
        /// </summary>
		/// <param name="timeout"> Value to wait for the operation to finish before timing out.</param> 
        IResultCollector<TResult>^ Execute(String^ func, UInt32 timeout);

        /// <summary>
        /// Execute a function, 
        /// Return resultCollector.
        /// </summary>
        IResultCollector<TResult>^ Execute(String^ func);

      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static Execution<TResult>^ Create( gemfire::Execution* nativeptr, IResultCollector<TResult>^ rc )
        {
          return ( nativeptr != nullptr ?
            gcnew Execution<TResult>( nativeptr, rc ) : nullptr );
	}

        /// <summary>
        /// Private constructor to wrap a native object pointer.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Execution( gemfire::Execution* nativeptr, IResultCollector<TResult>^ rc )
          : SBWrap( nativeptr ) { m_rc = rc;}
      private:
        IResultCollector<TResult>^ m_rc;
      };

    }
  }
}
 } //namespace 
