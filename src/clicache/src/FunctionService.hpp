/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/FunctionService.hpp>
#include "Cache.hpp"
//#include "impl/NativeWrapper.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
    //  generic<class TKey, class TValue>
      ref class Pool;
     
      generic<class TResult>
      ref class Execution;

      /// <summary>
      /// A factory class used to create Execute object for function execution
      /// </summary>
      /// <remarks>
      generic<class TResult>
      public ref class FunctionService 
        : public Internal::SBWrap<gemfire::FunctionService>
      {
      public:

        /// <summary>
        /// Creates a new region Execution object
        /// </summary>
        /// <remarks>
        /// If Pool is multiusersecure mode then one need to pass logical instance of Region Pool->CreateSecureUserCache(<credentials>)->getRegion(<regionPath>).
        /// </remarks>
        generic <class TKey, class TValue>
        static Execution<TResult>^ OnRegion( IRegion<TKey, TValue>^ rg );

        /// <summary>
        /// Creates a new Execution object on one server
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <exception cref="UnsupportedOperationException">unsupported operation exception, when Pool is in multiusersecure mode.</exception>
        //generic<class TKey, class TValue>
        static Execution<TResult>^ OnServer( Pool/*<TKey, TValue>*/^ pl );

        /// <summary>
        /// Creates a new Execution object on all servers in the pool
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <exception cref="UnsupportedOperationException">unsupported operation exception, when Pool is in multiusersecure mode.</exception>
        //generic<class TKey, class TValue>
        static Execution<TResult>^ OnServers( Pool/*<TKey, TValue>*/^ pl );

        /// <summary>
        /// Creates a new Execution object on one server.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <exception cref="IllegalStateException">when Pool has been closed.</exception>
        //generic<class TResult>
        static Execution<TResult>^ OnServer( IRegionService^ cache );

        /// <summary>
        /// Creates a new Execution object on all servers in the pool.
        /// </summary>
        /// <remarks>
        /// </remarks>
        /// <exception cref="IllegalStateException">when Pool has been closed.</exception>
        //generic<class TResult>
        static Execution<TResult>^ OnServers( IRegionService^ cache );
        
      internal:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline FunctionService( gemfire::FunctionService* nativeptr )
          : SBWrap( nativeptr ) { }

      };

    }
  }
}
 } //namespace 
