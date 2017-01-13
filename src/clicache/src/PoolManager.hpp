/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/PoolManager.hpp>
//#include "impl/NativeWrapper.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      //generic<class TKey, class TValue>
      ref class Pool;
     // generic<class TKey, class TValue>
      ref class PoolFactory;

      /// <summary>
      /// This interface provides for the configuration and creation of instances of PoolFactory.
      /// </summary>
     // generic<class TKey, class TValue>
      public ref class PoolManager STATICCLASS
      {
      public:

        /// <summary>
        /// Creates a new PoolFactory which is used to configure and create Pools.
        /// </summary>
        static PoolFactory/*<TKey, TValue>*/^ CreateFactory();

        /// <summary>
        /// Returns a map containing all the pools in this manager.
        /// The keys are pool names and the values are Pool instances.
        /// </summary>
        static const Dictionary<String^, Pool/*<TKey, TValue>*/^>^ GetAll();

        /// <summary>
        /// Find by name an existing connection pool.
        /// </summary>
        static Pool/*<TKey, TValue>*/^ Find(String^ name);

        /// <summary>
        /// Find the pool used by the given region.
        /// </summary>
        static Pool/*<TKey, TValue>*/^ Find(Generic::Region<Object^, Object^>^ region);

        /// <summary>
        /// Destroys all created pools.
        /// </summary>
        static void Close(Boolean KeepAlive);

        /// <summary>
        /// Destroys all created pools.
        /// </summary>
        static void Close();
      };
    }
  }
}

 } //namespace 
