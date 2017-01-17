/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
