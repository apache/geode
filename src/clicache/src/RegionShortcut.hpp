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

using namespace System;
//using namespace System::Runtime::InteropServices;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {    
    /// <summary> 
    /// Each enum represents a predefined <see cref="RegionAttributes" /> in a <see cref="Cache" />.
    /// These enum values can be used to create regions using a <see cref="RegionFactory" />
    /// obtained by calling <see cref="Cache.CreateRegionFactory(RegionShortcut) />.
    /// <p>Another way to use predefined region attributes is in cache.xml by setting
    /// the refid attribute on a region element or region-attributes element to the
    /// string of each value.
    /// </summary>
      public enum class RegionShortcut {

        /// <summary>
        /// A PROXY region has no local state and forwards all operations to a server.
        /// </summary>
        PROXY,

        /// <summary>
        /// A CACHING_PROXY region has local state but can also send operations to a server.
        /// If the local state is not found then the operation is sent to the server
        /// and the local state is updated to contain the server result.
        /// </summary>
        CACHING_PROXY,
          
        /// <summary>
        /// A CACHING_PROXY_ENTRY_LRU region has local state but can also send operations to a server.
        /// If the local state is not found then the operation is sent to the server
        /// and the local state is updated to contain the server result.
        /// It will also destroy entries once it detects that the number of enteries crossing default limit of #100000.
        /// </summary>
        CACHING_PROXY_ENTRY_LRU,
        
        /// <summary>
        /// A LOCAL region only has local state and never sends operations to a server.
        /// </summary>
        LOCAL,

       /// <summary>
       /// A LOCAL_ENTRY_LRU region only has local state and never sends operations to a server.
       /// It will also destroy entries once it detects once it detects that the number of enteries crossing default limit of #100000.
       /// </summary>
       LOCAL_ENTRY_LRU
      } ;
    }
  }
}

 } //namespace 
