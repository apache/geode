/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"

using namespace System;
//using namespace System::Runtime::InteropServices;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
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
