/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */



#pragma once

#include "../../gf_defs.hpp"
#include <cppcache/ScopeType.hpp>


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Enumerated type for region distribution scope.
      /// Contains values for setting <c>Scope</c>.
      /// Local scope is invalid (it is a non-native client local region), and
      /// DistributedAck and DistributedNoAck have the same behavior.
      /// </summary>
      public enum class ScopeType
      {
        /// <summary>No distribution.</summary>
        Local = 0,

        /// <summary>
        /// Distribute without waiting for acknowledgement.
        /// </summary>
        DistributedNoAck,

        /// <summary>
        /// Distribute and wait for all peers to acknowledge.
        /// </summary>
        DistributedAck,

        /// <summary>
        /// Distribute with full interprocess synchronization
        /// -- NOT IMPLEMENTED.
        /// </summary>
        Global,

        /// <summary>Invalid scope.</summary>
        Invalid
      };


      /// <summary>
      /// Static class containing convenience methods for <c>ScopeType</c>.
      /// </summary>
      /// <seealso cref="RegionAttributes.Scope" />
      /// <seealso cref="AttributesFactory.SetScope" />
      public ref class Scope STATICCLASS
      {
      public:

        /// <summary>
        /// True if the given scope is local.
        /// </summary>
        /// <param name="type">scope</param>
        /// <returns>true if <c>Local</c></returns>
        inline static bool IsLocal( ScopeType type )
        {
          return (type == ScopeType::Local);
        }

        /// <summary>
        /// True if the given scope is one of the distributed scopes.
        /// </summary>
        /// <param name="type">scope</param>
        /// <returns>
        /// true if other than <c>Local</c>; could be <c>Invalid</c>
        /// </returns>
        inline static bool IsDistributed( ScopeType type ) 
        {
          return (type != ScopeType::Local);
        }

        /// <summary>
        /// True if the given scope is distributed-no-ack.
        /// </summary>
        /// <param name="type">scope</param>
        /// <returns>true if <c>DistributedNoAck</c></returns>
        inline static bool IsDistributedNoAck( ScopeType type ) 
        {
          return (type == ScopeType::DistributedNoAck);
        }

        /// <summary>
        /// True if the given scope is distributed-ack.
        /// </summary>
        /// <param name="type">scope</param>
        /// <returns>true if <c>DistributedAck</c></returns>
        inline static bool IsDistributedAck( ScopeType type ) 
        {
          return (type == ScopeType::DistributedAck);
        }

        ///// <summary>
        ///// True if the given scope is global.
        ///// </summary>
        ///// <param name="type">scope</param>
        ///// <returns>true if <c>Global</c></returns>
        //inline static bool IsGlobal( ScopeType type ) 
        //{
        //  return (type == ScopeType::Global);
        //}

        /// <summary>
        /// True if acknowledgements are required for the given scope.
        /// </summary>
        /// <param name="type">scope</param>
        /// <returns>
        /// true if <c>DistributedAck</c>, false otherwise
        /// </returns>
        inline static bool IsAck( ScopeType type )
        {
          return (type == ScopeType::DistributedAck);
        }
      };

    }
  }
}
 } //namespace 

