/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "IGFSerializable.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// This interface class is the superclass of all user objects 
      /// in the cache that can be used as a key.
      /// </summary>
      /// <remarks>
      /// If an implementation is required to act as a key in the cache, then
      /// it must implement this interface and preferably override
      /// <c>System.Object.ToString</c> to obtain proper string representation.
      /// Note that this interface requires that the class overrides
      /// <c>Object.GetHashCode</c>. Though this is not enforced, the default
      /// implementation in <c>System.Object</c> is almost certainly incorrect
      /// and will not work correctly.
      /// </remarks>
      public interface class ICacheableKey
        : public IGFSerializable
      {
      public:

        /// <summary>
        /// Get the hash code for this object. This is used in the internal
        /// hash tables and so must have a nice distribution pattern.
        /// </summary>
        /// <returns>
        /// The hashcode for this object.
        /// </returns>
        int32_t GetHashCode( );

        /// <summary>
        /// Returns true if this <c>ICacheableKey</c> matches the other.
        /// </summary>
        bool Equals( ICacheableKey^ other );
      };

    }
  }
}
 } //namespace 
