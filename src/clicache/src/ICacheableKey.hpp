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
