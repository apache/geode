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
#include <gfcpp/CacheableKey.hpp>
//#include "impl/NativeWrapper.hpp"
#include "Serializable.hpp"
#include "ICacheableKey.hpp"


using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {
      /// <summary>
      /// This class wraps the native C++ <c>apache::geode::client::Serializable</c> objects
      /// as managed <see cref="../../IGFSerializable" /> objects.
      /// </summary>
      public ref class CacheableKey
        : public Serializable, public ICacheableKey
      {
      public:
        /// <summary>
        /// Return the hashcode for this key.
        /// It gets the hash code by calling the <c>hashcode()</c> function
        /// of the underlying <c>apache::geode::client::CacheableKey</c> object.
        /// </summary>
        virtual int32_t GetHashCode() override;

        /// <summary>
        /// Return true if this key matches other object. It invokes the '=='
        /// operator of the underlying <c>apache::geode::client::CacheableKey</c> object.
        /// </summary>
        virtual bool Equals(ICacheableKey^ other);

        /// <summary>
        /// Return true if this key matches other object.
        /// It invokes the '==' operator if the underlying object is a
        /// <c>apache::geode::client::CacheableKey</c>, else returns
        /// <c>System.Object.Equals()</c>
        /// </summary>
        virtual bool Equals(Object^ obj) override;

        // Static conversion functions from primitive types.

        /// <summary>
        /// Implicit conversion operator from a boolean
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator CacheableKey^ (bool value);

        /// <summary>
        /// Implicit conversion operator from a byte
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator CacheableKey^ (Byte value);

        /// <summary>
        /// Implicit conversion operator from a double
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator CacheableKey^ (Double value);

        /// <summary>
        /// Implicit conversion operator from a float
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator CacheableKey^ (Single value);

        /// <summary>
        /// Implicit conversion operator from a 16-bit integer
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator CacheableKey^ (int16_t value);

        /// <summary>
        /// Implicit conversion operator from a character
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator CacheableKey^ (Char value);

        /// <summary>
        /// Implicit conversion operator from a 32-bit integer
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator CacheableKey^ (int32_t value);

        /// <summary>
        /// Implicit conversion operator from a 64-bit integer
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator CacheableKey^ (int64_t value);

        /// <summary>
        /// Implicit conversion operator from a string
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator CacheableKey^ (String^ value);

      internal:
        /// <summary>
        /// Default constructor.
        /// </summary>
        inline CacheableKey()
          : Generic::Serializable() { }

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CacheableKey(apache::geode::client::Serializable* nativeptr)
          : Generic::Serializable(nativeptr) { }
      };
    }
  }
}
} //namespace 

