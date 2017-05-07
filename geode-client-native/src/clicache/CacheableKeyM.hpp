/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/CacheableKey.hpp"
#include "impl/NativeWrapper.hpp"
#include "SerializableM.hpp"
#include "ICacheableKey.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      /// <summary>
      /// This class wraps the native C++ <c>gemfire::Serializable</c> objects
      /// as managed <see cref="IGFSerializable" /> objects.
      /// </summary>
        [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class CacheableKey
        : public GemStone::GemFire::Cache::Serializable, public GemStone::GemFire::Cache::ICacheableKey
      {
      public:
        /// <summary>
        /// Return the hashcode for this key.
        /// It gets the hash code by calling the <c>hashcode()</c> function
        /// of the underlying <c>gemfire::CacheableKey</c> object.
        /// </summary>
        virtual int32_t GetHashCode() override;

        /// <summary>
        /// Return true if this key matches other object. It invokes the '=='
        /// operator of the underlying <c>gemfire::CacheableKey</c> object.
        /// </summary>
        virtual bool Equals(GemStone::GemFire::Cache::ICacheableKey^ other);

        /// <summary>
        /// Return true if this key matches other object.
        /// It invokes the '==' operator if the underlying object is a
        /// <c>gemfire::CacheableKey</c>, else returns
        /// <c>System.Object.Equals()</c>
        /// </summary>
        virtual bool Equals(Object^ obj) override;

        // Static conversion functions from primitive types.

        /// <summary>
        /// Implicit conversion operator from a boolean
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::CacheableKey^ (bool value);

        /// <summary>
        /// Implicit conversion operator from a byte
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::CacheableKey^ (Byte value);

        /// <summary>
        /// Implicit conversion operator from a double
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::CacheableKey^ (Double value);

        /// <summary>
        /// Implicit conversion operator from a float
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::CacheableKey^ (Single value);

        /// <summary>
        /// Implicit conversion operator from a 16-bit integer
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::CacheableKey^ (int16_t value);

        /// <summary>
        /// Implicit conversion operator from a character
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::CacheableKey^ (Char value);

        /// <summary>
        /// Implicit conversion operator from a 32-bit integer
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::CacheableKey^ (int32_t value);

        /// <summary>
        /// Implicit conversion operator from a 64-bit integer
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::CacheableKey^ (int64_t value);

        /// <summary>
        /// Implicit conversion operator from a string
        /// to a <c>CacheableKey</c>.
        /// </summary>
        static operator GemStone::GemFire::Cache::CacheableKey^ (String^ value);

      internal:
        /// <summary>
        /// Default constructor.
        /// </summary>
        inline CacheableKey()
          : /*GemStone::GemFire::Cache::*/Serializable() { }

        /// <summary>
        /// Internal constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CacheableKey(gemfire::Serializable* nativeptr)
          : /*GemStone::GemFire::Cache::*/Serializable(nativeptr) { }
      };
    }
  }
}
