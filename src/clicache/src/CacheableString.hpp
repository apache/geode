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
#include <gfcpp/CacheableString.hpp>
#include "impl/ManagedString.hpp"
#include "CacheableKey.hpp"
#include "GeodeClassIds.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      /// <summary>
      /// An immutable string wrapper that can serve as a distributable
      /// key object for caching as well as being a string value.
      /// </summary>
      ref class CacheableString
        : public CacheableKey
      {
      public:
        /// <summary>
        /// Allocates a new instance copying from the given string.
        /// </summary>
        /// <param name="value">the string value of the new instance</param>
        /// <exception cref="IllegalArgumentException">
        /// if the provided string is null or has zero length
        /// </exception>
        CacheableString(String^ value);

        /// <summary>
        /// Allocates a new instance copying from the given character array.
        /// </summary>
        /// <param name="value">
        /// the character array value of the new instance
        /// </param>
        /// <exception cref="IllegalArgumentException">
        /// if the provided array is null or has zero length
        /// </exception>
        CacheableString(array<Char>^ value);

        /// <summary>
        /// Static function to create a new instance copying from
        /// the given string.
        /// </summary>
        /// <remarks>
        /// Providing a null or zero size string will return a null
        /// <c>CacheableString</c> object.
        /// </remarks>
        /// <param name="value">the string value of the new instance</param>
        inline static CacheableString^ Create(String^ value)
        {
          return (value != nullptr ?
                  gcnew CacheableString(value, true) : nullptr);
        }

        /// <summary>
        /// Serializes this managed object.
        /// </summary>
        /// <param name="output">
        /// the DataOutput object to use for serializing the object
        /// </param>
        virtual void ToData(DataOutput^ output) override;

        /// <summary>
        /// Deserializes the managed object -- returns an instance of the
        /// <c>IGFSerializable</c> class.
        /// </summary>
        /// <param name="input">
        /// the DataInput stream to use for reading the object data
        /// </param>
        /// <returns>the deserialized object</returns>
        virtual IGFSerializable^ FromData(DataInput^ input) override;

        // <summary>
        /// Returns the classId of the instance being serialized.
        /// This is used by deserialization to determine what instance
        /// type to create and deserialize into.
        /// </summary>
        /// <returns>the classId</returns>
        virtual property uint32_t ClassId
        {
          virtual uint32_t get() override
          {
            return m_type;
          }
        }


        /// <summary>
        /// return the size of this object in bytes
        /// </summary>
        virtual property uint32_t ObjectSize
        {
          virtual uint32_t get() override;
        }

        /// <summary>
        /// Static function to create a new instance copying from
        /// the given character array.
        /// </summary>
        /// <remarks>
        /// Providing a null or zero size character array will return a null
        /// <c>CacheableString</c> object.
        /// </remarks>
        /// <param name="value">
        /// the character array value of the new instance
        /// </param>
        inline static CacheableString^ Create(array<Char>^ value)
        {
          return (value != nullptr && value->Length > 0 ?
                  gcnew CacheableString(value, true) : nullptr);
        }

        /// <summary>
        /// Return a string representation of the object.
        /// This returns the same string as <c>Value</c> property.
        /// </summary>
        virtual String^ ToString() override
        {
          return m_value;
        }

        /// <summary>
        /// Return true if this key matches other object.
        /// It invokes the '==' operator of the underlying
        /// <c>apache::geode::client::CacheableString</c> object.
        /// </summary>
        virtual bool Equals(Apache::Geode::Client::ICacheableKey^ other) override;

        /// <summary>
        /// Return true if this key matches other object.
        /// It invokes the '==' operator of the underlying
        /// <c>apache::geode::client::CacheableString</c> object.
        /// </summary>
        virtual bool Equals(Object^ obj) override;

        /// <summary>
        /// Return the hashcode for this key.
        /// </summary>
        virtual int32_t GetHashCode() override;

        /// <summary>
        /// Gets the string value.
        /// </summary>
        property String^ Value
        {
          inline String^ get()
          {
            return m_value;
          }
        }

        /// <summary>
        /// Static function to check whether IsNullOrEmpty.
        /// </summary>
        /// <remarks>
        /// This is similar to the C# string.IsNullOrEmpty method.
        /// </remarks>
        /// <param name="value">the CacheableString value to check</param>
        inline static bool IsNullOrEmpty(CacheableString^ value)
        {
          return (value == nullptr || value->Length == 0);
        }

        /// <summary>
        /// Implicit conversion operator to underlying <c>System.String</c>.
        /// </summary>
        inline static operator String ^ (CacheableString^ str)
        {
          return (str != nullptr ? CacheableString::GetString(str) : nullptr);
        }

        /// <summary>
        /// Gets the length of the underlying C string.
        /// </summary>
        property uint32_t Length
        {
          inline uint32_t get()
          {
            return m_value->Length;
          }
        }

        /// <summary>
        /// True when the underlying C string is a wide-character string.
        /// </summary>
        property bool IsWideString
        {
          inline bool get()
          {
            return true;//TODO:
          }
        }

      internal:
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableString(GeodeClassIds::CacheableASCIIString);
        }

        static IGFSerializable^ createDeserializableHuge()
        {
          return gcnew CacheableString(GeodeClassIds::CacheableASCIIStringHuge);
        }

        static IGFSerializable^ createUTFDeserializable()
        {
          return gcnew CacheableString(GeodeClassIds::CacheableString);
        }

        static IGFSerializable^ createUTFDeserializableHuge()
        {
          return gcnew CacheableString(GeodeClassIds::CacheableStringHuge);
        }
        /// <summary>
        /// Factory function to register wrapper
        /// </summary>
        static IGFSerializable^ Create(apache::geode::client::Serializable* obj)
        {
          return (obj != nullptr ?
                  gcnew CacheableString(obj) : nullptr);
        }

        /// <summary>
        /// Internal function to create a <c>apache::geode::client::CacheableString</c>
        /// from the given managed string.
        /// </summary>
        static void GetCacheableString(String^ value,
                                       apache::geode::client::CacheableStringPtr& cStr);

        /// <summary>
        /// Internal function to create a <c>apache::geode::client::CacheableString</c>
        /// from the given managed array of characters.
        /// </summary>
        static void GetCacheableString(array<Char>^ value,
                                       apache::geode::client::CacheableStringPtr& cStr);

        /// <summary>
        /// Get the <c>System.String</c> from the given
        /// <c>apache::geode::client::CacheableString</c>
        /// </summary>
        inline static String^ GetString(apache::geode::client::CacheableString * cStr)
        {
          if (cStr == NULL) {
            return nullptr;
          }
          else if (cStr->isWideString()) {
            return ManagedString::Get(cStr->asWChar());
          }
          else {
            return ManagedString::Get(cStr->asChar());
          }
        }

        inline static String^ GetString(CacheableString^ cStr)
        {
          return cStr->Value;
        }

        CacheableString(uint32_t type) : CacheableKey()
        {
          m_type = type;
        }

      private:
        String^ m_value;
        uint32_t m_type;
        int m_hashcode;

        CacheableString() : CacheableKey()
        {
          m_type = GeodeClassIds::CacheableASCIIString;
        }

        void SetStringType();
        /// <summary>
        /// Private constructor to create a CacheableString without checking
        /// for arguments.
        /// </summary>
        CacheableString(String^ value, bool noParamCheck);

        /// <summary>
        /// Private constructor to create a CacheableString without checking
        /// for arguments.
        /// </summary>
        CacheableString(array<Char>^ value, bool noParamCheck);

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CacheableString(apache::geode::client::Serializable* nativeptr)
          : CacheableKey(nativeptr) { }
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache


