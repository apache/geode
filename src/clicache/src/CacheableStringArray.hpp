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
#include <gfcpp/CacheableBuiltins.hpp>
#include "Serializable.hpp"
#include "GemFireClassIds.hpp"
#include "CacheableString.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      ref class CacheableString;

      /// <summary>
      /// An immutable wrapper for array of strings that can serve as
      /// a distributable object for caching.
      /// </summary>
      ref class CacheableStringArray
        : public Serializable
      {
      public:
        /// <summary>
        /// Static function to create a new instance copying from the given
        /// string array.
        /// </summary>
        /// <remarks>
        /// If the given array of strings is null or of zero-length then
        /// this method returns null.
        /// </remarks>
        /// <exception cref="IllegalArgumentException">
        /// If the array contains a string greater than or equal 64K in length.
        /// </exception>
        inline static CacheableStringArray^ Create(array<String^>^ strings)
        {
          return (strings != nullptr && strings->Length > 0 ?
            gcnew CacheableStringArray(strings) : nullptr);
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


        /// <summary>
        /// Returns the classId of the instance being serialized.
        /// This is used by deserialization to determine what instance
        /// type to create and deserialize into.
        /// </summary>
        /// <returns>the classId</returns>
        virtual property uint32_t ClassId
        {
          virtual uint32_t get() override
          {
            return GemFireClassIds::CacheableStringArray;
          }
        }

        /// <summary>
        /// return the size of this object in bytes
        /// </summary>
        virtual property uint32_t ObjectSize
        {
          virtual uint32_t get() override
          {
            int size = 0; 
            for( int i = 0; i < m_value->Length; i++ )
            {
              size += m_value[i]->Length;
            }
            return (uint32_t) (size + sizeof(this));
          }

        }

        /// <summary>
        /// Returns a copy of the underlying array of strings.
        /// </summary>
        array<String^>^ GetValues();

        /// <summary>
        /// Returns a copy of the underlying string at the given index.
        /// </summary>
        property String^ GFINDEXER(int32_t)
        {
          String^ get(int32_t index);
        }

        /// <summary>
        /// Gets the length of the array.
        /// </summary>
        property int32_t Length
        {
          inline int32_t get()
          {
            return m_value->Length;
          }
        }

        virtual String^ ToString() override
        {
          return m_value->ToString();
        }

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableStringArray();
        }

      internal:
        /// <summary>
        /// Factory function to register wrapper
        /// </summary>
        static IGFSerializable^ Create(apache::geode::client::Serializable* obj)
        {
          return (obj != nullptr ?
            gcnew CacheableStringArray(obj) : nullptr);
        }

      private:
        array<String^>^ m_value;
        /// <summary>
        /// Allocates a new instance copying from the given string array.
        /// </summary>
        /// <exception cref="IllegalArgumentException">
        /// If the array contains a string greater than or equal 64K in length.
        /// </exception>
        CacheableStringArray(array<String^>^ strings);


        inline CacheableStringArray()
          : Serializable() 
        { 
          //apache::geode::client::Serializable* sp = apache::geode::client::CacheableStringArray::createDeserializable();
          //SetSP(sp);
        }

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CacheableStringArray(apache::geode::client::Serializable* nativeptr)
          : Serializable(nativeptr) { }
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache


