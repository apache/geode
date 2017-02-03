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
#include "CacheableKey.hpp"
#include "Serializable.hpp"
#include "ExceptionTypes.hpp"
#include "GeodeClassIds.hpp"
#include "DataOutput.hpp"
#include "DataInput.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      //namespace Internal


      /// <summary>
      /// An immutable template wrapper for C++ <c>CacheableKey</c>s that can
      /// serve as a distributable key object for caching.
      /// </summary>
      template <typename TNative, typename TManaged, uint32_t TYPEID>
      ref class CacheableBuiltinKey
        : public CacheableKey
      {
      public:
        /// <summary>
        /// Allocates a new instance 
        /// </summary>
        CacheableBuiltinKey()
        {
          apache::geode::client::SharedPtr<TNative>& nativeptr = TNative::create();

          SetSP(nativeptr.ptr());
        }

        /// <summary>
        /// Allocates a new instance with the given value.
        /// </summary>
        /// <param name="value">the value of the new instance</param>
        CacheableBuiltinKey(TManaged value)
        {
          apache::geode::client::SharedPtr<TNative>& nativeptr = TNative::create(value);

          SetSP(nativeptr.ptr());
        }

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
            return TYPEID;
          }
        }

        /// <summary>
        /// Return a string representation of the object.
        /// This returns the string for the <c>Value</c> property.
        /// </summary>
        virtual String^ ToString() override
        {
          return static_cast<TNative*>(NativePtr())->value().ToString();
        }

        /// <summary>
        /// Return true if this key matches other object.
        /// It invokes the '==' operator of the underlying
        /// native object.
        /// </summary>
        virtual bool Equals(ICacheableKey^ other) override
        {
          if (other == nullptr || other->ClassId != TYPEID)
          {
            return false;
          }
          return static_cast<TNative*>(NativePtr())->operator==(
            *static_cast<TNative*>(((CacheableKey^)other)->NativePtr()));
        }

        /// <summary>
        /// Return true if this key matches other object.
        /// It invokes the '==' operator of the underlying
        /// native object.
        /// </summary>
        virtual bool Equals(Object^ obj) override
        {
          CacheableBuiltinKey^ otherKey =
            dynamic_cast<CacheableBuiltinKey^>(obj);

          if (otherKey != nullptr) {
            return static_cast<TNative*>(NativePtr())->operator==(
              *static_cast<TNative*>(otherKey->NativePtr()));
          }
          return false;
        }

        /// <summary>
        /// Comparison operator against another value.
        /// </summary>
        bool operator == (TManaged other)
        {
          return (static_cast<TNative*>(NativePtr())->value() == other);
        }

        /// <summary>
        /// Gets the value.
        /// </summary>
        property TManaged Value
        {
          inline TManaged get()
          {
            return static_cast<TNative*>(NativePtr())->value();
          }
        }

      protected:

        /// <summary>
        /// Protected constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CacheableBuiltinKey(apache::geode::client::Serializable* nativeptr)
          : CacheableKey(nativeptr) { }
      };


      /// <summary>
      /// An immutable template array wrapper that can serve as a
      /// distributable object for caching.
      /// </summary>
      template <typename TNative, typename TNativePtr, typename TManaged,
        uint32_t TYPEID>
      ref class CacheableBuiltinArray
        : public Serializable
      {
      public:

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
            return TYPEID;
          }
        }

        virtual void ToData(DataOutput^ output) override
        {
          output->WriteObject(m_value);
        }

        virtual IGFSerializable^ FromData(DataInput^ input) override
        {
          input->ReadObject(m_value);
          return this;
        }

        virtual property uint32_t ObjectSize
        {
          virtual uint32_t get() override
          {
            return (uint32_t)(m_value->Length) * sizeof(TManaged);
          }
        }
        /// <summary>
        /// Returns a copy of the underlying array.
        /// </summary>
        property array<TManaged>^ Value
        {
          inline array<TManaged>^ get()
          {
            return m_value;
          }
        }

        /// <summary>
        /// Returns the size of this array.
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
        /// Returns the value at the given index.
        /// </summary>
        property TManaged GFINDEXER(int32_t)
        {
          inline TManaged get(int32_t index)
          {
            return m_value[index];
          }
        }


      protected:

        array<TManaged>^ m_value;
        /// <summary>
        /// Protected constructor 
        /// </summary>
        inline CacheableBuiltinArray()
        {
          //TODO:
          //apache::geode::client::Serializable* sp = TNative::createDeserializable();
          //SetSP(sp);
        }

        /// <summary>
        /// Protected constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CacheableBuiltinArray(apache::geode::client::Serializable* nptr)
          : Serializable(nptr)
        {
          //TODO: ??
          // ManagedPtrWrap< apache::geode::client::Serializable,
          // Internal::SBWrap<apache::geode::client::Serializable> > nptr = nativeptr;
          TNative* nativeptr = static_cast<TNative*>(nptr);
          int32_t len = nativeptr->length();
          if (len > 0)
          {
            array<TManaged>^ buffer = gcnew array<TManaged>(len);
            pin_ptr<TManaged> pin_buffer = &buffer[0];

            memcpy((void*)pin_buffer, nativeptr->value(),
                   len * sizeof(TManaged));
            m_value = buffer;
          }
        }

        /// <summary>
        /// Allocates a new instance copying from the given array.
        /// </summary>
        /// <remarks>
        /// This method performs no argument checking which is the
        /// responsibility of the caller.
        /// </remarks>
        /// <param name="buffer">the array to copy from</param>
        CacheableBuiltinArray(array<TManaged>^ buffer)
        {
          m_value = buffer;
          //setting local value as well
          //m_value = gcnew array<TManaged>(buffer->Length);
          //System::Array::Copy(buffer, 0, m_value,0, buffer->Length);             
        }

        /// <summary>
        /// Allocates a new instance copying given length from the
        /// start of given array.
        /// </summary>
        /// <remarks>
        /// This method performs no argument checking which is the
        /// responsibility of the caller.
        /// </remarks>
        /// <param name="buffer">the array to copy from</param>
        /// <param name="length">length of array from start to copy</param>
        CacheableBuiltinArray(array<TManaged>^ buffer, int32_t length)
        {
          //TODO:
          if (length > buffer->Length) {
            length = buffer->Length;
          }
          //setting local value as well
          m_value = gcnew array<TManaged>(length);
          System::Array::Copy(buffer, 0, m_value, 0, length);
        }
      };




      //n = native type
      //m = CacheableInt(managed cacheable)
      //mt = managed type(bool, int)
#define _GFCLI_CACHEABLE_KEY_DEF_NEW(n, m, mt)                                   \
      ref class m : public CacheableBuiltinKey<n, mt,        \
        GeodeClassIds::m>                                                   \
      {                                                                       \
      public:                                                                 \
         /** <summary>
         *  Allocates a new instance with the given value.
         *  </summary>
         *  <param name="value">the value of the new instance</param>
         */                                                                   \
         inline m()                                                            \
         : CacheableBuiltinKey() { }                                         \
         /** <summary>
          *  Allocates a new instance with the given value.
          *  </summary>
          *  <param name="value">the value of the new instance</param>
          */                                                                   \
          inline m(mt value)                                                    \
          : CacheableBuiltinKey(value) { }                                    \
          /** <summary>
           *  Static function to create a new instance given value.
           *  </summary>
           *  <param name="value">the value of the new instance</param>
           */                                                                   \
           inline static m^ Create(mt value)                                     \
           {                                                                     \
           return gcnew m(value);                                              \
           }                                                                     \
           /** <summary>
            * Explicit conversion operator to contained value type.
            * </summary>
            */                                                                   \
            inline static explicit operator mt (m^ value)                         \
           {                                                                     \
           return value->Value;                                                \
           }                                                                     \
           \
           /** <summary>
            * Factory function to register this class.
            * </summary>
            */                                                                   \
            static IGFSerializable^ CreateDeserializable()                        \
           {                                                                     \
           return gcnew m();                                       \
           }                                                                     \
           \
           internal:                                                               \
           static IGFSerializable^ Create(apache::geode::client::Serializable* obj)            \
           {                                                                     \
           return (obj != nullptr ? gcnew m(obj) : nullptr);                   \
           }                                                                     \
           \
           private:                                                                \
             inline m(apache::geode::client::Serializable* nativeptr)                            \
              : CacheableBuiltinKey(nativeptr) { }                                \
      };


#define _GFCLI_CACHEABLE_ARRAY_DEF_NEW(m, mt)                                    \
      ref class m : public CacheableBuiltinArray<            \
        apache::geode::client::m, apache::geode::client::m##Ptr, mt, GeodeClassIds::m>                  \
            {                                                                       \
      public:                                                                 \
        /** <summary>
      *  Static function to create a new instance copying
      *  from the given array.
      *  </summary>
      *  <remarks>
      *  Providing a null or zero size array will return a null object.
      *  </remarks>
      *  <param name="value">the array to create the new instance</param>
      */                                                                   \
      inline static m^ Create(array<mt>^ value)                             \
      {                                                                     \
      return (value != nullptr /*&& value->Length > 0*/ ? \
      gcnew m(value) : nullptr);                                        \
      }                                                                     \
      /** <summary>
       *  Static function to create a new instance copying
       *  from the given array.
       *  </summary>
       *  <remarks>
       *  Providing a null or zero size array will return a null object.
       *  </remarks>
       *  <param name="value">the array to create the new instance</param>
       */                                                                   \
       inline static m^ Create(array<mt>^ value, int32_t length)               \
      {                                                                     \
      return (value != nullptr && value->Length > 0 ? \
      gcnew m(value, length) : nullptr);                                \
      }                                                                     \
      /** <summary>
       * Explicit conversion operator to contained array type.
       * </summary>
       */                                                                   \
       inline static explicit operator array<mt> ^ (m^ value)                 \
      {                                                                     \
      return (value != nullptr ? value->Value : nullptr);                 \
      }                                                                     \
      \
      /** <summary>
       * Factory function to register this class.
       * </summary>
       */                                                                   \
       static IGFSerializable^ CreateDeserializable()                        \
      {                                                                     \
      return gcnew m();                                                   \
      }                                                                     \
      \
            internal:                                                               \
              static IGFSerializable^ Create(apache::geode::client::Serializable* obj)            \
      {                                                                     \
      return (obj != nullptr ? gcnew m(obj) : nullptr);                   \
      }                                                                     \
      \
            private:                                                                \
            /** <summary>
             * Allocates a new instance
             *  </summary>
             */                                                                   \
             inline m()                                                            \
             : CacheableBuiltinArray() { }                                       \
             /** <summary>
              * Allocates a new instance copying from the given array.
              *  </summary>
              *  <remarks>
              *  Providing a null or zero size array will return a null object.
              *  </remarks>
              *  <param name="value">the array to create the new instance</param>
              */                                                                   \
              inline m(array<mt>^ value)                                            \
              : CacheableBuiltinArray(value) { }                                  \
              /** <summary>
               * Allocates a new instance copying given length from the
               * start of given array.
               *  </summary>
               *  <remarks>
               *  Providing a null or zero size array will return a null object.
               *  </remarks>
               *  <param name="value">the array to create the new instance</param>
               */                                                                   \
               inline m(array<mt>^ value, int32_t length)                              \
               : CacheableBuiltinArray(value, length) { }                          \
               inline m(apache::geode::client::Serializable* nativeptr)                            \
               : CacheableBuiltinArray(nativeptr) { }                              \
      };


      // Built-in CacheableKeys

      /// <summary>
      /// An immutable wrapper for booleans that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_NEW(apache::geode::client::CacheableBoolean,
                                   CacheableBoolean, bool);

      /// <summary>
      /// An immutable wrapper for bytes that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_NEW(apache::geode::client::CacheableByte,
                                   CacheableByte, Byte);

      /// <summary>
      /// An immutable wrapper for 16-bit characters that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_NEW(apache::geode::client::CacheableWideChar,
                                   CacheableCharacter, Char);

      /// <summary>
      /// An immutable wrapper for doubles that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_NEW(apache::geode::client::CacheableDouble,
                                   CacheableDouble, Double);

      /// <summary>
      /// An immutable wrapper for floats that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_NEW(apache::geode::client::CacheableFloat,
                                   CacheableFloat, Single);

      /// <summary>
      /// An immutable wrapper for 16-bit integers that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_NEW(apache::geode::client::CacheableInt16,
                                   CacheableInt16, int16_t);

      /// <summary>
      /// An immutable wrapper for 32-bit integers that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_NEW(apache::geode::client::CacheableInt32,
                                   CacheableInt32, int32_t);

      /// <summary>
      /// An immutable wrapper for 64-bit integers that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_NEW(apache::geode::client::CacheableInt64,
                                   CacheableInt64, int64_t);


      // Built-in Cacheable array types

      /// <summary>
      /// An immutable wrapper for byte arrays that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_NEW(CacheableBytes, Byte);

      /// <summary>
      /// An immutable wrapper for array of doubles that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_NEW(CacheableDoubleArray, Double);

      /// <summary>
      /// An immutable wrapper for array of floats that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_NEW(CacheableFloatArray, Single);

      /// <summary>
      /// An immutable wrapper for array of 16-bit integers that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_NEW(CacheableInt16Array, int16_t);

      /// <summary>
      /// An immutable wrapper for array of 32-bit integers that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_NEW(CacheableInt32Array, int32_t);

      /// <summary>
      /// An immutable wrapper for array of 64-bit integers that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_NEW(CacheableInt64Array, int64_t);

      /// <summary>
      /// An immutable wrapper for array of booleans that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_NEW(BooleanArray, bool);

      /// <summary>
      /// An immutable wrapper for array of 16-bit characters that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_NEW(CharArray, Char);
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

