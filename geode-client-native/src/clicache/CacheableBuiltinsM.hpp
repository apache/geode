/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/CacheableBuiltins.hpp"
#include "CacheableKeyM.hpp"
#include "SerializableM.hpp"

#include "DataInputM.hpp"
#include "DataOutputM.hpp"
#include "LogM.hpp"

#include "GemFireClassIdsM.hpp"

using namespace System;
using namespace GemStone::GemFire::Cache::Internal;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Template
      {
          [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
       ref class BultinHashcodes  
        {
        public :
          static int GetHash(bool val)
          {
            if (val) return 1231;
            else return 1237;
          }
          
          static int GetHash(int val)
          {
            return val;
          }

          static int GetHash(int16 val)
          {
            return val;
          }

          static int GetHash(uint16 val)
          {
            return val;
          }

          static int GetHash(uint32 val)
          {
            return val;
          }

          static int GetHash(Single val)
          {
            return val.GetHashCode();
          }

          static int GetHash(Byte val)
          {
            return val;
          }

          static int GetHash(long val)
          {
            return val.GetHashCode();
          }

          static int GetHash(double val)
          {
            return val.GetHashCode();
          }

          static int GetHash(int64_t val)
          {
            return val.GetHashCode();
          }

          static int GetHash(Object^ val)
          {
            return val->GetHashCode();;
          }
    
        };

        /// <summary>
        /// An immutable template wrapper for C++ <c>CacheableKey</c>s that can
        /// serve as a distributable key object for caching.
        /// </summary>
       //[Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
        template <typename TNative, typename TManaged, uint32_t TYPEID>
        public ref class CacheableBuiltinKey
          : public GemStone::GemFire::Cache::CacheableKey
        {
         public:
          /// <summary>
          /// Allocates a new instance 
          /// </summary>
          /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinKey()
          {
            //TODO:Hitesh 
            gemfire::SharedPtr<TNative>& nativeptr = TNative::create();
            SetSP(nativeptr.ptr());
          }

          /// <summary>
          /// Allocates a new instance with the given value.
          /// </summary>
          /// <param name="value">the value of the new instance</param>
          /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinKey(TManaged value)
          {
            m_value = value;
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

          virtual void ToData(GemStone::GemFire::Cache::DataOutput^ output) override
          {
            output->WriteObject(m_value); 
          }

          virtual GemStone::GemFire::Cache::IGFSerializable^ FromData(GemStone::GemFire::Cache::DataInput^ input) override
          {
              input->ReadObject(m_value);
              return this;          
          }

          virtual property uint32_t ObjectSize 
          {
            virtual uint32_t get() override
            {
              return (uint32_t) (sizeof(m_value));
            }
          }

          /// <summary>
          /// Return a string representation of the object.
          /// This returns the string for the <c>Value</c> property.
          /// </summary>
          virtual String^ ToString() override
          {
            return m_value.ToString();
          }

          /// <summary>
          /// Return true if this key matches other object.
          /// It invokes the '==' operator of the underlying
          /// native object.
          /// </summary>
          virtual bool Equals(GemStone::GemFire::Cache::ICacheableKey^ other) override
          {
            if (other == nullptr || other->ClassId != TYPEID)
            {
              return false;
            }
            /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinKey^ otherKey =
              dynamic_cast</*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinKey^>(other);

            if (otherKey == nullptr)
              return false;

            return m_value.Equals(otherKey->Value);
          }

          virtual int32_t GetHashCode() override
          {
            //return m_value.GetHashCode();
            return BultinHashcodes::GetHash(m_value);
          }


          /// <summary>
          /// Return true if this key matches other object.
          /// It invokes the '==' operator of the underlying
          /// native object.
          /// </summary>
          virtual bool Equals(Object^ obj) override
          {
            /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinKey^ otherKey =
              dynamic_cast</*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinKey^>(obj);

            if (otherKey == nullptr)
              return false;
           
            return m_value.Equals(otherKey->Value);
          }

          /// <summary>
          /// Comparison operator against another value.
          /// </summary>
          bool operator == (TManaged other)
          {
            return m_value.Equals(other.Value);
          }

          /// <summary>
          /// Gets the value.
          /// </summary>
          property TManaged Value
          {
            inline TManaged get()
            {
              return m_value;
            }
          }

        protected:

          TManaged m_value;

          /// <summary>
          /// Protected constructor to wrap a native object pointer
          /// </summary>
          /// <param name="nativeptr">The native object pointer</param>
          inline /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinKey(gemfire::Serializable* nativeptr)
            : /*GemStone::GemFire::Cache::*/CacheableKey(nativeptr) {
              //TODO:Hitesh ??
           m_value = static_cast<TNative*>(nativeptr)->value();
          }
        };


        /// <summary>
        /// An immutable template array wrapper that can serve as a
        /// distributable object for caching.
        /// </summary>
        //[Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
        template <typename TNative, typename TNativePtr, typename TManaged,
          uint32_t TYPEID>
        public ref class CacheableBuiltinArray
          : public GemStone::GemFire::Cache::Serializable
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

          virtual void ToData(GemStone::GemFire::Cache::DataOutput^ output) override
          {
            output->WriteObject(m_value); 
          }

          virtual GemStone::GemFire::Cache::IGFSerializable^ FromData(GemStone::GemFire::Cache::DataInput^ input) override
          {
            input->ReadObject(m_value);
            return this;
          }

          virtual property uint32_t ObjectSize 
          {
            virtual uint32_t get() override
            {
              return (uint32_t) (m_value->Length * sizeof(TManaged));
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
          inline /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinArray()
          {
            //TODO:Hitesh
            gemfire::Serializable* sp = TNative::createDeserializable();
            SetSP(sp);
          }

          /// <summary>
          /// Protected constructor to wrap a native object pointer
          /// </summary>
          /// <param name="nativeptr">The native object pointer</param>
          inline /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinArray(gemfire::Serializable* nptr)
            : GemStone::GemFire::Cache::Serializable(nptr)
          { 
            //TODO:Hitesh ??
             // ManagedPtrWrap< gemfire::Serializable,
               // Internal::SBWrap<gemfire::Serializable> > nptr = nativeptr;
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
          /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinArray(array<TManaged>^ buffer)
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
          /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinArray(array<TManaged>^ buffer, int32_t length)
          {
            //TODO:Hitesh
            if (length > buffer->Length) {
              length = buffer->Length;
            }
            //setting local value as well
            m_value = gcnew array<TManaged>(length);
            System::Array::Copy(buffer, 0, m_value,0, length);
          }
        };

      }

#define _GFCLI_CACHEABLE_KEY_DEF_(n, m, mt)                                   \
      public ref class m : public /*GemStone::GemFire::Cache::*/Template::CacheableBuiltinKey<n, mt,        \
        GemFireClassIds::m>                                                   \
      {                                                                       \
      public:                                                                 \
         /** <summary>
         *  Allocates a new instance with the given value.
         *  </summary>
         *  <param name="value">the value of the new instance</param>
         */                                                                   \
        inline m()                                                            \
          : /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinKey() { }                                         \
        /** <summary>
         *  Allocates a new instance with the given value.
         *  </summary>
         *  <param name="value">the value of the new instance</param>
         */                                                                   \
        inline m(mt value)                                                    \
          : /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinKey(value) { }                                    \
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
        static GemStone::GemFire::Cache::IGFSerializable^ CreateDeserializable()                        \
        {                                                                     \
          return gcnew m();                                       \
        }                                                                     \
                                                                              \
      internal:                                                               \
        static GemStone::GemFire::Cache::IGFSerializable^ Create(gemfire::Serializable* obj)            \
        {                                                                     \
          return (obj != nullptr ? gcnew m(obj) : nullptr);                   \
        }                                                                     \
                                                                              \
      private:                                                                \
        inline m(gemfire::Serializable* nativeptr)                            \
        : /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinKey(nativeptr) { }                                \
      };


#define _GFCLI_CACHEABLE_ARRAY_DEF_(m, mt)                                    \
      public ref class m : public GemStone::GemFire::Cache::Template::CacheableBuiltinArray<            \
        gemfire::m, gemfire::m##Ptr, mt, GemFireClassIds::m>                  \
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
          return (value != nullptr && value->Length > 0 ?                     \
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
          return (value != nullptr && value->Length > 0 ?                     \
            gcnew m(value, length) : nullptr);                                \
        }                                                                     \
        /** <summary>
         * Explicit conversion operator to contained array type.
         * </summary>
         */                                                                   \
        inline static explicit operator array<mt>^ (m^ value)                 \
        {                                                                     \
          return (value != nullptr ? value->Value : nullptr);                 \
        }                                                                     \
                                                                              \
        /** <summary>
         * Factory function to register this class.
         * </summary>
         */                                                                   \
        static GemStone::GemFire::Cache::IGFSerializable^ CreateDeserializable()                        \
        {                                                                     \
          return gcnew m();                                                   \
        }                                                                     \
                                                                              \
      internal:                                                               \
        static GemStone::GemFire::Cache::IGFSerializable^ Create(gemfire::Serializable* obj)            \
        {                                                                     \
          return (obj != nullptr ? gcnew m(obj) : nullptr);                  \
        }                                                                     \
                                                                              \
      private:                                                                \
        /** <summary>
         * Allocates a new instance
         *  </summary>
         */                                                                   \
        inline m()                                                            \
          : /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinArray() { }                                       \
        /** <summary>
         * Allocates a new instance copying from the given array.
         *  </summary>
         *  <remarks>
         *  Providing a null or zero size array will return a null object.
         *  </remarks>
         *  <param name="value">the array to create the new instance</param>
         */                                                                   \
        inline m(array<mt>^ value)                                            \
          : /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinArray(value) { }                                  \
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
          : /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinArray(value, length) { }                          \
        inline m(gemfire::Serializable* nativeptr)                            \
          : /*GemStone::GemFire::Cache::Internal::*/CacheableBuiltinArray(nativeptr) { }                              \
      };


      // Built-in CacheableKeys

      /// <summary>
      /// An immutable wrapper for booleans that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_(gemfire::CacheableBoolean,
        CacheableBoolean, bool);

      /// <summary>
      /// An immutable wrapper for bytes that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_(gemfire::CacheableByte,
        CacheableByte, Byte);

      /// <summary>
      /// An immutable wrapper for 16-bit characters that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_(gemfire::CacheableWideChar,
        CacheableCharacter, Char);

      /// <summary>
      /// An immutable wrapper for doubles that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_(gemfire::CacheableDouble,
        CacheableDouble, Double);

      /// <summary>
      /// An immutable wrapper for floats that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_(gemfire::CacheableFloat,
        CacheableFloat, Single);

      /// <summary>
      /// An immutable wrapper for 16-bit integers that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_(gemfire::CacheableInt16,
        CacheableInt16, int16_t);

      /// <summary>
      /// An immutable wrapper for 32-bit integers that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_(gemfire::CacheableInt32,
        CacheableInt32, int32_t);

      /// <summary>
      /// An immutable wrapper for 64-bit integers that can serve
      /// as a distributable key object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_KEY_DEF_(gemfire::CacheableInt64,
        CacheableInt64, int64_t);


      // Built-in Cacheable array types

      /// <summary>
      /// An immutable wrapper for byte arrays that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_(CacheableBytes, Byte);

      /// <summary>
      /// An immutable wrapper for array of doubles that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_(CacheableDoubleArray, Double);

      /// <summary>
      /// An immutable wrapper for array of floats that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_(CacheableFloatArray, Single);

      /// <summary>
      /// An immutable wrapper for array of 16-bit integers that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_(CacheableInt16Array, int16_t);

      /// <summary>
      /// An immutable wrapper for array of 32-bit integers that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_(CacheableInt32Array, int32_t);

      /// <summary>
      /// An immutable wrapper for array of 64-bit integers that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_(CacheableInt64Array, int64_t);

      /// <summary>
      /// An immutable wrapper for array of booleans that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_(BooleanArray, bool);

      /// <summary>
      /// An immutable wrapper for array of 16-bit characters that can serve
      /// as a distributable object for caching.
      /// </summary>
      _GFCLI_CACHEABLE_ARRAY_DEF_(CharArray, Char);

    }
  }
}
