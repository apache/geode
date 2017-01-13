/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once



#include "gf_defs.hpp"
#include <gfcpp/CacheableBuiltins.hpp>
#include "Serializable.hpp"
#include "GemFireClassIds.hpp"
#include "CacheableString.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
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
        static IGFSerializable^ Create(gemfire::Serializable* obj)
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
          //gemfire::Serializable* sp = gemfire::CacheableStringArray::createDeserializable();
          //SetSP(sp);
        }

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline CacheableStringArray(gemfire::Serializable* nativeptr)
          : Serializable(nativeptr) { }
      };
    }
  }
}
 } //namespace 

