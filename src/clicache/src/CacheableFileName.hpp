/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once



#include "gf_defs.hpp"
#include "ICacheableKey.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      /// <summary>
      /// An immutable filename wrapper that can serve as a distributable
      /// key object for caching as well as being a string value.
      /// </summary>
      public ref class CacheableFileName
        : public ICacheableKey
      {
      public:
        /// <summary>
        /// Static function to create a new instance from the given string.
        /// </summary>
        inline static CacheableFileName^ Create(String^ value)
        {
          return (value != nullptr && value->Length > 0 ?
            gcnew CacheableFileName(value) : nullptr);
        }

        /// <summary>
        /// Static function to create a new instance from the
        /// given character array.
        /// </summary>
        inline static CacheableFileName^ Create(array<Char>^ value)
        {
          return (value != nullptr && value->Length > 0 ?
            gcnew CacheableFileName(value) : nullptr);
        }

        // Region: IGFSerializable Members

        /// <summary>
        /// Serializes this object.
        /// </summary>
        /// <param name="output">
        /// the DataOutput object to use for serializing the object
        /// </param>
        virtual void ToData(DataOutput^ output);

        /// <summary>
        /// Deserialize this object, typical implementation should return
        /// the 'this' pointer.
        /// </summary>
        /// <param name="input">
        /// the DataInput stream to use for reading the object data
        /// </param>
        /// <returns>the deserialized object</returns>
        virtual IGFSerializable^ FromData(DataInput^ input);

        /// <summary>
        /// return the size of this object in bytes
        /// </summary>
        virtual property uint32_t ObjectSize
        {
          virtual uint32_t get();
        }

        /// <summary>
        /// Returns the classId of the instance being serialized.
        /// This is used by deserialization to determine what instance
        /// type to create and deserialize into.
        /// </summary>
        /// <returns>the classId</returns>
        virtual property uint32_t ClassId
        {
          virtual uint32_t get();
        }

        /// <summary>
        /// Return a string representation of the object.
        /// This returns the same string as <c>Value</c> property.
        /// </summary>
        virtual String^ ToString() override
        {
          return m_str;
        }

        // End Region: IGFSerializable Members

        // Region: ICacheableKey Members

        /// <summary>
        /// Return the hashcode for this key.
        /// </summary>
        virtual int32_t GetHashCode() override;

        /// <summary>
        /// Return true if this key matches other object.
        /// </summary>
        virtual bool Equals(ICacheableKey^ other);

        /// <summary>
        /// Return true if this key matches other object.
        /// </summary>
        virtual bool Equals(Object^ obj) override;

        // End Region: ICacheableKey Members

        /// <summary>
        /// Gets the string value.
        /// </summary>
        property String^ Value
        {
          inline String^ get()
          {
            return m_str;
          }
        }

      internal:
        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableFileName((String^)nullptr);
        }

      private:
        /// <summary>
        /// Allocates a new instance from the given string.
        /// </summary>
        inline CacheableFileName(String^ value)
          : m_str(value == nullptr ? String::Empty : value),m_hashcode(0) { }

        /// <summary>
        /// Allocates a new instance copying from the given character array.
        /// </summary>
        inline CacheableFileName(array<Char>^ value)
          : m_str(gcnew String(value)),m_hashcode(0) { }

        String^ m_str;
        int m_hashcode;
      };
    }
  }
}
 } //namespace 


