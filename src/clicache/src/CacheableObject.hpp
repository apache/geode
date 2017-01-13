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
#include "GemFireClassIds.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      /// <summary>
      /// An mutable generic <see cref="System.Object" /> wrapper that can
      /// serve as a distributable value for caching.
      /// </summary>
      /// <remarks>
      /// <para>
      /// This class can serialize any class which has either the
      /// [Serializable] attribute set or implements
      /// <see cref="System.Runtime.Serialization.ISerializable" /> interface.
      /// However, for better efficiency the latter should be avoided and the
      /// user should implement <see cref="../../IGFSerializable" /> instead.
      /// </para><para>
      /// The user must keep in mind that the rules that apply to runtime
      /// serialization would be the rules that apply to this class. For
      /// the serialization will be carried out by serializing all the
      /// members (public/private/protected) of the class. Each of the
      /// contained classes should also have either the [Serializable]
      /// attribute set or implement <c>ISerializable</c> interface.
      /// </para>
      /// </remarks>
      public ref class CacheableObject
        : public IGFSerializable
      {
      public:
        /// <summary>
        /// Static function to create a new instance from the given object.
        /// </summary>
        /// <remarks>
        /// If the given object is null then this method returns null.
        /// </remarks>
        inline static CacheableObject^ Create(Object^ value)
        {
          return (value != nullptr ? gcnew CacheableObject(value) :
            nullptr);
        }

        /// <summary>
        /// Serializes this <see cref="System.Object" /> using
        /// <see cref="System.Runtime.Serialization.Formatters.Binary.BinaryFormatter" /> class.
        /// </summary>
        /// <param name="output">
        /// the DataOutput object to use for serializing the object
        /// </param>
        virtual void ToData(DataOutput^ output);

        /// <summary>
        /// Deserializes the <see cref="System.Object" /> using
        /// <see cref="System.Runtime.Serialization.Formatters.Binary.BinaryFormatter" /> class.
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
          inline virtual uint32_t get()
          {
            return GemFireClassIds::CacheableManagedObject;
          }
        }

        /// <summary>
        /// Gets the object value.
        /// </summary>
        /// <remarks>
        /// The user can modify the object and the changes shall be reflected
        /// immediately in the local cache. For this change to be propagate to
        /// other members of the distributed system, the object needs to be
        /// put into the cache.
        /// </remarks>
        property Object^ Value
        {
          inline Object^ get()
          {
            return m_obj;
          }
        }

        virtual String^ ToString() override
        {
          return (m_obj == nullptr ? nullptr : m_obj->ToString());
        }

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableObject(nullptr);
        }

      internal:
        /// <summary>
        /// Allocates a new instance from the given object.
        /// </summary>
        inline CacheableObject(Object^ value)
          : m_obj(value), m_objectSize(0) { }

        

      private:
        Object^ m_obj;
        uint32_t m_objectSize;
      };
    }
  }
}
 } //namespace 
