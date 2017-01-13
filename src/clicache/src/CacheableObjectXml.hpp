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
      /// A mutable generic <see cref="System.Object" /> wrapper that can
      /// serve as a distributable value for caching.
      /// </summary>
      /// <remarks>
      /// <para>
      /// This class can contain any object and uses the
      /// <see cref="System.Xml.Serialization.XmlSerializer" /> to
      /// serialize and deserialize the object. So the user must use the
      /// <c>XmlSerializer</c> attributes to control the serialization/deserialization
      /// of the object (or implement the <see cref="System.Xml.Serialization.IXmlSerializable" />)
      /// to change the serialization/deserialization. However, the latter should
      /// be avoided for efficiency reasons and the user should implement
      /// <see cref="../../IGFSerializable" /> instead.
      /// </para><para>
      /// The user must keep in mind that the rules that apply to <c>XmlSerializer</c>
      /// would be the rules that apply to this class. For instance the user
      /// cannot pass objects of class implementing or containing
      /// <see cref="System.Collections.IDictionary" /> class, must use
      /// <see cref="System.Xml.Serialization.XmlIncludeAttribute" /> to
      /// mark user-defined types etc.
      /// </para>
      /// </remarks>
      public ref class CacheableObjectXml
        : public IGFSerializable
      {
      public:
        /// <summary>
        /// Static function to create a new instance from the given object.
        /// </summary>
        /// <remarks>
        /// If the given object is null then this method returns null.
        /// </remarks>
        inline static CacheableObjectXml^ Create(Object^ value)
        {
          return (value != nullptr ? gcnew CacheableObjectXml(value) :
            nullptr);
        }

        /// <summary>
        /// Serializes this <see cref="System.Object" /> using
        /// <see cref="System.Xml.Serialization.XmlSerializer" /> class.
        /// </summary>
        /// <param name="output">
        /// the DataOutput object to use for serializing the object
        /// </param>
        virtual void ToData(DataOutput^ output);

        /// <summary>
        /// Deserializes the <see cref="System.Object" /> using
        /// <see cref="System.Xml.Serialization.XmlSerializer" /> class.
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
            return GemFireClassIds::CacheableManagedObjectXml;
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
          return gcnew CacheableObjectXml(nullptr);
        }

      internal:
        /// <summary>
        /// Allocates a new instance from the given object.
        /// </summary>
        inline CacheableObjectXml(Object^ value)
          : m_obj(value), m_objectSize(0) { }

      private:
        Object^ m_obj;
        uint32_t m_objectSize;
      };
    }
  }
}
 } //namespace 
