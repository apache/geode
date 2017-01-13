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
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// A mutable <c>IGFSerializable</c> object array wrapper that can serve
      /// as a distributable object for caching. Though this class provides
      /// compatibility with java Object[] serialization, it provides the
      /// semantics of .NET generic <c>List</c> class.
      /// </summary>
      public ref class CacheableObjectArray
        : public List<Object^>, public IGFSerializable
      {
      public:
        /// <summary>
        /// Allocates a new empty instance.
        /// </summary>
        inline CacheableObjectArray()
          : List<Object^>()
        { }

        /// <summary>
        /// Allocates a new instance copying from the given collection.
        /// </summary>
        /// <param name="collection">
        /// The collection whose elements are copied to this list.
        /// </param>
        inline CacheableObjectArray(IEnumerable<Object^>^ collection)
          : List<Object^>(collection)
        { }

        /// <summary>
        /// Allocates a new empty instance with given initial size.
        /// </summary>
        /// <param name="capacity">
        /// The initial capacity of the vector.
        /// </param>
        inline CacheableObjectArray(int32_t capacity)
          : List<Object^>(capacity)
        { }

        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableObjectArray^ Create()
        {
          return gcnew CacheableObjectArray();
        }

        /// <summary>
        /// Static function to create a new instance copying from the
        /// given collection.
        /// </summary>
        inline static CacheableObjectArray^ Create(
          IEnumerable<Object^>^ collection)
        {
          return gcnew CacheableObjectArray(collection);
        }

        /// <summary>
        /// Static function to create a new instance with given initial size.
        /// </summary>
        inline static CacheableObjectArray^ Create(int32_t capacity)
        {
          return gcnew CacheableObjectArray(capacity);
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
          virtual uint32_t get()
          {
            return GemFireClassIds::CacheableObjectArray;
          }
        }

        // End Region: IGFSerializable Members

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableObjectArray();
        }
      };

    }
  }
}
 } //namespace 
