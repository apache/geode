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


using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      /// <summary>
      /// A mutable <c>IGFSerializable</c> vector wrapper that can serve as
      /// a distributable object for caching.
      /// </summary>
        [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class CacheableStack
        : public Stack<IGFSerializable^>, public IGFSerializable
      {
      public:
        /// <summary>
        /// Allocates a new empty instance.
        /// </summary>
        inline CacheableStack()
          : Stack<IGFSerializable^>()
        { }

        /// <summary>
        /// Allocates a new instance copying from the given collection.
        /// </summary>
        /// <param name="collection">
        /// The collection whose elements are copied to this list.
        /// </param>
        inline CacheableStack(IEnumerable<IGFSerializable^>^ collection)
          : Stack<IGFSerializable^>(collection)
        { }

        /// <summary>
        /// Allocates a new empty instance with given initial size.
        /// </summary>
        /// <param name="capacity">
        /// The initial capacity of the vector.
        /// </param>
        inline CacheableStack(int32_t capacity)
          : Stack<IGFSerializable^>(capacity)
        { }

        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableStack^ Create()
        {
          return gcnew CacheableStack();
        }

        /// <summary>
        /// Static function to create a new instance copying from the
        /// given collection.
        /// </summary>
        inline static CacheableStack^ Create(
          IEnumerable<IGFSerializable^>^ collection)
        {
          return gcnew CacheableStack(collection);
        }

        /// <summary>
        /// Static function to create a new instance with given initial size.
        /// </summary>
        inline static CacheableStack^ Create(int32_t capacity)
        {
          return gcnew CacheableStack(capacity);
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

        // End Region: IGFSerializable Members

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableStack();
        }
      };
    }
  }
}
