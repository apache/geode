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
#include "ICacheableKey.hpp"
#include "GemFireClassIdsM.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      /// <summary>
      /// A mutable <c>ICacheableKey</c> to <c>IGFSerializable</c> hash map
      /// that can serve as a distributable object for caching. This class
      /// extends .NET generic <c>Dictionary</c> class.
      /// </summary>
        [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class CacheableHashMap
        : public Dictionary<ICacheableKey^, IGFSerializable^>,
        public IGFSerializable
      {
      public:
        /// <summary>
        /// Allocates a new empty instance.
        /// </summary>
        inline CacheableHashMap()
          : Dictionary<ICacheableKey^, IGFSerializable^>()
        { }

        /// <summary>
        /// Allocates a new instance copying from the given dictionary.
        /// </summary>
        /// <param name="dictionary">
        /// The dictionary whose elements are copied to this HashMap.
        /// </param>
        inline CacheableHashMap(System::Collections::Generic::IDictionary<ICacheableKey^, IGFSerializable^>^
          dictionary)
          : Dictionary<ICacheableKey^, IGFSerializable^>(dictionary)
        { }

        /// <summary>
        /// Allocates a new empty instance with given initial size.
        /// </summary>
        /// <param name="capacity">
        /// The initial capacity of the HashMap.
        /// </param>
        inline CacheableHashMap(int32_t capacity)
          : Dictionary<ICacheableKey^, IGFSerializable^>(capacity)
        { }

        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableHashMap^ Create()
        {
          return gcnew CacheableHashMap();
        }

        /// <summary>
        /// Static function to create a new instance copying from the
        /// given dictionary.
        /// </summary>
        inline static CacheableHashMap^ Create(
          System::Collections::Generic::IDictionary<ICacheableKey^, IGFSerializable^>^ dictionary)
        {
          return gcnew CacheableHashMap(dictionary);
        }

        /// <summary>
        /// Static function to create a new instance with given initial size.
        /// </summary>
        inline static CacheableHashMap^ Create(int32_t capacity)
        {
          return gcnew CacheableHashMap(capacity);
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
          inline virtual uint32_t get()
          {
            return GemFireClassIds::CacheableHashMap;
          }
        }

        // End Region: IGFSerializable Members

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableHashMap();
        }
      };
    }
  }
}
