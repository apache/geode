/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "CacheableVectorM.hpp"


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
      /// a distributable object for caching. This class extends .NET generic
      /// <c>List</c> class.
      /// </summary>
        [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class CacheableArrayList
        : public CacheableVector
      {
      public:
        /// <summary>
        /// Allocates a new empty instance.
        /// </summary>
        inline CacheableArrayList()
          : CacheableVector()
        { }

        /// <summary>
        /// Allocates a new instance copying from the given collection.
        /// </summary>
        /// <param name="collection">
        /// The collection whose elements are copied to this list.
        /// </param>
        inline CacheableArrayList(IEnumerable<IGFSerializable^>^ collection)
          : CacheableVector(collection)
        { }

        /// <summary>
        /// Allocates a new empty instance with given initial size.
        /// </summary>
        /// <param name="capacity">
        /// The initial capacity of the vector.
        /// </param>
        inline CacheableArrayList(int32_t capacity)
          : CacheableVector(capacity)
        { }

        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableArrayList^ Create()
        {
          return gcnew CacheableArrayList();
        }

        /// <summary>
        /// Static function to create a new instance copying from the
        /// given collection.
        /// </summary>
        inline static CacheableArrayList^ Create(
          IEnumerable<IGFSerializable^>^ collection)
        {
          return gcnew CacheableArrayList(collection);
        }

        /// <summary>
        /// Static function to create a new instance with given initial size.
        /// </summary>
        inline static CacheableArrayList^ Create(int32_t capacity)
        {
          return gcnew CacheableArrayList(capacity);
        }

        // Region: IGFSerializable Members

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
            return GemFireClassIds::CacheableArrayList;
          }
        }

        // End Region: IGFSerializable Members

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableArrayList();
        }
      };
    }
  }
}
