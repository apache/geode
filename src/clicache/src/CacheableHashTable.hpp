/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "CacheableHashMap.hpp"
#include "DataInput.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      /// <summary>
      /// A mutable <c>ICacheableKey</c> to <c>IGFSerializable</c> hash table
      /// that can serve as a distributable object for caching. This class
      /// extends .NET generic <c>Dictionary</c> class.
      /// </summary>
      ref class CacheableHashTable
        : public CacheableHashMap
      {
      public:

        /// <summary>
        /// Allocates a new empty instance.
        /// </summary>
        inline CacheableHashTable()
          : CacheableHashMap()
        { }

        /// <summary>
        /// Allocates a new instance copying from the given dictionary.
        /// </summary>
        /// <param name="dictionary">
        /// The dictionary whose elements are copied to this HashTable.
        /// </param>
        inline CacheableHashTable(Object^ dictionary)
          : CacheableHashMap(dictionary)
        { }

        /// <summary>
        /// Allocates a new empty instance with given initial size.
        /// </summary>
        /// <param name="capacity">
        /// The initial capacity of the HashTable.
        /// </param>
        inline CacheableHashTable(int32_t capacity)
          : CacheableHashMap(capacity)
        { }


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
            return GemFireClassIds::CacheableHashTable;
          }
        }

        // End Region: IGFSerializable Members

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableHashTable();
        }

        virtual IGFSerializable^ FromData(DataInput^ input) override
        {
          m_dictionary = input->ReadHashtable();
          return this;
        }
      internal:

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ Create(Object^ hashtable)
        {
          return gcnew CacheableHashTable(hashtable);
        }
      };

    }
  }
}
 } //namespace 
