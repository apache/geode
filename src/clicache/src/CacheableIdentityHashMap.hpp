/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "gf_defs.hpp"
#include "CacheableHashMap.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {
      /// <summary>
      /// A mutable <c>ICacheableKey</c> to <c>IGFSerializable</c> hash map
      /// that can serve as a distributable object for caching. This class
      /// extends .NET generic <c>Dictionary</c> class. This class is meant
      /// as a means to interoperate with java server side
      /// <c>IdentityHashMap</c> class objects but is intentionally not
      /// intended to provide <c>java.util.IdentityHashMap</c> semantics.
      /// </summary>
      ref class CacheableIdentityHashMap
        : public CacheableHashMap
      {
      public:
        /// <summary>
        /// Allocates a new empty instance.
        /// </summary>
        inline CacheableIdentityHashMap()
          : CacheableHashMap()
        { }

        /// <summary>
        /// Allocates a new instance copying from the given dictionary.
        /// </summary>
        /// <param name="dictionary">
        /// The dictionary whose elements are copied to this HashMap.
        /// </param>
        inline CacheableIdentityHashMap(Object^ dictionary)
          : CacheableHashMap(dictionary)
        { }

        /// <summary>
        /// Allocates a new empty instance with given initial size.
        /// </summary>
        /// <param name="capacity">
        /// The initial capacity of the HashMap.
        /// </param>
        inline CacheableIdentityHashMap(int32_t capacity)
          : CacheableHashMap(capacity)
        { }

        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableIdentityHashMap^ Create()
        {
          return gcnew CacheableIdentityHashMap();
        }

        /// <summary>
        /// Static function to create a new instance copying from the
        /// given dictionary.
        /// </summary>
        inline static CacheableIdentityHashMap^ Create(
          Object^ dictionary)
        {
          return gcnew CacheableIdentityHashMap(dictionary);
        }

        /// <summary>
        /// Static function to create a new instance with given initial size.
        /// </summary>
        inline static CacheableIdentityHashMap^ Create(int32_t capacity)
        {
          return gcnew CacheableIdentityHashMap(capacity);
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
            return GemFireClassIds::CacheableIdentityHashMap;
          }
        }

        // End Region: IGFSerializable Members

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableIdentityHashMap();
        }
      };
    }
  }
}
 } //namespace 
