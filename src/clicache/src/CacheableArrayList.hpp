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
#include "CacheableVector.hpp"


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
      /// A mutable <c>IGFSerializable</c> vector wrapper that can serve as
      /// a distributable object for caching. This class extends .NET generic
      /// <c>List</c> class.
      /// </summary>
      ref class CacheableArrayList
        : public CacheableVector
      {
      public:
        /// <summary>
        /// Allocates a new empty instance.
        /// </summary>
        inline CacheableArrayList(System::Collections::IList^ list)
          : CacheableVector(list)
        { }

        
        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableArrayList^ Create()
        {
          return gcnew CacheableArrayList(gcnew System::Collections::Generic::List<Object^>());
        }

        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableArrayList^ Create(System::Collections::IList^ list)
        {
          return gcnew CacheableArrayList(list);
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
          return gcnew CacheableArrayList(gcnew System::Collections::Generic::List<Object^>());
        }
      };
    }
  }
}
 } //namespace 
