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
#include "IGFSerializable.hpp"
#include "GeodeClassIds.hpp"


using namespace System;
using namespace System::Collections::Generic;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      /// <summary>
      /// A mutable <c>IGFSerializable</c> vector wrapper that can serve as
      /// a distributable object for caching. This class extends .NET generic
      /// <c>List</c> class.
      /// </summary>
      ref class CacheableVector
        : public IGFSerializable
      {
      public:
        /// <summary>
        /// Allocates a new empty instance.
        /// </summary>
        inline CacheableVector(System::Collections::IList^ arrayList)
        {
          m_arrayList = arrayList;
        }


        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableVector^ Create()
        {
          return gcnew CacheableVector(gcnew System::Collections::ArrayList());
        }

        /// <summary>
        /// Static function to create a new empty instance.
        /// </summary>
        inline static CacheableVector^ Create(System::Collections::IList^ list)
        {
          return gcnew CacheableVector(list);
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
            return GeodeClassIds::CacheableVector;
          }
        }

        virtual property System::Collections::IList^ Value
        {
          virtual System::Collections::IList^ get()
          {
            return m_arrayList;
          }
        }

        // End Region: IGFSerializable Members

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableVector(gcnew System::Collections::ArrayList());
        }

      private:
        System::Collections::IList^ m_arrayList;
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

