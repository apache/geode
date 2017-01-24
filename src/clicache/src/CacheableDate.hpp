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
#include "ICacheableKey.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      /// <summary>
      /// An immutable date wrapper that can serve as a distributable
      /// key object for caching as well as being a string value.
      /// </summary>
      public ref class CacheableDate
        : public ICacheableKey
      {
      public:
        /// <summary>
        /// Allocates a new default instance.
        /// </summary>
        inline CacheableDate()
          { }

        /// <summary>
        /// Initializes a new instance of the <c>CacheableDate</c> to the
        /// given <c>System.DateTime</c> value.
        /// </summary>
        /// <param name="dateTime">
        /// A <c>System.DateTime</c> value to initialize this instance.
        /// </param>
        CacheableDate(DateTime dateTime);

        /// <summary>
        /// Static function that returns a new default instance.
        /// </summary>
        inline static CacheableDate^ Create()
        {
          return gcnew CacheableDate();
        }

        /// <summary>
        /// Static function that returns a new instance initialized to the
        /// given <c>System.DateTime</c> value.
        /// </summary>
        inline static CacheableDate^ Create(DateTime dateTime)
        {
          return gcnew CacheableDate(dateTime);
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

        /// <summary>
        /// Return a string representation of the object.
        /// </summary>
        virtual String^ ToString() override;

        // End Region: IGFSerializable Members


        // Region: ICacheableKey Members

        /// <summary>
        /// Return the hashcode for this key.
        /// </summary>
        virtual int32_t GetHashCode() override;

        /// <summary>
        /// Return true if this key matches other object.
        /// </summary>
        virtual bool Equals(ICacheableKey^ other);

        /// <summary>
        /// Return true if this key matches other object.
        /// </summary>
        virtual bool Equals(Object^ obj) override;

        // End Region: ICacheableKey Members

        /// <summary>
        /// Gets the <c>System.DateTime</c> value.
        /// </summary>
        property DateTime Value
        {
          inline DateTime get()
          {
            return m_dateTime;
          }
        }

        /// <summary>
        /// <c>DataTime</c> value since 1/1/1970
        /// </summary>
        static initonly DateTime EpochTime = DateTime(1970, 1, 1,
          0, 0, 0, DateTimeKind::Utc);

        /// <summary>
        /// Factory function to register this class.
        /// </summary>
        static IGFSerializable^ CreateDeserializable()
        {
          return gcnew CacheableDate();
        }

      private:
        DateTime m_dateTime;
        int m_hashcode;
      };
    }
  }
}
 } //namespace 

