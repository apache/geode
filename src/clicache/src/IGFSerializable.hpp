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
#include "gfcpp/gf_types.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      namespace Generic
      {

      ref class DataOutput;
      ref class DataInput;
      ref class Serializable;

      /// <summary>
      /// This interface class is the superclass of all user objects 
      /// in the cache that can be serialized.
      /// </summary>
      public interface class IGFSerializable
      {
      public:

        /// <summary>
        /// Serializes this object.
        /// </summary>
        /// <param name="output">
        /// the DataOutput object to use for serializing the object
        /// </param>
        void ToData( DataOutput^ output );

        //bool HasDelta();

        /// <summary>
        /// Deserialize this object, typical implementation should return
        /// the 'this' pointer.
        /// </summary>
        /// <param name="input">
        /// the DataInput stream to use for reading the object data
        /// </param>
        /// <returns>the deserialized object</returns>
        IGFSerializable^ FromData( DataInput^ input );

        /// <summary>
        /// Get the size of this object in bytes.
        /// This is only needed if you use the HeapLRU feature.
        /// </summary>
        /// <remarks>
        /// Note that you can simply return zero if you are not using the HeapLRU feature.
        /// </remarks>
        /// <returns>the size of this object in bytes.</returns>
        property uint32_t ObjectSize
        {
          uint32_t get( );
        }

        /// <summary>
        /// Returns the classId of the instance being serialized.
        /// This is used by deserialization to determine what instance
        /// type to create and deserialize into.
        /// </summary>
        /// <remarks>
        /// The classId must be unique within an application suite
        /// and in the range 0 to ((2^31)-1) both inclusive. An application can
        /// thus define upto 2^31 custom <c>IGFSerializable</c> classes.
        /// Returning a value greater than ((2^31)-1) may result in undefined
        /// behaviour.
        /// </remarks>
        /// <returns>the classId</returns>
        property uint32_t ClassId
        {
          uint32_t get( );
        }

        /// <summary>
        /// Return a string representation of the object.
        /// </summary>
        String^ ToString( );
      };
      } // end namespace generic
    }
  }
}
