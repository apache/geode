/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
