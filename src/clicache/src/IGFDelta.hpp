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


namespace Apache
{
  namespace Geode
  {
    namespace Client
    {

      ref class DataOutput;
      ref class DataInput;
      ref class Serializable;

      /// <summary>
      /// This interface is used for delta propagation.
      /// To use delta propagation, an application class must implement interfaces <c>IGFDelta</c> as well as <c>IGFSerializable</c>.
      /// The <c>IGFDelta</c> interface methods <c>HasDelta( ), ToDelta( )</c> and <c>FromDelta( )</c> must be implemented by the class, as these methods are used by GemFire
      /// to detect the presence of delta in an object, to serialize the delta, and to apply a serialized delta to an existing object
      /// of the class.
      /// If a customized cloning method is required, the class must also implement the interface <c>System.ICloneable</c>.
      /// To use cloning in delta propagation for a region, the region attribute for cloning must be enabled.
      /// </summary>
      public interface class IGFDelta
      {
      public:
        
        /// <summary>
        /// Writes out delta information to out in a user-defined format. This is
        /// invoked on an application object after GemFire determines the presence
        /// of delta in it by calling <c>HasDelta()</c> on the object.
        /// </summary>
        /// <exception cref="GemFireIOException">
        /// </exception>
        void ToDelta( DataOutput^ out );

        /// <summary>
        /// Reads in delta information to this object in a user-defined format. This is
        /// invoked on an existing application object after GemFire determines the
        /// presence of delta in <c>DataInput</c> instance.
        /// </summary>
        /// <exception cref="InvalidDeltaException">
        /// if the delta in the <c>DataInput</c> instance cannot be applied
        /// to this instance (possible causes may include mismatch of Delta version or logic error).
        /// </exception>
        /// <exception cref="GemFireIOException">
        /// </exception>
        void FromDelta( DataInput^ in );
       
        /// <summary>
        /// <c>HasDelta( )</c> is invoked by GemFire during <c>Region.Put( ICacheableKey, IGFSerializable )</c> to determine if the object contains a delta.
        /// If <c>HasDelta( )</c> returns true, the delta in the object is serialized by invoking <c>ToDelta( DataOutput )</c>.
        /// If <c>HasDelta( )</c> returns false, the object is serialized by invoking <c>IGFSerializable.ToData( DataOutput )</c>.
        /// </summary>
        bool HasDelta( );
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

