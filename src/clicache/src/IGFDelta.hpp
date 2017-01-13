/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#pragma once

#include "gf_defs.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
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

    }
  }
}
 } //namespace 
