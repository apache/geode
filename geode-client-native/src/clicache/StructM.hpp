/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#pragma once

#include "gf_defs.hpp"
#include "SerializableM.hpp"
#include "cppcache/Struct.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ref class StructSet;
      interface class IGFSerializable;

      /// <summary>
      /// Encapsulates a row of query struct set.
      /// </summary>
      /// <remarks>
      /// A Struct has a StructSet as its parent. It contains the field values
      /// returned after executing a Query obtained from a QueryService which in turn
      /// is obtained from a Cache.
      /// </remarks>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class Struct sealed
        : public GemStone::GemFire::Cache::Serializable
      {
      public:

        /// <summary>
        /// Get the field value for the given index number.
        /// </summary>
        /// <returns>
        /// The value of the field or null if index is out of bounds.
        /// </returns>
        property IGFSerializable^ GFINDEXER( size_t )
        {
          IGFSerializable^ get( size_t index );
        }

        /// <summary>
        /// Get the field value for the given field name.
        /// </summary>
        /// <returns>The value of the field.</returns>
        /// <exception cref="IllegalArgumentException">
        /// if the field name is not found.
        /// </exception>
        property IGFSerializable^ GFINDEXER( String^ )
        {
          IGFSerializable^ get( String^ fieldName );
        }

        /// <summary>
        /// Get the parent <c>StructSet</c> of this <c>Struct</c>.
        /// </summary>
        /// <returns>
        /// A reference to the parent <c>StructSet</c> of this <c>Struct</c>.
        /// </returns>
        property StructSet^ Set
        {
          StructSet^ get( );
        }

        /// <summary>
        /// Check whether another field value is available to iterate over
        /// in this <c>Struct</c>.
        /// </summary>
        /// <returns>true if available otherwise false.</returns>
        bool HasNext( );

        /// <summary>
        /// Get the number of field values available.
        /// </summary>
        /// <returns>the number of field values available.</returns>
        property size_t Length
        {
          size_t get( );
        }

        /// <summary>
        /// Get the next field value item available in this <c>Struct</c>.
        /// </summary>
        /// <returns>
        /// A reference to the next item in the <c>Struct</c>
        /// or null if no more available.
        /// </returns>
        IGFSerializable^ Next( );


      internal:

        /// <summary>
        /// Factory function to register wrapper
        /// </summary>
        inline static IGFSerializable^ Create( gemfire::Serializable* obj )
        {
          return ( obj != nullptr ?
            gcnew Struct( obj ) : nullptr );
        }

        inline static IGFSerializable^ CreateDeserializable( )
        {
          return gcnew Struct(  ) ;
        }

      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Struct( gemfire::Serializable* nativeptr )
          : GemStone::GemFire::Cache::Serializable( nativeptr ) { }

        inline Struct(  )
          : GemStone::GemFire::Cache::Serializable( gemfire::Struct::createDeserializable()) { }
      };

    }
  }
}
