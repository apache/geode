/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

#pragma once

#include "gf_defs.hpp"
#include "Serializable.hpp"
#include <gfcpp/Struct.hpp>

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache {
      namespace Generic
      {
      generic<class TResult>
      ref class StructSet;

      /// <summary>
      /// Encapsulates a row of query struct set.
      /// </summary>
      /// <remarks>
      /// A Struct has a StructSet as its parent. It contains the field values
      /// returned after executing a Query obtained from a QueryService which in turn
      /// is obtained from a Cache.
      /// </remarks>
      //generic<class TResult>
      public ref class Struct sealed
        : public GemStone::GemFire::Cache::Generic::Serializable
      {
      public:

        /// <summary>
        /// Get the field value for the given index number.
        /// </summary>
        /// <returns>
        /// The value of the field or null if index is out of bounds.
        /// </returns>
        property /*GemStone::GemFire::Cache::Generic::IGFSerializable^*//*TResult*/ Object^ GFINDEXER( size_t )
        {
          /*GemStone::GemFire::Cache::Generic::IGFSerializable^*/ /*TResult*/ Object^ get( size_t index );
        }

        /// <summary>
        /// Get the field value for the given field name.
        /// </summary>
        /// <returns>The value of the field.</returns>
        /// <exception cref="IllegalArgumentException">
        /// if the field name is not found.
        /// </exception>
        property /*GemStone::GemFire::Cache::Generic::IGFSerializable^*//*TResult*/Object^ GFINDEXER( String^ )
        {
          /*GemStone::GemFire::Cache::Generic::IGFSerializable^*//*TResult*/Object^ get( String^ fieldName );
        }

        /// <summary>
        /// Get the parent <c>StructSet</c> of this <c>Struct</c>.
        /// </summary>
        /// <returns>
        /// A reference to the parent <c>StructSet</c> of this <c>Struct</c>.
        /// </returns>
        property GemStone::GemFire::Cache::Generic::StructSet</*TResult*/Object^>^ Set
        {
          GemStone::GemFire::Cache::Generic::StructSet</*TResult*/Object^>^ get( );
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
        /*GemStone::GemFire::Cache::Generic::IGFSerializable^*//*TResult*/Object^ Next( );


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline GemStone::GemFire::Cache::Generic::Struct/*<TResult>*/( ::gemfire::Serializable* nativeptr )
          : GemStone::GemFire::Cache::Generic::Serializable( nativeptr ) { }

        inline GemStone::GemFire::Cache::Generic::Struct/*<TResult>*/(  )
          : GemStone::GemFire::Cache::Generic::Serializable( ::gemfire::Struct::createDeserializable()) { }

      internal:

        /// <summary>
        /// Factory function to register wrapper
        /// </summary>
        inline static GemStone::GemFire::Cache::Generic::IGFSerializable^ /*Struct^*/ /*<TResult>*/ Create( ::gemfire::Serializable* obj )
        {
          return ( obj != nullptr ?
            gcnew GemStone::GemFire::Cache::Generic::Struct/*<TResult>*/( obj ) : nullptr );
          /*return ( obj != nullptr ?
            gcnew Struct( obj ) : nullptr );*/
        }

        inline static GemStone::GemFire::Cache::Generic::IGFSerializable^ CreateDeserializable( )
        {
          return gcnew GemStone::GemFire::Cache::Generic::Struct/*<TResult>*/(  ) ;
          //return gcnew Struct(  ) ;
        }
      };
    }
  }
}
} //namespace 
