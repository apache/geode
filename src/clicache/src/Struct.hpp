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
#include "Serializable.hpp"
#include <gfcpp/Struct.hpp>

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
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
        : public Apache::Geode::Client::Generic::Serializable
      {
      public:

        /// <summary>
        /// Get the field value for the given index number.
        /// </summary>
        /// <returns>
        /// The value of the field or null if index is out of bounds.
        /// </returns>
        property /*Apache::Geode::Client::Generic::IGFSerializable^*//*TResult*/ Object^ GFINDEXER( size_t )
        {
          /*Apache::Geode::Client::Generic::IGFSerializable^*/ /*TResult*/ Object^ get( size_t index );
        }

        /// <summary>
        /// Get the field value for the given field name.
        /// </summary>
        /// <returns>The value of the field.</returns>
        /// <exception cref="IllegalArgumentException">
        /// if the field name is not found.
        /// </exception>
        property /*Apache::Geode::Client::Generic::IGFSerializable^*//*TResult*/Object^ GFINDEXER( String^ )
        {
          /*Apache::Geode::Client::Generic::IGFSerializable^*//*TResult*/Object^ get( String^ fieldName );
        }

        /// <summary>
        /// Get the parent <c>StructSet</c> of this <c>Struct</c>.
        /// </summary>
        /// <returns>
        /// A reference to the parent <c>StructSet</c> of this <c>Struct</c>.
        /// </returns>
        property Apache::Geode::Client::Generic::StructSet</*TResult*/Object^>^ Set
        {
          Apache::Geode::Client::Generic::StructSet</*TResult*/Object^>^ get( );
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
        /*Apache::Geode::Client::Generic::IGFSerializable^*//*TResult*/Object^ Next( );


      private:

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline Apache::Geode::Client::Generic::Struct/*<TResult>*/( ::apache::geode::client::Serializable* nativeptr )
          : Apache::Geode::Client::Generic::Serializable( nativeptr ) { }

        inline Apache::Geode::Client::Generic::Struct/*<TResult>*/(  )
          : Apache::Geode::Client::Generic::Serializable( ::apache::geode::client::Struct::createDeserializable()) { }

      internal:

        /// <summary>
        /// Factory function to register wrapper
        /// </summary>
        inline static Apache::Geode::Client::Generic::IGFSerializable^ /*Struct^*/ /*<TResult>*/ Create( ::apache::geode::client::Serializable* obj )
        {
          return ( obj != nullptr ?
            gcnew Apache::Geode::Client::Generic::Struct/*<TResult>*/( obj ) : nullptr );
          /*return ( obj != nullptr ?
            gcnew Struct( obj ) : nullptr );*/
        }

        inline static Apache::Geode::Client::Generic::IGFSerializable^ CreateDeserializable( )
        {
          return gcnew Apache::Geode::Client::Generic::Struct/*<TResult>*/(  ) ;
          //return gcnew Struct(  ) ;
        }
      };
    }
  }
}
} //namespace 
