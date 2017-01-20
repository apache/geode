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
#include <gfcpp/ResultSet.hpp>
#include "impl/NativeWrapper.hpp"
#include "ISelectResults.hpp"


using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {
			interface class IGFSerializable;

      generic<class TResult>
      ref class SelectResultsIterator;
      /// <summary>
      /// Encapsulates a query result set.
      /// It specifies the interface for the resultset obtained from the
      /// Gemfire cache server
      /// </summary>
      generic<class TResult>
      public ref class ResultSet sealed
        : public Internal::SBWrap<apache::geode::client::ResultSet>, public ISelectResults<TResult>
      {
      public:

        /// <summary>
        /// True if this <c>ResultSet</c> is modifiable.
        /// </summary>
        virtual property bool IsModifiable
        {
          virtual bool get( );
        }

        /// <summary>
        /// The size of the <c>ResultSet</c>.
        /// </summary>
        virtual property int32_t Size
        {
          virtual int32_t get( );
        }

        /// <summary>
        /// Get an object at the given index.
        /// </summary>
        virtual property /*IGFSerializable^*/TResult GFINDEXER( size_t )
        {
          virtual /*IGFSerializable^*/TResult get( size_t index );
        }

        /// <summary>
        /// Get an iterator for the result set.
        /// </summary>
        virtual SelectResultsIterator<TResult>^ GetIterator( );

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        /// <returns>
        /// A <c>System.Collections.Generic.IEnumerator</c> that
        /// can be used to iterate through the <c>ResultSet</c>.
        /// </returns>
        virtual System::Collections::Generic::IEnumerator</*IGFSerializable^*/TResult>^
          GetEnumerator( );


      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static ResultSet<TResult>^ Create(apache::geode::client::ResultSet* nativeptr)
        {
          return (nativeptr != nullptr ? gcnew ResultSet(nativeptr) : nullptr);
        }


      private:

        virtual System::Collections::IEnumerator^ GetIEnumerator( ) sealed
          = System::Collections::IEnumerable::GetEnumerator;

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline ResultSet(apache::geode::client::ResultSet* nativeptr)
          : SBWrap(nativeptr) { }
      };

    }
  }
}
 } //namespace 
