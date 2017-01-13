/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include <gfcpp/ResultSet.hpp>
#include "impl/NativeWrapper.hpp"
#include "ISelectResults.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache {
      
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
        : public Internal::SBWrap<gemfire::ResultSet>, public ISelectResults<TResult>
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
        inline static ResultSet<TResult>^ Create(gemfire::ResultSet* nativeptr)
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
        inline ResultSet(gemfire::ResultSet* nativeptr)
          : SBWrap(nativeptr) { }
      };

    }
  }
}
 } //namespace 
