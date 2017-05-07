/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/StructSet.hpp"
#include "impl/NativeWrapper.hpp"
#include "ICqResults.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ref class SelectResultsIterator;
      interface class IGFSerializable;

      /// <summary>
      /// Encapsulates a query struct set.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class StructSet sealed
        : public Internal::SBWrap<gemfire::StructSet>, public ICqResults
      {
      public:

        /// <summary>
        /// True if this <c>StructSet</c> is modifiable.
        /// </summary>
        /// <returns>returns false always at this time.</returns>
        virtual property bool IsModifiable
        {
          virtual bool get( );
        }

        /// <summary>
        /// The size of the <c>StructSet</c>.
        /// </summary>
        /// <returns>
        /// the number of items in the <c>StructSet</c>.
        /// </returns>
        virtual property int32_t Size
        {
          virtual int32_t get( );
        }

        /// <summary>
        /// Index operator to directly access an item in the <c>StructSet</c>.
        /// </summary>
        /// <exception cref="IllegalArgumentException">
        /// if the index is out of bounds.
        /// </exception>
        /// <returns>Item at the given index.</returns>
        virtual property IGFSerializable^ GFINDEXER( size_t )
        {
          virtual IGFSerializable^ get( size_t index );
        }

        /// <summary>
        /// Get a <c>SelectResultsIterator</c> with which to iterate
        /// over the items in the <c>StructSet</c>.
        /// </summary>
        /// <returns>
        /// The <c>SelectResultsIterator</c> with which to iterate.
        /// </returns>
        virtual SelectResultsIterator^ GetIterator( );

        /// <summary>
        /// Get the index number of the specified field name
        /// in the <c>StructSet</c>.
        /// </summary>
        /// <param name="fieldName">
        /// the field name for which the index is required.
        /// </param>
        /// <returns>the index number of the specified field name.</returns>
        /// <exception cref="IllegalArgumentException">
        /// if the field name is not found.
        /// </exception>
        size_t GetFieldIndex( String^ fieldName );

        /// <summary>
        /// Get the field name of the <c>StructSet</c> from the
        /// specified index number.
        /// </summary>
        /// <param name="index">
        /// the index number of the field name to get.
        /// </param>
        /// <returns>
        /// the field name from the specified index number or null if not found.
        /// </returns>
        String^ GetFieldName( size_t index );


        // Region: IEnumerable<IGFSerializable^> Members

        /// <summary>
        /// Returns an enumerator that iterates through the <c>StructSet</c>.
        /// </summary>
        /// <returns>
        /// A <c>System.Collections.Generic.IEnumerator</c> that
        /// can be used to iterate through the <c>StructSet</c>.
        /// </returns>
        virtual System::Collections::Generic::IEnumerator<IGFSerializable^>^
          GetEnumerator( );

        // End Region: IEnumerable<IGFSerializable^> Members


      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static StructSet^ Create(gemfire::StructSet* nativeptr)
        {
          return (nativeptr != nullptr ? gcnew StructSet(nativeptr) : nullptr);
        }


      private:

        virtual System::Collections::IEnumerator^ GetIEnumerator( ) sealed
          = System::Collections::IEnumerable::GetEnumerator;

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline StructSet(gemfire::StructSet* nativeptr)
          : SBWrap(nativeptr) { }
      };

    }
  }
}
