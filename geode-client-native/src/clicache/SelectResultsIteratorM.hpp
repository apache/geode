/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/SelectResultsIterator.hpp"
#include "impl/NativeWrapper.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      interface class IGFSerializable;

      /// <summary>
      /// Iterator for a query result.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class SelectResultsIterator sealed
        : public Internal::UMWrap<gemfire::SelectResultsIterator>,
        public System::Collections::Generic::IEnumerator<IGFSerializable^>
      {
      public:

        /// <summary>
        /// Gets the element in the collection at the current
        /// position of the enumerator.
        /// </summary>
        /// <returns>
        /// The element in the collection at the current position
        /// of the enumerator.
        /// </returns>
        virtual property IGFSerializable^ Current
        {
          virtual IGFSerializable^ get( );
        }

        /// <summary>
        /// Advances the enumerator to the next element of the collection.
        /// </summary>
        /// <returns>
        /// true if the enumerator was successfully advanced to the next
        /// element; false if the enumerator has passed the end of
        /// the collection.
        /// </returns>
        virtual bool MoveNext( );

        /// <summary>
        /// Sets the enumerator to its initial position, which is before
        /// the first element in the collection.
        /// </summary>
        virtual void Reset( );

        /// <summary>
        /// Get the current element and move to the next one.
        /// </summary>
        IGFSerializable^ Next( );

        /// <summary>
        /// Check if there is a next element.
        /// </summary>
        property bool HasNext
        {
          bool get( );
        }


      internal:

        /// <summary>
        /// Internal factory function to wrap a native object pointer inside
        /// this managed class with null pointer check.
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        /// <returns>
        /// The managed wrapper object; null if the native pointer is null.
        /// </returns>
        inline static SelectResultsIterator^ Create(
          gemfire::SelectResultsIterator* nativeptr )
        {
          return ( nativeptr != nullptr ?
            gcnew SelectResultsIterator( nativeptr ) : nullptr );
        }


      private:

        virtual property Object^ ICurrent
        {
          virtual Object^ get( ) sealed
            = System::Collections::IEnumerator::Current::get
          {
            return Current;
          }
        }

        /// <summary>
        /// Private constructor to wrap a native object pointer
        /// </summary>
        /// <param name="nativeptr">The native object pointer</param>
        inline SelectResultsIterator(
          gemfire::SelectResultsIterator* nativeptr )
          : UMWrap( nativeptr, true ) { }
      };

    }
  }
}
