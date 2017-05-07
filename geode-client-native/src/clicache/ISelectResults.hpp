/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "cppcache/SelectResults.hpp"
#include "impl/NativeWrapper.hpp"
#include "IGFSerializable.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      ref class SelectResultsIterator;

      /// <summary>
      /// Interface to encapsulate a select query result set.
      /// </summary>
      [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public interface class ISelectResults
        : public System::Collections::Generic::IEnumerable<IGFSerializable^>
      {
      public:

        /// <summary>
        /// True if this <c>ISelectResults</c> is modifiable.
        /// </summary>
        property bool IsModifiable
        {
          bool get( );
        }

        /// <summary>
        /// The size of the <c>ISelectResults</c>.
        /// </summary>
        property int32_t Size
        {
          int32_t get( );
        }

        /// <summary>
        /// Get an object at the given index.
        /// </summary>
        property IGFSerializable^ GFINDEXER( size_t )
        {
          IGFSerializable^ get( size_t index );
        }

        /// <summary>
        /// Get an iterator for the result set.
        /// </summary>
        SelectResultsIterator^ GetIterator( );
      };

    }
  }
}
