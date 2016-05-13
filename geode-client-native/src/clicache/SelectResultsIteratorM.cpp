/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "SelectResultsIteratorM.hpp"
#include "impl/SafeConvert.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      IGFSerializable^ SelectResultsIterator::Current::get( )
      {
        return SafeUMSerializableConvert( NativePtr->current( ).ptr( ) );
      }

      bool SelectResultsIterator::MoveNext( )
      {
        return NativePtr->moveNext( );
      }

      void SelectResultsIterator::Reset( )
      {
        NativePtr->reset( );
      }

      IGFSerializable^ SelectResultsIterator::Next( )
      {
        return SafeUMSerializableConvert( NativePtr->next( ).ptr( ) );
      }

      bool SelectResultsIterator::HasNext::get( )
      {
        return NativePtr->hasNext( );
      }

    }
  }
}
