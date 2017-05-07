/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "ResultSetM.hpp"
#include "SelectResultsIteratorM.hpp"
#include "impl/SafeConvert.hpp"
#include "ExceptionTypesM.hpp"



using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      bool ResultSet::IsModifiable::get( )
      {
        return NativePtr->isModifiable( );
      }

      int32_t ResultSet::Size::get( )
      {
        return NativePtr->size( );
      }

      IGFSerializable^ ResultSet::default::get( size_t index )
      {
        _GF_MG_EXCEPTION_TRY

          return SafeUMSerializableConvert(NativePtr->operator[](static_cast<int32_t>(index)).ptr());

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      SelectResultsIterator^ ResultSet::GetIterator( )
      {
        gemfire::SelectResultsIterator* nativeptr =
          new gemfire::SelectResultsIterator( NativePtr->getIterator( ) );

        return SelectResultsIterator::Create( nativeptr );
      }

      System::Collections::Generic::IEnumerator<IGFSerializable^>^
        ResultSet::GetEnumerator( )
      {
        return GetIterator( );
      }

      System::Collections::IEnumerator^ ResultSet::GetIEnumerator( )
      {
        return GetIterator( );
      }

    }
  }
}
