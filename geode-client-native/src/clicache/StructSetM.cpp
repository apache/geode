/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "StructSetM.hpp"
#include "SelectResultsIteratorM.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      bool StructSet::IsModifiable::get( )
      {
        return NativePtr->isModifiable( );
      }

      int32_t StructSet::Size::get( )
      {
        return NativePtr->size( );
      }

      IGFSerializable^ StructSet::default::get( size_t index )
      {
        _GF_MG_EXCEPTION_TRY

          return SafeUMSerializableConvert((NativePtr->operator[](static_cast<int32_t>(index))).ptr());

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      SelectResultsIterator^ StructSet::GetIterator( )
      {
        gemfire::SelectResultsIterator* nativeptr =
          new gemfire::SelectResultsIterator(NativePtr->getIterator());

        return SelectResultsIterator::Create( nativeptr );
      }

      System::Collections::Generic::IEnumerator<IGFSerializable^>^
        StructSet::GetEnumerator( )
      {
        return GetIterator( );
      }

      System::Collections::IEnumerator^ StructSet::GetIEnumerator( )
      {
        return GetIterator( );
      }

      size_t StructSet::GetFieldIndex( String^ fieldName )
      {
        ManagedString mg_fieldName( fieldName );

        _GF_MG_EXCEPTION_TRY

          return NativePtr->getFieldIndex( mg_fieldName.CharPtr );

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      String^ StructSet::GetFieldName( size_t index )
      {
        return ManagedString::Get( NativePtr->getFieldName( static_cast<int32_t> (index) ) );
      }

    }
  }
}
