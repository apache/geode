/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "StructSet.hpp"
#include "SelectResultsIterator.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TResult>
      bool StructSet<TResult>::IsModifiable::get( )
      {
        return NativePtr->isModifiable( );
      }

      generic<class TResult>
      int32_t StructSet<TResult>::Size::get( )
      {
        return NativePtr->size( );
      }

      generic<class TResult>
      /*GemStone::GemFire::Cache::Generic::IGFSerializable^*/ TResult StructSet<TResult>::default::get( size_t index )
      {
        //return SafeUMSerializableConvertGeneric((NativePtr->operator[](static_cast<int32_t>(index))).ptr());
        return Serializable::GetManagedValueGeneric<TResult>((NativePtr->operator[](static_cast<int32_t>(index))));
      }

      generic<class TResult>
      SelectResultsIterator<TResult>^ StructSet<TResult>::GetIterator( )
      {
        gemfire::SelectResultsIterator* nativeptr =
          new gemfire::SelectResultsIterator(NativePtr->getIterator());
        return SelectResultsIterator<TResult>::Create( nativeptr );
      }

      generic<class TResult>
      System::Collections::Generic::IEnumerator</*GemStone::GemFire::Cache::Generic::IGFSerializable^*/TResult>^
        StructSet<TResult>::GetEnumerator( )
      {
        return GetIterator( );
      }

      generic<class TResult>
      System::Collections::IEnumerator^ StructSet<TResult>::GetIEnumerator( )
      {
        return GetIterator( );
      }

      generic<class TResult>
      size_t StructSet<TResult>::GetFieldIndex( String^ fieldName )
      {
        ManagedString mg_fieldName( fieldName );

        _GF_MG_EXCEPTION_TRY2/* due to auto replace */

          return NativePtr->getFieldIndex( mg_fieldName.CharPtr );

        _GF_MG_EXCEPTION_CATCH_ALL2/* due to auto replace */
      }

      generic<class TResult>
      String^ StructSet<TResult>::GetFieldName( size_t index )
      {
        return ManagedString::Get( NativePtr->getFieldName( static_cast<int32_t> (index) ) );
      }

    }
  }
}
 } //namespace 
