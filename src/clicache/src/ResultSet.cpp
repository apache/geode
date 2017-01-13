/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "ResultSet.hpp"
#include "SelectResultsIterator.hpp"
#include "impl/ManagedString.hpp"
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
      bool ResultSet<TResult>::IsModifiable::get( )
      {
        return NativePtr->isModifiable( );
      }

      generic<class TResult>
      int32_t ResultSet<TResult>::Size::get( )
      {
        return NativePtr->size( );
      }

      generic<class TResult>
      /*IGFSerializable^*/TResult ResultSet<TResult>::default::get( size_t index )
      {
          //return SafeUMSerializableConvertGeneric(NativePtr->operator[](static_cast<int32_t>(index)).ptr());
           return (Serializable::GetManagedValueGeneric<TResult>(NativePtr->operator[](static_cast<int32_t>(index))));
      }

      generic<class TResult>
      SelectResultsIterator<TResult>^ ResultSet<TResult>::GetIterator( )
      {
        gemfire::SelectResultsIterator* nativeptr =
          new gemfire::SelectResultsIterator( NativePtr->getIterator( ) );

        return SelectResultsIterator<TResult>::Create( nativeptr );
      }

      generic<class TResult>
      System::Collections::Generic::IEnumerator</*IGFSerializable^*/TResult>^
        ResultSet<TResult>::GetEnumerator( )
      {
        return GetIterator( );
      }

      generic<class TResult>
      System::Collections::IEnumerator^ ResultSet<TResult>::GetIEnumerator( )
      {
        return GetIterator( );
      }

    }
  }
}
 } //namespace 
