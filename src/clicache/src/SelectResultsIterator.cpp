/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "SelectResultsIterator.hpp"

#include "impl/SafeConvert.hpp"

using namespace System;
namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TResult>
      /*GemStone::GemFire::Cache::Generic::IGFSerializable^*/TResult SelectResultsIterator<TResult>::Current::get( )
      {
        //return SafeUMSerializableConvertGeneric( NativePtr->current( ).ptr( ) ); 
        return Serializable::GetManagedValueGeneric<TResult>(NativePtr->current( ));
      }

      generic<class TResult>
      bool SelectResultsIterator<TResult>::MoveNext( )
      {
        return NativePtr->moveNext( );
      }

      generic<class TResult>
      void SelectResultsIterator<TResult>::Reset( )
      {
        NativePtr->reset( );
      }

      generic<class TResult>
      /*GemStone::GemFire::Cache::Generic::IGFSerializable^*/TResult SelectResultsIterator<TResult>::Next( )
      {
        //return SafeUMSerializableConvertGeneric( NativePtr->next( ).ptr( ) );
        return Serializable::GetManagedValueGeneric<TResult>(NativePtr->next( ));
      }

      generic<class TResult>
      bool SelectResultsIterator<TResult>::HasNext::get( )
      {
        return NativePtr->hasNext( );
      }

    }
  }
}
 } //namespace 
