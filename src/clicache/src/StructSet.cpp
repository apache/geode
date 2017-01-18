/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        apache::geode::client::SelectResultsIterator* nativeptr =
          new apache::geode::client::SelectResultsIterator(NativePtr->getIterator());
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
