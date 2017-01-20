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
#include "SelectResultsIterator.hpp"

#include "impl/SafeConvert.hpp"

using namespace System;
namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {

      generic<class TResult>
      /*Apache::Geode::Client::Generic::IGFSerializable^*/TResult SelectResultsIterator<TResult>::Current::get( )
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
      /*Apache::Geode::Client::Generic::IGFSerializable^*/TResult SelectResultsIterator<TResult>::Next( )
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
