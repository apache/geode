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
#include <gfcpp/Struct.hpp>
#include "Struct.hpp"
#include "StructSet.hpp"
#include "ExceptionTypes.hpp"
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
      Object^ Struct::default::get( size_t index )
      {
       /*   return SafeUMSerializableConvertGeneric(static_cast<apache::geode::client::Struct*>(
            NativePtr())->operator[](static_cast<int32_t>(index)).ptr());*/
          return (Serializable::GetManagedValueGeneric<Object^>(static_cast<apache::geode::client::Struct*>(
            NativePtr())->operator[](static_cast<int32_t>(index))));
      }

      Object^ Struct::default::get( String^ fieldName )
      {
        ManagedString mg_fieldName( fieldName );
        /*return SafeUMSerializableConvertGeneric(static_cast<apache::geode::client::Struct*>(
            NativePtr())->operator[](mg_fieldName.CharPtr).ptr());*/

        return (Serializable::GetManagedValueGeneric</*TResult*/Object^>(static_cast<apache::geode::client::Struct*>(
            NativePtr())->operator[](mg_fieldName.CharPtr)));
      }

      StructSet<Object^>^ Struct::Set::get( )
      {
        return StructSet</*TResult*/Object^>::Create(static_cast<apache::geode::client::Struct*>(
          NativePtr())->getStructSet().ptr());
      }

      
      bool Struct/*<TResult>*/::HasNext( )
      {
        return static_cast<apache::geode::client::Struct*>(NativePtr())->hasNext();
      }

      size_t Struct/*<TResult>*/::Length::get( )
      {
        return static_cast<apache::geode::client::Struct*>(NativePtr())->length();
      }

      Object^ Struct/*<TResult>*/::Next( )
      {
        /*return SafeUMSerializableConvertGeneric(static_cast<apache::geode::client::Struct*>(
          NativePtr())->next().ptr());*/
        return (Serializable::GetManagedValueGeneric</*TResult*/Object^>(static_cast<apache::geode::client::Struct*>(
          NativePtr())->next()));
      }

    }
  }
}
 } //namespace 
