/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include <gfcpp/Struct.hpp>
#include "Struct.hpp"
#include "StructSet.hpp"
#include "ExceptionTypes.hpp"
#include "impl/SafeConvert.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      Object^ Struct::default::get( size_t index )
      {
       /*   return SafeUMSerializableConvertGeneric(static_cast<gemfire::Struct*>(
            NativePtr())->operator[](static_cast<int32_t>(index)).ptr());*/
          return (Serializable::GetManagedValueGeneric<Object^>(static_cast<gemfire::Struct*>(
            NativePtr())->operator[](static_cast<int32_t>(index))));
      }

      Object^ Struct::default::get( String^ fieldName )
      {
        ManagedString mg_fieldName( fieldName );
        /*return SafeUMSerializableConvertGeneric(static_cast<gemfire::Struct*>(
            NativePtr())->operator[](mg_fieldName.CharPtr).ptr());*/

        return (Serializable::GetManagedValueGeneric</*TResult*/Object^>(static_cast<gemfire::Struct*>(
            NativePtr())->operator[](mg_fieldName.CharPtr)));
      }

      StructSet<Object^>^ Struct::Set::get( )
      {
        return StructSet</*TResult*/Object^>::Create(static_cast<gemfire::Struct*>(
          NativePtr())->getStructSet().ptr());
      }

      
      bool Struct/*<TResult>*/::HasNext( )
      {
        return static_cast<gemfire::Struct*>(NativePtr())->hasNext();
      }

      size_t Struct/*<TResult>*/::Length::get( )
      {
        return static_cast<gemfire::Struct*>(NativePtr())->length();
      }

      Object^ Struct/*<TResult>*/::Next( )
      {
        /*return SafeUMSerializableConvertGeneric(static_cast<gemfire::Struct*>(
          NativePtr())->next().ptr());*/
        return (Serializable::GetManagedValueGeneric</*TResult*/Object^>(static_cast<gemfire::Struct*>(
          NativePtr())->next()));
      }

    }
  }
}
 } //namespace 
