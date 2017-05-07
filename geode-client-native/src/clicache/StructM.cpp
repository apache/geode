/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "cppcache/Struct.hpp"
#include "StructM.hpp"
#include "StructSetM.hpp"
#include "impl/SafeConvert.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      IGFSerializable^ Struct::default::get( size_t index )
      {
        _GF_MG_EXCEPTION_TRY

          return SafeUMSerializableConvert(static_cast<gemfire::Struct*>(
            NativePtr())->operator[](static_cast<int32_t>(index)).ptr());

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      IGFSerializable^ Struct::default::get( String^ fieldName )
      {
        ManagedString mg_fieldName( fieldName );

        _GF_MG_EXCEPTION_TRY

          return SafeUMSerializableConvert(static_cast<gemfire::Struct*>(
            NativePtr())->operator[](mg_fieldName.CharPtr).ptr());

        _GF_MG_EXCEPTION_CATCH_ALL
      }

      StructSet^ Struct::Set::get( )
      {
        return StructSet::Create(static_cast<gemfire::Struct*>(
          NativePtr())->getStructSet().ptr());
      }

      bool Struct::HasNext( )
      {
        return static_cast<gemfire::Struct*>(NativePtr())->hasNext();
      }

      size_t Struct::Length::get( )
      {
        return static_cast<gemfire::Struct*>(NativePtr())->length();
      }

      IGFSerializable^ Struct::Next( )
      {
        return SafeUMSerializableConvert(static_cast<gemfire::Struct*>(
          NativePtr())->next().ptr());
      }

    }
  }
}
