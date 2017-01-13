/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

//#include "gf_includes.hpp"
#include "CacheableKey.hpp"
#include "CacheableString.hpp"
#include "CacheableBuiltins.hpp"

using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
     // generic<class TKey>
      int32_t CacheableKey::GetHashCode()
      {
        return static_cast<gemfire::CacheableKey*>(NativePtr())->hashcode();
      }

     // generic<class TKey>
      bool CacheableKey::Equals(Generic::ICacheableKey^ other)
      {
        if (other == nullptr || other->ClassId != ClassId) {
          return false;
        }
        return static_cast<gemfire::CacheableKey*>(NativePtr())->operator==(
          *static_cast<gemfire::CacheableKey*>(
            ((Generic::CacheableKey^)other)->NativePtr()));
      }

      //generic<class TKey>
      bool CacheableKey::Equals(Object^ obj)
      {
        CacheableKey^ otherKey =
          dynamic_cast<CacheableKey^>(obj);

        if (otherKey != nullptr) {
          return static_cast<gemfire::CacheableKey*>(NativePtr())->operator==(
            *static_cast<gemfire::CacheableKey*>(otherKey->NativePtr()));
        }
        return false;
      }

      //generic<class TKey>
      CacheableKey::operator CacheableKey^ (Byte value)
      {
        return (CacheableKey^) CacheableByte::Create(value);
      }

      //generic<class TKey>
      CacheableKey::operator CacheableKey^ (bool value)
      {
        return (CacheableKey^) CacheableBoolean::Create(value);
      }

      //generic<class TKey>
      CacheableKey::operator CacheableKey^ (Char value)
      {
        return (CacheableKey^) CacheableCharacter::Create(value);
      }

      //generic<class TKey>
      CacheableKey::operator CacheableKey^ (Double value)
      {
        return (CacheableKey^) CacheableDouble::Create(value);
      }

      //generic<class TKey>
      CacheableKey::operator CacheableKey^ (Single value)
      {
        return (CacheableKey^) CacheableFloat::Create(value);
      }

      //generic<class TKey>
      CacheableKey::operator CacheableKey^ (int16_t value)
      {
        return (CacheableKey^) CacheableInt16::Create(value);
      }

      //generic<class TKey>
      CacheableKey::operator CacheableKey^ (int32_t value)
      {
        return (CacheableKey^) CacheableInt32::Create(value);
      }

     // generic<class TKey>
      CacheableKey::operator CacheableKey^ (int64_t value)
      {
        return (CacheableKey^) CacheableInt64::Create(value);
      }

      //generic<class TKey>
      CacheableKey::operator CacheableKey^ (String^ value)
      {
        return dynamic_cast<CacheableKey^>(CacheableString::Create(value));
      }
    }
  }
}
} //namespace 
