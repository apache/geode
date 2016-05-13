/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "gf_includes.hpp"
#include "SerializableM.hpp"
#include "CacheableKeyM.hpp"
#include "CacheableStringM.hpp"
#include "LogM.hpp"
#include "CacheableBuiltinsM.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    { 
      int32_t GemStone::GemFire::Cache::CacheableKey::GetHashCode()
      {
        return static_cast<gemfire::CacheableKey*>(NativePtr())->hashcode();
      }

      bool GemStone::GemFire::Cache::CacheableKey::Equals(GemStone::GemFire::Cache::ICacheableKey^ other)
      {
        if (other == nullptr || other->ClassId != ClassId) {
          return false;
        }
        return static_cast<gemfire::CacheableKey*>(NativePtr())->operator==(
          *static_cast<gemfire::CacheableKey*>(
            ((GemStone::GemFire::Cache::CacheableKey^)other)->NativePtr()));
      }

      bool GemStone::GemFire::Cache::CacheableKey::Equals(Object^ obj)
      {
        GemStone::GemFire::Cache::CacheableKey^ otherKey =
          dynamic_cast<GemStone::GemFire::Cache::CacheableKey^>(obj);

        if (otherKey != nullptr) {
          return static_cast<gemfire::CacheableKey*>(NativePtr())->operator==(
            *static_cast<gemfire::CacheableKey*>(otherKey->NativePtr()));
        }
        return false;
      }

      GemStone::GemFire::Cache::CacheableKey::operator GemStone::GemFire::Cache::CacheableKey^ (Byte value)
      {
        return GemStone::GemFire::Cache::CacheableByte::Create(value);
      }

      GemStone::GemFire::Cache::CacheableKey::operator GemStone::GemFire::Cache::CacheableKey^ (bool value)
      {
        return GemStone::GemFire::Cache::CacheableBoolean::Create(value);
      }

      GemStone::GemFire::Cache::CacheableKey::operator GemStone::GemFire::Cache::CacheableKey^ (Char value)
      {
        return GemStone::GemFire::Cache::CacheableCharacter::Create(value);
      }

      GemStone::GemFire::Cache::CacheableKey::operator GemStone::GemFire::Cache::CacheableKey^ (Double value)
      {
        return GemStone::GemFire::Cache::CacheableDouble::Create(value);
      }

      GemStone::GemFire::Cache::CacheableKey::operator GemStone::GemFire::Cache::CacheableKey^ (Single value)
      {
        return GemStone::GemFire::Cache::CacheableFloat::Create(value);
      }

      GemStone::GemFire::Cache::CacheableKey::operator GemStone::GemFire::Cache::CacheableKey^ (int16_t value)
      {
        return GemStone::GemFire::Cache::CacheableInt16::Create(value);
      }

      GemStone::GemFire::Cache::CacheableKey::operator GemStone::GemFire::Cache::CacheableKey^ (int32_t value)
      {
        return GemStone::GemFire::Cache::CacheableInt32::Create(value);
      }

      GemStone::GemFire::Cache::CacheableKey::operator GemStone::GemFire::Cache::CacheableKey^ (int64_t value)
      {
        return GemStone::GemFire::Cache::CacheableInt64::Create(value);
      }

      GemStone::GemFire::Cache::CacheableKey::operator GemStone::GemFire::Cache::CacheableKey^ (String^ value)
      {
        return GemStone::GemFire::Cache::CacheableString::Create(value);
      }
    }
  }
}
