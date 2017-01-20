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
#include "CacheableKey.hpp"
#include "CacheableString.hpp"
#include "CacheableBuiltins.hpp"

using namespace System;

namespace Apache
{
  namespace Geode
  {
    namespace Client
    {
namespace Generic
    {
     // generic<class TKey>
      int32_t CacheableKey::GetHashCode()
      {
        return static_cast<apache::geode::client::CacheableKey*>(NativePtr())->hashcode();
      }

     // generic<class TKey>
      bool CacheableKey::Equals(Generic::ICacheableKey^ other)
      {
        if (other == nullptr || other->ClassId != ClassId) {
          return false;
        }
        return static_cast<apache::geode::client::CacheableKey*>(NativePtr())->operator==(
          *static_cast<apache::geode::client::CacheableKey*>(
            ((Generic::CacheableKey^)other)->NativePtr()));
      }

      //generic<class TKey>
      bool CacheableKey::Equals(Object^ obj)
      {
        CacheableKey^ otherKey =
          dynamic_cast<CacheableKey^>(obj);

        if (otherKey != nullptr) {
          return static_cast<apache::geode::client::CacheableKey*>(NativePtr())->operator==(
            *static_cast<apache::geode::client::CacheableKey*>(otherKey->NativePtr()));
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
