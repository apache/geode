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

#pragma once

//#include "gf_includes.hpp"
#include "Region.hpp"
#include "Pool.hpp"
#include "PoolManager.hpp"
#include "PoolFactory.hpp"
#include "CacheableString.hpp"
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
      //generic<class TKey, class TValue>
      PoolFactory/*<TKey, TValue>*/^ PoolManager/*<TKey, TValue>*/::CreateFactory()
      {
        return PoolFactory/*<TKey, TValue>*/::Create(apache::geode::client::PoolManager::createFactory().ptr());
      }

      //generic<class TKey, class TValue>
      const Dictionary<String^, Pool/*<TKey, TValue>*/^>^ PoolManager/*<TKey, TValue>*/::GetAll()
      {
        apache::geode::client::HashMapOfPools pools = apache::geode::client::PoolManager::getAll();
        Dictionary<String^, Pool/*<TKey, TValue>*/^>^ result = gcnew Dictionary<String^, Pool/*<TKey, TValue>*/^>();
        for (apache::geode::client::HashMapOfPools::Iterator iter = pools.begin(); iter != pools.end(); ++iter)
        {
          String^ key = CacheableString::GetString(iter.first().ptr());
          Pool/*<TKey, TValue>*/^ val = Pool/*<TKey, TValue>*/::Create(iter.second().ptr());
          result->Add(key, val);
        }
        return result;
      }

      //generic<class TKey, class TValue>
      Pool/*<TKey, TValue>*/^ PoolManager/*<TKey, TValue>*/::Find(String^ name)
      {
        ManagedString mg_name( name );
        apache::geode::client::PoolPtr pool = apache::geode::client::PoolManager::find(mg_name.CharPtr);
        return Pool/*<TKey, TValue>*/::Create(pool.ptr());
      }

      //generic <class TKey, class TValue>
      Pool/*<TKey, TValue>*/^ PoolManager/*<TKey, TValue>*/::Find(Generic::Region<Object^, Object^>^ region)
      {
        //return Pool::Create(apache::geode::client::PoolManager::find(apache::geode::client::RegionPtr(GetNativePtr<apache::geode::client::Region>(region))).ptr());
        return Pool/*<TKey, TValue>*/::Create(apache::geode::client::PoolManager::find(apache::geode::client::RegionPtr(region->_NativePtr)).ptr());
      }

      //generic<class TKey, class TValue>
      void PoolManager/*<TKey, TValue>*/::Close(Boolean KeepAlive)
      {
        apache::geode::client::PoolManager::close(KeepAlive);
      }

      //generic<class TKey, class TValue>
      void PoolManager/*<TKey, TValue>*/::Close()
      {
        apache::geode::client::PoolManager::close();
      }
    }
  }
}

 } //namespace 
