/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {
      //generic<class TKey, class TValue>
      PoolFactory/*<TKey, TValue>*/^ PoolManager/*<TKey, TValue>*/::CreateFactory()
      {
        return PoolFactory/*<TKey, TValue>*/::Create(gemfire::PoolManager::createFactory().ptr());
      }

      //generic<class TKey, class TValue>
      const Dictionary<String^, Pool/*<TKey, TValue>*/^>^ PoolManager/*<TKey, TValue>*/::GetAll()
      {
        gemfire::HashMapOfPools pools = gemfire::PoolManager::getAll();
        Dictionary<String^, Pool/*<TKey, TValue>*/^>^ result = gcnew Dictionary<String^, Pool/*<TKey, TValue>*/^>();
        for (gemfire::HashMapOfPools::Iterator iter = pools.begin(); iter != pools.end(); ++iter)
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
        gemfire::PoolPtr pool = gemfire::PoolManager::find(mg_name.CharPtr);
        return Pool/*<TKey, TValue>*/::Create(pool.ptr());
      }

      //generic <class TKey, class TValue>
      Pool/*<TKey, TValue>*/^ PoolManager/*<TKey, TValue>*/::Find(Generic::Region<Object^, Object^>^ region)
      {
        //return Pool::Create(gemfire::PoolManager::find(gemfire::RegionPtr(GetNativePtr<gemfire::Region>(region))).ptr());
        return Pool/*<TKey, TValue>*/::Create(gemfire::PoolManager::find(gemfire::RegionPtr(region->_NativePtr)).ptr());
      }

      //generic<class TKey, class TValue>
      void PoolManager/*<TKey, TValue>*/::Close(Boolean KeepAlive)
      {
        gemfire::PoolManager::close(KeepAlive);
      }

      //generic<class TKey, class TValue>
      void PoolManager/*<TKey, TValue>*/::Close()
      {
        gemfire::PoolManager::close();
      }
    }
  }
}

 } //namespace 
