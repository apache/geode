/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_includes.hpp"
#include "RegionM.hpp"
#include "PoolM.hpp"
#include "PoolManagerM.hpp"
#include "PoolFactoryM.hpp"
#include "CacheableStringM.hpp"
#include "impl/ManagedString.hpp"
#include "impl/SafeConvert.hpp"
#include "ExceptionTypesM.hpp"


using namespace System;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {
      PoolFactory^ PoolManager::CreateFactory()
      {
        return PoolFactory::Create(gemfire::PoolManager::createFactory().ptr());
      }

      const Dictionary<String^, Pool^>^ PoolManager::GetAll()
      {
        gemfire::HashMapOfPools pools = gemfire::PoolManager::getAll();
        Dictionary<String^, Pool^>^ result = gcnew Dictionary<String^, Pool^>();
        for (gemfire::HashMapOfPools::Iterator iter = pools.begin(); iter != pools.end(); ++iter)
        {
          String^ key = CacheableString::GetString(iter.first().ptr());
          Pool^ val = Pool::Create(iter.second().ptr());
          result->Add(key, val);
        }
        return result;
      }

      Pool^ PoolManager::Find(String^ name)
      {
        ManagedString mg_name( name );
        gemfire::PoolPtr pool = gemfire::PoolManager::find(mg_name.CharPtr);
        return Pool::Create(pool.ptr());
      }

      Pool^ PoolManager::Find(Region^ region)
      {
        return Pool::Create(gemfire::PoolManager::find(gemfire::RegionPtr(GetNativePtr<gemfire::Region>(region))).ptr());
      }

      void PoolManager::Close(Boolean KeepAlive)
      {
        gemfire::PoolManager::close(KeepAlive);
      }

      void PoolManager::Close()
      {
        gemfire::PoolManager::close();
      }
    }
  }
}

