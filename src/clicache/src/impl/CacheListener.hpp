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
//#include "../gf_includes.hpp"
#include "../ICacheListener.hpp"
#include "../CacheListenerAdapter.hpp"
#include "../ICacheListener.hpp"
#include "../Region.hpp"
//#include "../../../Region.hpp"
//#include "../../../Cache.hpp"

using namespace System;

using namespace GemStone::GemFire::Cache;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TValue>
      public ref class CacheListenerGeneric : CacheListenerAdapter<Object^, Object^>
      {
        private:

          ICacheListener<TKey, TValue>^ m_listener;

        public:

          void SetCacheListener(ICacheListener<TKey, TValue>^ listener)
          {
            m_listener = listener;
          }

          virtual void AfterUpdate(GemStone::GemFire::Cache::Generic::EntryEvent<Object^, Object^>^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<apache::geode::client::EntryEvent>(event));
            m_listener->AfterUpdate(%gevent);
          }

          virtual void AfterCreate(GemStone::GemFire::Cache::Generic::EntryEvent<Object^, Object^>^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<apache::geode::client::EntryEvent>(event));
            m_listener->AfterCreate(%gevent);
          }

          virtual void AfterInvalidate(GemStone::GemFire::Cache::Generic::EntryEvent<Object^, Object^>^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<apache::geode::client::EntryEvent>(event));
            m_listener->AfterInvalidate(%gevent);
          }

          virtual void AfterDestroy(GemStone::GemFire::Cache::Generic::EntryEvent<Object^, Object^>^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<apache::geode::client::EntryEvent>(event));
            m_listener->AfterDestroy(%gevent);
          }

          virtual void AfterRegionLive(GemStone::GemFire::Cache::Generic::RegionEvent<Object^, Object^>^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Generic::GetNativePtr<apache::geode::client::RegionEvent>(event));
            m_listener->AfterRegionLive(%gevent);
          }

          virtual void AfterRegionClear(GemStone::GemFire::Cache::Generic::RegionEvent<Object^, Object^>^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Generic::GetNativePtr<apache::geode::client::RegionEvent>(event));
            m_listener->AfterRegionClear(%gevent);
          }

          virtual void AfterRegionDestroy(GemStone::GemFire::Cache::Generic::RegionEvent<Object^, Object^>^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Generic::GetNativePtr<apache::geode::client::RegionEvent>(event));
            m_listener->AfterRegionDestroy(%gevent);
          }

          virtual void AfterRegionInvalidate(GemStone::GemFire::Cache::Generic::RegionEvent<Object^, Object^>^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Generic::GetNativePtr<apache::geode::client::RegionEvent>(event));
            m_listener->AfterRegionInvalidate(%gevent);
          }

          virtual void AfterRegionDisconnected(GemStone::GemFire::Cache::Generic::IRegion<Object^, Object^>^ event) override
          {
            GemStone::GemFire::Cache::Generic::IRegion<TKey, TValue>^ gevent = GemStone::GemFire::Cache::Generic::Region<TKey, TValue>::Create(GemStone::GemFire::Cache::Generic::GetNativePtr<apache::geode::client::Region>(reinterpret_cast<GemStone::GemFire::Cache::Generic::Region<Object^, Object^>^>(event)));
            m_listener->AfterRegionDisconnected(gevent);
          }

          virtual void Close(GemStone::GemFire::Cache::Generic::IRegion<Object^, Object^>^ event) override
          {
            GemStone::GemFire::Cache::Generic::IRegion<TKey, TValue>^ gevent = GemStone::GemFire::Cache::Generic::Region<TKey, TValue>::Create(GemStone::GemFire::Cache::Generic::GetNativePtr<apache::geode::client::Region>(reinterpret_cast<GemStone::GemFire::Cache::Generic::Region<Object^, Object^>^>(event)));
            m_listener->Close(gevent);
          }
      };
    }
    }
  }
}
