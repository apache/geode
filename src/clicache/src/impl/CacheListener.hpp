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

using namespace Apache::Geode::Client;

namespace Apache
{
  namespace Geode
  {
    namespace Client
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

          virtual void AfterUpdate(Apache::Geode::Client::EntryEvent<Object^, Object^>^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Client::GetNativePtr<apache::geode::client::EntryEvent>(event));
            m_listener->AfterUpdate(%gevent);
          }

          virtual void AfterCreate(Apache::Geode::Client::EntryEvent<Object^, Object^>^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Client::GetNativePtr<apache::geode::client::EntryEvent>(event));
            m_listener->AfterCreate(%gevent);
          }

          virtual void AfterInvalidate(Apache::Geode::Client::EntryEvent<Object^, Object^>^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Client::GetNativePtr<apache::geode::client::EntryEvent>(event));
            m_listener->AfterInvalidate(%gevent);
          }

          virtual void AfterDestroy(Apache::Geode::Client::EntryEvent<Object^, Object^>^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Client::GetNativePtr<apache::geode::client::EntryEvent>(event));
            m_listener->AfterDestroy(%gevent);
          }

          virtual void AfterRegionLive(Apache::Geode::Client::RegionEvent<Object^, Object^>^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Client::GetNativePtr<apache::geode::client::RegionEvent>(event));
            m_listener->AfterRegionLive(%gevent);
          }

          virtual void AfterRegionClear(Apache::Geode::Client::RegionEvent<Object^, Object^>^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Client::GetNativePtr<apache::geode::client::RegionEvent>(event));
            m_listener->AfterRegionClear(%gevent);
          }

          virtual void AfterRegionDestroy(Apache::Geode::Client::RegionEvent<Object^, Object^>^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Client::GetNativePtr<apache::geode::client::RegionEvent>(event));
            m_listener->AfterRegionDestroy(%gevent);
          }

          virtual void AfterRegionInvalidate(Apache::Geode::Client::RegionEvent<Object^, Object^>^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Client::GetNativePtr<apache::geode::client::RegionEvent>(event));
            m_listener->AfterRegionInvalidate(%gevent);
          }

          virtual void AfterRegionDisconnected(Apache::Geode::Client::IRegion<Object^, Object^>^ event) override
          {
            Apache::Geode::Client::IRegion<TKey, TValue>^ gevent = Apache::Geode::Client::Region<TKey, TValue>::Create(Apache::Geode::Client::GetNativePtr<apache::geode::client::Region>(reinterpret_cast<Apache::Geode::Client::Region<Object^, Object^>^>(event)));
            m_listener->AfterRegionDisconnected(gevent);
          }

          virtual void Close(Apache::Geode::Client::IRegion<Object^, Object^>^ event) override
          {
            Apache::Geode::Client::IRegion<TKey, TValue>^ gevent = Apache::Geode::Client::Region<TKey, TValue>::Create(Apache::Geode::Client::GetNativePtr<apache::geode::client::Region>(reinterpret_cast<Apache::Geode::Client::Region<Object^, Object^>^>(event)));
            m_listener->Close(gevent);
          }
      };
    }  // namespace Client
  }  // namespace Geode
}  // namespace Apache

