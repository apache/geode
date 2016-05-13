/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
//#include "../gf_includesN.hpp"
#include "../ICacheListenerN.hpp"
#include "../../../CacheListenerAdapter.hpp"
#include "../ICacheListenerN.hpp"
#include "../RegionMN.hpp"
//#include "../../../RegionM.hpp"
//#include "../../../CacheM.hpp"

using namespace System;

using namespace GemStone::GemFire::Cache;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      generic<class TKey, class TValue>
      public ref class CacheListenerGeneric : CacheListenerAdapter
      {
        private:

          ICacheListener<TKey, TValue>^ m_listener;

        public:

          void SetCacheListener(ICacheListener<TKey, TValue>^ listener)
          {
            m_listener = listener;
          }

          virtual void AfterUpdate(GemStone::GemFire::Cache::EntryEvent^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::EntryEvent>(event));
            m_listener->AfterUpdate(%gevent);
          }

          virtual void AfterCreate(GemStone::GemFire::Cache::EntryEvent^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::EntryEvent>(event));
            m_listener->AfterCreate(%gevent);
          }

          virtual void AfterInvalidate(GemStone::GemFire::Cache::EntryEvent^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::EntryEvent>(event));
            m_listener->AfterInvalidate(%gevent);
          }

          virtual void AfterDestroy(GemStone::GemFire::Cache::EntryEvent^ event) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::EntryEvent>(event));
            m_listener->AfterDestroy(%gevent);
          }

          virtual void AfterRegionLive(GemStone::GemFire::Cache::RegionEvent^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::RegionEvent>(event));
            m_listener->AfterRegionLive(%gevent);
          }

          virtual void AfterRegionClear(GemStone::GemFire::Cache::RegionEvent^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::RegionEvent>(event));
            m_listener->AfterRegionClear(%gevent);
          }

          virtual void AfterRegionDestroy(GemStone::GemFire::Cache::RegionEvent^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::RegionEvent>(event));
            m_listener->AfterRegionDestroy(%gevent);
          }

          virtual void AfterRegionInvalidate(GemStone::GemFire::Cache::RegionEvent^ event) override
          {
            RegionEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::RegionEvent>(event));
            m_listener->AfterRegionInvalidate(%gevent);
          }

          virtual void AfterRegionDisconnected(GemStone::GemFire::Cache::Region^ event) override
          {
            IRegion<TKey, TValue>^ gevent = Region<TKey, TValue>::Create(Generic::GetNativePtr<gemfire::Region>(event));
            m_listener->AfterRegionDisconnected(gevent);
          }

          virtual void Close(GemStone::GemFire::Cache::Region^ event) override
          {
            IRegion<TKey, TValue>^ gevent = Region<TKey, TValue>::Create(Generic::GetNativePtr<gemfire::Region>(event));
            m_listener->Close(gevent);
          }
      };
    }
    }
  }
}
