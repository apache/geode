/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
//#include "../gf_includes.hpp"
//#include "../../../ICacheWriter.hpp"
#include "../CacheWriterAdapter.hpp"
#include "../ICacheWriter.hpp"
//#include "../Region.hpp"
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
      public ref class CacheWriterGeneric : GemStone::GemFire::Cache::Generic::CacheWriterAdapter<Object^, Object^>
      {
        private:

          ICacheWriter<TKey, TValue>^ m_writer;

        public:

          void SetCacheWriter(ICacheWriter<TKey, TValue>^ writer)
          {
            m_writer = writer;
          }

          virtual bool BeforeUpdate( GemStone::GemFire::Cache::Generic::EntryEvent<Object^, Object^>^ ev ) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::EntryEvent>(ev));
            return m_writer->BeforeUpdate(%gevent);
          }

          virtual bool BeforeCreate(GemStone::GemFire::Cache::Generic::EntryEvent<Object^, Object^>^ ev) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::EntryEvent>(ev));
            return m_writer->BeforeCreate(%gevent);
          }

          virtual bool BeforeDestroy(GemStone::GemFire::Cache::Generic::EntryEvent<Object^, Object^>^ ev) override
          {
            EntryEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::EntryEvent>(ev));
            return m_writer->BeforeDestroy(%gevent);
          }

          virtual bool BeforeRegionClear( GemStone::GemFire::Cache::Generic::RegionEvent<Object^, Object^>^ ev ) override
          {
            RegionEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::RegionEvent>(ev));
            return m_writer->BeforeRegionClear(%gevent);
          }

          virtual bool BeforeRegionDestroy(GemStone::GemFire::Cache::Generic::RegionEvent<Object^, Object^>^ ev) override
          {
            RegionEvent<TKey, TValue> gevent(Generic::GetNativePtr<gemfire::RegionEvent>(ev));
            return m_writer->BeforeRegionDestroy(%gevent);
          }
          
          virtual void Close(GemStone::GemFire::Cache::Generic::Region<Object^, Object^>^ region) override
          {
            IRegion<TKey, TValue>^ gregion = GemStone::GemFire::Cache::Generic::Region<TKey, TValue>::Create(Generic::GetNativePtr<gemfire::Region>(region));
            m_writer->Close(gregion);
          }
      };
    }
    }
  }
}
