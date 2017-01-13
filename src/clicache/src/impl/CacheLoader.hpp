/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#pragma once

//#include "../gf_includes.hpp"
//#include "../../../ICacheLoader.hpp"
#include "../ICacheLoader.hpp"
//#include "../Serializable.hpp"
#include "../Region.hpp"
#include "SafeConvert.hpp"
//#include "../legacy/impl/SafeConvert.hpp"
//#include "../../../Region.hpp"
//#include "../../../Cache.hpp"

using namespace System;

//using namespace GemStone::GemFire::Cache::Generic;

namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      public interface class ICacheLoaderProxy
      {
      public:
        gemfire::CacheablePtr load( const gemfire::RegionPtr& region,
          const gemfire::CacheableKeyPtr& key, const gemfire::UserDataPtr& helper );

        void close( const gemfire::RegionPtr& region );
      };

      generic<class TKey, class TValue>
      public ref class CacheLoaderGeneric : ICacheLoaderProxy // : GemStone::GemFire::Cache::ICacheLoader /*<Object^, Object^>*/
      {
        private:

          ICacheLoader<TKey, TValue>^ m_loader;

        public:

          void SetCacheLoader(ICacheLoader<TKey, TValue>^ loader)
          {
            m_loader = loader;
          }

          virtual gemfire::CacheablePtr load( const gemfire::RegionPtr& region,
            const gemfire::CacheableKeyPtr& key, const gemfire::UserDataPtr& helper )
          {
            IRegion<TKey, TValue>^ gregion = Region<TKey, TValue>::Create(region.ptr());

            TKey gkey = Serializable::GetManagedValueGeneric<TKey>(key);

            Object^ ghelper = Serializable::GetManagedValueGeneric<Object^>(helper);

            //return SafeMSerializableConvertGeneric(m_loader->Load(gregion, gkey, ghelper));
            return Serializable::GetUnmanagedValueGeneric<TValue>(m_loader->Load(gregion, gkey, ghelper));
          }

          virtual void close( const gemfire::RegionPtr& region )
          {
            IRegion<TKey, TValue>^ gregion = Region<TKey, TValue>::Create(region.ptr());
            m_loader->Close(gregion);
          }
      };
    }
    }
  }
}
