/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "ICacheListener.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Utility class that implements all methods in <c>ICacheListener</c>
      /// with empty implementations. Applications can subclass this class
      /// and only override the methods for the events of interest.
      /// </summary>
      generic<class TKey, class TValue>
      public ref class CacheListenerAdapter
        : public ICacheListener<TKey, TValue>
      {
      public:
        virtual void AfterCreate(EntryEvent<TKey, TValue>^ ev)
        {
        }

        virtual void AfterUpdate(EntryEvent<TKey, TValue>^ ev)
        {
        }

        virtual void AfterInvalidate(EntryEvent<TKey, TValue>^ ev)
        {
        }

        virtual void AfterDestroy(EntryEvent<TKey, TValue>^ ev)
        {
        }

        virtual void AfterRegionInvalidate(RegionEvent<TKey, TValue>^ ev)
        {
        }

        virtual void AfterRegionDestroy(RegionEvent<TKey, TValue>^ ev)
        {
        }

        virtual void AfterRegionLive(RegionEvent<TKey, TValue>^ ev)
        {
        }

        virtual void AfterRegionClear(RegionEvent<TKey, TValue>^ ev)
        {
        }

        virtual void Close(GemStone::GemFire::Cache::Generic::IRegion<TKey, TValue>^ region)
        {
        }
		virtual void AfterRegionDisconnected(GemStone::GemFire::Cache::Generic::IRegion<TKey, TValue>^ region)
        {
        }
      };

    }
  }
}
 } //namespace 
