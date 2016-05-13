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
    namespace Cache
    {

      /// <summary>
      /// Utility class that implements all methods in <c>ICacheListener</c>
      /// with empty implementations. Applications can subclass this class
      /// and only override the methods for the events of interest.
      /// </summary>
        [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class CacheListenerAdapter
        : public ICacheListener
      {
      public:
        virtual void AfterCreate(EntryEvent^ ev)
        {
        }

        virtual void AfterUpdate(EntryEvent^ ev)
        {
        }

        virtual void AfterInvalidate(EntryEvent^ ev)
        {
        }

        virtual void AfterDestroy(EntryEvent^ ev)
        {
        }

        virtual void AfterRegionInvalidate(RegionEvent^ ev)
        {
        }

        virtual void AfterRegionDestroy(RegionEvent^ ev)
        {
        }

        virtual void AfterRegionLive(RegionEvent^ ev)
        {
        }

        virtual void AfterRegionClear(RegionEvent^ ev)
        {
        }

        virtual void Close(Region^ region)
        {
        }
        virtual void AfterRegionDisconnected( Region^ region )
        {
        }
      };

    }
  }
}
