/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#pragma once

#include "gf_defs.hpp"
#include "ICacheWriter.hpp"


namespace GemStone
{
  namespace GemFire
  {
    namespace Cache
    {

      /// <summary>
      /// Utility class that implements all methods in <c>ICacheWriter</c>
      /// with empty implementations. Applications can subclass this class
      /// and only override the methods for the events of interest.
      /// </summary>
        [Obsolete("Use classes and APIs from the GemStone.GemFire.Cache.Generic namespace")]
      public ref class CacheWriterAdapter
        : public ICacheWriter
      {
      public:
        virtual bool BeforeUpdate(EntryEvent^ ev)
        {
          return true;
        }

        virtual bool BeforeCreate(EntryEvent^ ev)
        {
          return true;
        }

        virtual bool BeforeDestroy(EntryEvent^ ev)
        {
          return true;
        }

        virtual bool BeforeRegionDestroy(RegionEvent^ ev)
        {
          return true;
        }

        virtual bool BeforeRegionClear(RegionEvent^ ev)
        {
          return true;
        }

        virtual void Close(Region^ region)
        {
        }
      };

    }
  }
}
