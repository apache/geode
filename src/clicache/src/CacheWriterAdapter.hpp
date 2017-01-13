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
    namespace Cache { namespace Generic
    {

      /// <summary>
      /// Utility class that implements all methods in <c>ICacheWriter</c>
      /// with empty implementations. Applications can subclass this class
      /// and only override the methods for the events of interest.
      /// </summary>
      generic<class TKey, class TValue>
      public ref class CacheWriterAdapter
        : public ICacheWriter<TKey, TValue>
      {
      public:
        virtual bool BeforeUpdate(EntryEvent<TKey, TValue>^ ev)
        {
          return true;
        }

        virtual bool BeforeCreate(EntryEvent<TKey, TValue>^ ev)
        {
          return true;
        }

        virtual bool BeforeDestroy(EntryEvent<TKey, TValue>^ ev)
        {
          return true;
        }

        virtual bool BeforeRegionDestroy(RegionEvent<TKey, TValue>^ ev)
        {
          return true;
        }

        virtual bool BeforeRegionClear(RegionEvent<TKey, TValue>^ ev)
        {
          return true;
        }

        virtual void Close(IRegion<TKey, TValue>^ region)
        {
        }
      };

    }
  }
}
 } //namespace 
