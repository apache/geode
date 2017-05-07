//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  public class ETCacheWriter : ICacheWriter
  {
    public static ICacheWriter Create()
    {
      return new ETCacheWriter();
    }

    #region ICacheWriter Members

    public bool BeforeUpdate(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "BEFORE_UPDATE_COUNT");
      return true;
    }

    public bool BeforeCreate(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "BEFORE_CREATE_COUNT");
      return true;
    }

    public bool BeforeDestroy(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "BEFORE_DESTROY_COUNT");
      return true;
    }
    public bool BeforeRegionClear(RegionEvent rv)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "BEFORE_REGION_CLEAR_COUNT");
      return true;
    }

    public bool BeforeRegionDestroy(RegionEvent rv)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "BEFORE_REGION_DESTROY_COUNT");
      return true;
    }

    public void Close(Region rv)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "CLOSE_COUNT");
    }

    #endregion
  }
}
