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

  public class ETCacheListener : ICacheListener
  {
    public static ICacheListener Create()
    {
      return new ETCacheListener();
    }

    #region ICacheListener Members

    public void AfterCreate(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_CREATE_COUNT");
    }

    public void AfterDestroy(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_DESTROY_COUNT");
    }

    public void AfterInvalidate(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_INVALIDATE_COUNT");
    }

    public void AfterRegionDestroy(RegionEvent ev)
    {
      //Util.BBIncrement("AFTER_REGION_DESTROY_COUNT");
    }

    public void AfterRegionInvalidate(RegionEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_REGION_INVALIDATE_COUNT");
    }
    public void AfterRegionClear(RegionEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_REGION_CLEAR_COUNT");
    }


    public void AfterUpdate(EntryEvent ev)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "AFTER_UPDATE_COUNT");
    }

    public virtual void AfterRegionLive(RegionEvent ev)
    {
      //Util.BBIncrement("AFTER_REGION_LIVE_COUNT");
    }

    public void Close(Region region)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "CLOSE_COUNT");
    }
    public void AfterRegionDisconnected(Region region)
    {
    }
    #endregion
  }
}
