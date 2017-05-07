using System;
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.DUnitFramework;
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class SimpleCacheListener<TKey, TValue> : ICacheListener<TKey, TValue>
  {
    #region ICacheListener Members
    public static bool isSuccess = true;
    public void AfterCreate(EntryEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterCreate event for: {0}", ev.Key);
    }

    public void AfterDestroy(EntryEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterDestroy event for: {0}", ev.Key);
    }

    public void AfterInvalidate(EntryEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterInvalidate event for: {0}", ev.Key);
    }

    public void AfterRegionDestroy(RegionEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterRegionDestroy event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionInvalidate(RegionEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterRegionInvalidate event of region: {0}", ev.Region.Name);
    }

    public void AfterUpdate(EntryEvent<TKey, TValue> ev)
    {
      TValue newval = ev.NewValue;
      if (newval == null && (newval as DeltaEx) == null)
      {
        isSuccess = false;
      }
      else
      {
        isSuccess = true;
      }
    }

    public void Close(IRegion<TKey, TValue> region)
    {
      Util.Log("SimpleCacheListener: Received Close event of region: {0}", region.Name);
    }

    public void AfterRegionLive(RegionEvent<TKey, TValue> ev)
    {
      Util.Log("SimpleCacheListener: Received AfterRegionLive event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionDisconnected(IRegion<TKey, TValue> region)
    {
      Util.Log("SimpleCacheListener: Received AfterRegionDisconnected event of region: {0}", region.Name);
    }

    public void AfterRegionClear(RegionEvent<TKey, TValue> ev)
    {
      // Do nothing.
    }

    #endregion
  }
}
