using System;
//using GemStone.GemFire.Cache;
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class SimpleCacheListener<TKey, TVal> : ICacheListener<TKey, TVal>
  {
    #region ICacheListener<TKey, TVal> Members

    public void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheListener: Received AfterCreate event for: {0}", ev.Key);
    }

    public void AfterDestroy(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheListener: Received AfterDestroy event for: {0}", ev.Key);
    }

    public void AfterInvalidate(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheListener: Received AfterInvalidate event for: {0}", ev.Key);
    }

    public void AfterRegionDestroy(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheListener: Received AfterRegionDestroy event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionClear(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheListener: Received AfterRegionClear event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionInvalidate(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheListener: Received AfterRegionInvalidate event of region: {0}", ev.Region.Name);
    }

    public void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheListener: Received AfterUpdate event of: {0}", ev.Key);
    }

    public void Close(IRegion<TKey, TVal> region)
    {
      Console.WriteLine("SimpleCacheListener: Received Close event of region: {0}", region.Name);
    }

    public void AfterRegionLive(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheListener: Received AfterRegionLive event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionDisconnected(IRegion<TKey, TVal> region)
    {
      Console.WriteLine("SimpleCacheListener: Received AfterRegionDisconnected event of region: {0}", region.Name);
    }

    #endregion
  }
}
