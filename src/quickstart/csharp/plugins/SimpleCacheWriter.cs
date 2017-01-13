using System;
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class SimpleCacheWriter<TKey, TVal> : ICacheWriter<TKey, TVal>
  {
    #region ICacheWriter<TKey, TVal> Members

    public bool BeforeUpdate(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheWriter: Received BeforeUpdate event for: {0}", ev.Key);
      return true;
    }

    public bool BeforeCreate(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheWriter: Received BeforeCreate event for: {0}", ev.Key);
      return true;
    }

    public bool BeforeDestroy(EntryEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheWriter: Received BeforeDestroy event for: {0}", ev.Key);
      return true;
    }

    public bool BeforeRegionClear(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheWriter: Received BeforeRegionClear event of region: {0}", ev.Region.Name);
      return true;
    }

    public bool BeforeRegionDestroy(RegionEvent<TKey, TVal> ev)
    {
      Console.WriteLine("SimpleCacheWriter: Received BeforeRegionDestroy event of region: {0}", ev.Region.Name);
      return true;
    }

    public void Close(IRegion<TKey, TVal> region)
    {
      Console.WriteLine("SimpleCacheWriter: Received Close event of region: {0}", region.Name);
    }

    #endregion
  }
}
