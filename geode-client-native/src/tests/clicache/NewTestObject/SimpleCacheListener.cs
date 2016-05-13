using System;
using GemStone.GemFire.Cache;

namespace GemStone.GemFire.Cache.Tests
{
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class SimpleCacheListener : ICacheListener
  {
    #region ICacheListener Members
    public static bool isSuccess = true;
    public void AfterCreate(EntryEvent ev)
    {
      //Console.WriteLine("SimpleCacheListener: Received AfterCreate event for: {0}", ev.Key);
    }

    public void AfterDestroy(EntryEvent ev)
    {
      //Console.WriteLine("SimpleCacheListener: Received AfterDestroy event for: {0}", ev.Key);
    }

    public void AfterInvalidate(EntryEvent ev)
    {
      //Console.WriteLine("SimpleCacheListener: Received AfterInvalidate event for: {0}", ev.Key);
    }

    public void AfterRegionDestroy(RegionEvent ev)
    {
      //Console.WriteLine("SimpleCacheListener: Received AfterRegionDestroy event of region: {0}", ev.Region.Name);
    }

    public void AfterRegionInvalidate(RegionEvent ev)
    {
      //Console.WriteLine("SimpleCacheListener: Received AfterRegionInvalidate event of region: {0}", ev.Region.Name);
    }

    public void AfterUpdate(EntryEvent ev)
    {
      IGFSerializable newval = ev.NewValue;
      if (newval == null && (newval as DeltaEx) == null)
      {
        isSuccess = false;
      }
      else
      {
        isSuccess = true;
      }
    }

    public void Close(Region region)
    {
      //Console.WriteLine("SimpleCacheListener: Received Close event of region: {0}", region.Name);
    }

    public void AfterRegionLive(RegionEvent ev)
    {
      //Console.WriteLine("SimpleCacheListener: Received AfterRegionLive event of region: {0}", ev.Region.Name);
    }
    
    public void AfterRegionDisconnected(Region region)
    {
      //Console.WriteLine("SimpleCacheListener: Received AfterRegionDisconnected event of region: {0}", region.Name);
    }

    public void AfterRegionClear(RegionEvent ev)
    {
      // Do nothing.
    }

    #endregion
  }
}
