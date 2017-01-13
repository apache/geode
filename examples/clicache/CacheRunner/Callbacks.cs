/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

using System;

namespace GemStone.GemFire.Cache.Examples
{
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class ExampleCacheListenerCallback : ICacheListener
  {
    public static ICacheListener Create()
    {
      return new ExampleCacheListenerCallback();
    }

    #region ICacheListener Members

    public void AfterCreate(EntryEvent ev)
    {
      Console.WriteLine("{0}--- Received afterCreate event of: {1}",
        Environment.NewLine, ev.Key);
    }

    public void AfterDestroy(EntryEvent ev)
    {
      Console.WriteLine("{0}--- Received afterDestroy event of: {1}",
        Environment.NewLine, ev.Key);
    }

    public void AfterInvalidate(EntryEvent ev)
    {
      Console.WriteLine("{0}--- Received afterInvalidate event of: {1}",
        Environment.NewLine, ev.Key);
    }

    public void AfterRegionClear(RegionEvent ev)
    {
      Console.WriteLine("{0}--- Received afterRegionClear event of region: {1}",
        Environment.NewLine, ev.Region.Name);
    }

    public void AfterRegionDestroy(RegionEvent ev)
    {
      Console.WriteLine("{0}--- Received afterRegionDestroy event of region: {1}",
        Environment.NewLine, ev.Region.Name);
    }

    public void AfterRegionInvalidate(RegionEvent ev)
    {
      Console.WriteLine("{0}--- Received afterRegionInvalidate event of region: {1}",
        Environment.NewLine, ev.Region.Name);
    }

    public void AfterRegionLive(RegionEvent ev)
    {
      Console.WriteLine("{0}--- Received afterRegionLive event of region: {1}",
        Environment.NewLine, ev.Region.Name);
    }

    public void AfterUpdate(EntryEvent ev)
    {
      Console.WriteLine("{0}--- Received afterUpdate event of: {1}",
        Environment.NewLine, ev.Key);
    }
    
    public void Close(Region region)
    {
      Console.WriteLine("{0}--- Received close event of region: {1}",
        Environment.NewLine, region.Name);
    }
    public void AfterRegionDisconnected(Region region)
    {
      Console.WriteLine("{0}--- Received disconnected event of region: {1}",
        Environment.NewLine, region.Name);
    }
    #endregion
  }
}
