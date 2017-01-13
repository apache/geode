using System;
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Generic.QuickStart
{
  /// <summary>
  /// Capture and display cache events.
  /// </summary>
  class SimpleCacheLoader<TKey, TVal> : ICacheLoader<TKey, TVal>
  {
    #region ICacheLoader Members

    public TVal Load(IRegion<TKey, TVal> region, TKey key, object helper)
    {
      Console.WriteLine("SimpleCacheLoader: Received Load event for region: {0} and key: {1}", region.Name, key);
      return default(TVal);
    }

    public void Close(IRegion<TKey, TVal> region)
    {
      Console.WriteLine("SimpleCacheLoader: Received Close event of region: {0}", region.Name);
    }

    #endregion
  }
}
