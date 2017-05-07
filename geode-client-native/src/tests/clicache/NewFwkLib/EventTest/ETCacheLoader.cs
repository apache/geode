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

  public class ETCacheLoader : ICacheLoader
  {
    public static ICacheLoader Create()
    {
      return new ETCacheLoader();
    }

    #region ICacheLoader Members

    public IGFSerializable Load(Region rp, ICacheableKey key, IGFSerializable helper)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "LOAD_CACHEABLE_STRING_COUNT");
      byte[] buffer = new byte[2000];
      Util.RandBytes(buffer);
      CacheableBytes value = CacheableBytes.Create(buffer);
      return value;
    }

    public void Close(Region rp)
    {
      Util.BBIncrement(EventTest.EventCountersBB, "CLOSE_COUNT");
    }

    #endregion
  }
}
