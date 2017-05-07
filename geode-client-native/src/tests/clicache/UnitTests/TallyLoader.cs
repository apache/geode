//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using GemStone.GemFire.DUnitFramework;

  class TallyLoader : ICacheLoader
  {
    #region Private members

    private int m_loads = 0;

    #endregion

    #region Public accessors

    public int Loads
    {
      get
      {
        return m_loads;
      }
    }

    #endregion

    public int ExpectLoads(int expected)
    {
      int tries = 0;
      while ((m_loads < expected) && (tries < 200))
      {
        Thread.Sleep(100);
        tries++;
      }
      return m_loads;
    }

    public void Reset()
    {
      m_loads = 0;
    }

    public void ShowTallies()
    {
      Util.Log("TallyLoader state: (loads = {0})", Loads);
    }

    public static TallyLoader Create()
    {
      return new TallyLoader();
    }

    #region ICacheLoader Members

    public virtual IGFSerializable Load(Region region, ICacheableKey key,
      IGFSerializable helper)
    {
      return new CacheableInt32(m_loads++);
    }

    public virtual void Close(Region region)
    {
      Util.Log("TallyLoader::Close");
    }

    #endregion
  }
}
