//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  [TestFixture]
  [Category("unicast_only")]
  public class CacheServerMsgs : UnitTests
  {
    Cache m_cache = null;

    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [TestFixtureSetUp]
    public override void InitTests()
    {
      base.InitTests();
      CacheHelper.InitConfig("MessagesTest", "theCache", "SERVER", null,
        null, null);
      m_cache = CacheHelper.DCache;
    }

    [TestFixtureTearDown]
    public override void EndTests()
    {
      try
      {
        CacheHelper.Close();
      }
      finally
      {
        base.EndTests();
      }
    }
  }
}
