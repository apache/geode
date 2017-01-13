//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.IO;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;

  [TestFixture]
  [Category("group3")]
  [Category("unicast_only")]
  [Category("generics")]
  
  public class SystemPropertiesTests : UnitTests
  {
    protected override ClientBase[] GetClients()
    {
      return null;
    }

    [Test]
    public void Default()
    {
      SystemProperties sp = new SystemProperties(null, '.' +
        Path.DirectorySeparatorChar + "non-existent");
      Assert.AreEqual(1, sp.StatisticsSampleInterval);
      Assert.IsTrue(sp.StatisticsEnabled);
      Assert.AreEqual("statArchive.gfs", sp.StatisticsArchiveFile);
      Assert.AreEqual(LogLevel.Config, sp.GFLogLevel);
      Util.Log("Default: ReadTimeoutUnitInMillis = {0} ", sp.ReadTimeoutUnitInMillis);
      Assert.AreEqual(false, sp.ReadTimeoutUnitInMillis);
    }

    [Test]
    public void Config()
    {
      // create a file for alternate properties...
      //StreamWriter sw = new StreamWriter("test.properties");
      //sw.WriteLine("gf.transport.config=./gfconfig");
      //sw.WriteLine("statistics.sample.interval=2000");
      //sw.WriteLine("statistics.enabled=false");
      //sw.WriteLine("statistics.archive.file=./stats.gfs");
      //sw.WriteLine("log.level=error");
      //sw.Close();

      SystemProperties sp = new SystemProperties(null, "test.properties");
      Assert.AreEqual(1, sp.StatisticsSampleInterval);
      Assert.IsTrue(sp.StatisticsEnabled);
      Assert.AreEqual("statArchive.gfs", sp.StatisticsArchiveFile);
      Assert.AreEqual(LogLevel.Config, sp.GFLogLevel);
      Assert.AreEqual(LogLevel.Config, sp.GFLogLevel);
      Assert.IsFalse(sp.OnClientDisconnectClearPdxTypeIds);
    }

    [Test]
    public void NewConfig()
    {
      // When the tests are run from the build script the environment variable
      // TESTSRC is set.
      string filePath = CacheHelper.TestDir + Path.DirectorySeparatorChar + "system.properties";

      SystemProperties sp = new SystemProperties(null, filePath);

      Assert.AreEqual(700, sp.StatisticsSampleInterval);
      Assert.IsFalse(sp.StatisticsEnabled);
      Assert.AreEqual("stats.gfs", sp.StatisticsArchiveFile);
      Assert.AreEqual("gfcpp.log", sp.LogFileName);
      Assert.AreEqual(LogLevel.Debug, sp.GFLogLevel);
      Assert.AreEqual("system", sp.Name);
      Assert.AreEqual("cache.xml", sp.CacheXmlFile);
      Assert.AreEqual(1024000000, sp.LogFileSizeLimit);
      Assert.AreEqual(1024000000, sp.StatsFileSizeLimit);
      Assert.AreEqual(123, sp.PingInterval);
      Assert.AreEqual(345, sp.ConnectTimeout);
      Assert.AreEqual(456, sp.RedundancyMonitorInterval);
      Assert.AreEqual(123, sp.HeapLRULimit);
      Assert.AreEqual(45, sp.HeapLRUDelta);
      Assert.AreEqual(1234, sp.NotifyAckInterval);
      Assert.AreEqual(5678, sp.NotifyDupCheckLife);
      Util.Log("NewConfig: ReadTimeoutUnitInMillis = {0} ", sp.ReadTimeoutUnitInMillis);
      Assert.IsTrue(sp.ReadTimeoutUnitInMillis);
      Assert.IsTrue(sp.OnClientDisconnectClearPdxTypeIds);
    }
  }
}
