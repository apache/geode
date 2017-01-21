/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.IO;

namespace Apache.Geode.Client.UnitTests
{
  using NUnit.Framework;
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client;

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
