//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;

  [TestFixture]
  [Category("group2")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ThinClientPoolTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1, m_client2;

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }

    [TestFixtureTearDown]
    public override void EndTests()
    {
      CacheHelper.StopJavaServers();
      base.EndTests();
    }

    [TearDown]
    public override void EndTest()
    {
      try
      {
        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
      finally
      {
        CacheHelper.StopJavaServers();
        CacheHelper.StopJavaLocators();
      }
      base.EndTest();
    }

    bool checkPoolAttributes
    (
      Generic.Pool pool,
      string[] locators,
      string[] servers,
      int freeConnectionTimeout,
      int loadConditioningInterval,
      int minConnections,
      int maxConnections,
      int retryAttempts,
      int idleTimeout,
      int pingInterval,
      string name,
      int readTimeout,
      string serverGroup,
      int socketBufferSize,
      bool subscriptionEnabled,
      int subscriptionMessageTrackingTimeout,
      int subscriptionAckInterval,
      int subscriptionRedundancy,
      int statisticInterval,
      int threadLocalConnections,
      bool prSingleHopEnabled,
      int updateLocatorListInterval
    )
    {
      if (pool == null)
      {
        Util.Log("checkPoolAttributes: pool is null");
        return false;
      }
      Util.Log("checkPoolAttributes: Checking pool " + pool.Name);
      if (!pool.Name.Equals(name))
      {
        Util.Log("checkPoolAttributes: Pool name expected [{0}], actual [{1}]", name, pool.Name);
        return false;
      }
      if (!Util.CompareArrays(locators, pool.Locators))
      {
        Util.Log("checkPoolAttributes: locator list mismatch");
        return false;
      }
      if (servers != null && !Util.CompareArrays(servers, pool.Servers))
      {
        Util.Log("checkPoolAttributes: server list mismatch");
        return false;
      }
      if (freeConnectionTimeout != pool.FreeConnectionTimeout)
      {
        Util.Log("checkPoolAttributes: FreeConnectionTimeout expected {0}, actual {1}",
          freeConnectionTimeout, pool.FreeConnectionTimeout);
        return false;
      }
      if (loadConditioningInterval != pool.LoadConditioningInterval)
      {
        Util.Log("checkPoolAttributes: LoadConditioningInterval expected {0}, actual {1}",
          loadConditioningInterval, pool.LoadConditioningInterval);
        return false;
      }
      if (minConnections != pool.MinConnections)
      {
        Util.Log("checkPoolAttributes: MinConnections expected {0}, actual {1}",
          minConnections, pool.MinConnections);
        return false;
      }
      if (maxConnections != pool.MaxConnections)
      {
        Util.Log("checkPoolAttributes: MaxConnections expected {0}, actual {1}",
          maxConnections, pool.MaxConnections);
        return false;
      }
      if (retryAttempts != pool.RetryAttempts)
      {
        Util.Log("checkPoolAttributes: RetryAttempts expected {0}, actual {1}",
          retryAttempts, pool.RetryAttempts);
        return false;
      }
      if (idleTimeout != pool.IdleTimeout)
      {
        Util.Log("checkPoolAttributes: IdleTimeout expected {0}, actual {1}",
          idleTimeout, pool.IdleTimeout);
        return false;
      }
      if (pingInterval != pool.PingInterval)
      {
        Util.Log("checkPoolAttributes: PingInterval expected {0}, actual {1}",
          pingInterval, pool.PingInterval);
        return false;
      }
      if (readTimeout != pool.ReadTimeout)
      {
        Util.Log("checkPoolAttributes: ReadTimeout expected {0}, actual {1}",
          readTimeout, pool.ReadTimeout);
        return false;
      }
      if (!serverGroup.Equals(pool.ServerGroup))
      {
        Util.Log("checkPoolAttributes: ServerGroup expected {0}, actual {1}",
          serverGroup, pool.ServerGroup);
        return false;
      }
      if (socketBufferSize != pool.SocketBufferSize)
      {
        Util.Log("checkPoolAttributes: SocketBufferSize expected {0}, actual {1}",
          socketBufferSize, pool.SocketBufferSize);
        return false;
      }
      if (subscriptionEnabled != pool.SubscriptionEnabled)
      {
        Util.Log("checkPoolAttributes: SubscriptionEnabled expected {0}, actual {1}",
          subscriptionEnabled, pool.SubscriptionEnabled);
        return false;
      }
      if (subscriptionMessageTrackingTimeout != pool.SubscriptionMessageTrackingTimeout)
      {
        Util.Log("checkPoolAttributes: SubscriptionMessageTrackingTimeout expected {0}, actual {1}",
          subscriptionMessageTrackingTimeout, pool.SubscriptionMessageTrackingTimeout);
        return false;
      }
      if (subscriptionAckInterval != pool.SubscriptionAckInterval)
      {
        Util.Log("checkPoolAttributes: SubscriptionAckInterval expected {0}, actual {1}",
          subscriptionAckInterval, pool.SubscriptionAckInterval);
        return false;
      }
      if (subscriptionRedundancy != pool.SubscriptionRedundancy)
      {
        Util.Log("checkPoolAttributes: SubscriptionRedundancy expected {0}, actual {1}",
          subscriptionRedundancy, pool.SubscriptionRedundancy);
        return false;
      }
      if (statisticInterval != pool.StatisticInterval)
      {
        Util.Log("checkPoolAttributes: StatisticInterval expected {0}, actual {1}",
          statisticInterval, pool.StatisticInterval);
        return false;
      }
      if (prSingleHopEnabled != pool.PRSingleHopEnabled)
      {
        Util.Log("checkPoolAttributes: PRSingleHopEnabled expected {0}, actual {1}",
          prSingleHopEnabled, pool.PRSingleHopEnabled);
        return false;
      }
      if (updateLocatorListInterval != pool.UpdateLocatorListInterval)
      {
        Util.Log("checkPoolAttributes: updateLocatorListInterval expected {0}, actual {1}",
          updateLocatorListInterval, pool.UpdateLocatorListInterval);
        return false;
      }
      /* TODO: Enable this check when available
      if (threadLocalConnections != pool.ThreadLocalConnections)
      {
        Util.Log("checkPoolAttributes: ThreadLocalConnections expected {0}, actual {1}",
          threadLocalConnections, pool.ThreadLocalConnections);
        return false;
      }
       * */
      Util.Log("checkPoolAttributes: Checked pool: OK");
      return true;
    }

    public void runPoolXmlCreation()
    {
      string xmlLocation = CacheHelper.TestDir + Path.DirectorySeparatorChar + "valid_cache_pool.xml";

      string duplicateXMLFile = CacheHelper.TestDir + Path.DirectorySeparatorChar + Util.Rand(3432898).ToString() + "valid_cache_pool.xml";
      CacheHelper.createDuplicateXMLFile(xmlLocation, duplicateXMLFile);
      xmlLocation = duplicateXMLFile;

      Cache cache = CacheFactory.CreateCacheFactory()
          .Set("cache-xml-file", xmlLocation)
          .Create();

      Region[] rootRegions = cache.RootRegions<object, object>();
      Assert.AreEqual(2, rootRegions.Length);

      ICollection<IRegion<object, object>> subRegionsCol = rootRegions[0].SubRegions(true);
      Region[] subRegions = new Region[subRegionsCol.Count];
      subRegionsCol.CopyTo(subRegions, 0);
      Assert.AreEqual(1, subRegions.Length);

      ICollection<IRegion<object, object>> subRegions2Col = rootRegions[1].SubRegions(true);
      Region[] subRegions2 = new Region[subRegions2Col.Count];
      subRegions2Col.CopyTo(subRegions2, 0);
      Assert.AreEqual(0, subRegions2.Length);

      string poolNameRegion1 = rootRegions[0].Attributes.PoolName;
      string poolNameRegion2 = rootRegions[1].Attributes.PoolName;
      string poolNameSubRegion = subRegions[0].Attributes.PoolName;

      Assert.AreEqual("test_pool_1", poolNameRegion1);
      Assert.AreEqual("test_pool_2", poolNameRegion2);
      Assert.AreEqual("test_pool_2", poolNameSubRegion);

      Pool poolOfRegion1 = PoolManager.Find(poolNameRegion1);
      Pool poolOfRegion2 = PoolManager.Find(poolNameRegion2);
      Pool poolOfSubRegion = PoolManager.Find(poolNameSubRegion);

      string[] locators = new string[1] { "localhost:" + CacheHelper.LOCATOR_PORT_1 };
      string[] servers = new string[2] { "localhost:" + CacheHelper.HOST_PORT_1, "localhost:" + CacheHelper.HOST_PORT_2 };

      // ARB:
      // TODO: check if server list contains the two endpoints (currently using null argument for servers list)
      bool check1 = checkPoolAttributes(poolOfRegion1, locators, null, 12345, 23456, 3, 7, 3, 5555, 12345,
        "test_pool_1", 23456, "ServerGroup1", 32768, true, 900123, 567, 0, 10123, 5, true, 250001);

      bool check2 = checkPoolAttributes(poolOfRegion2, null, servers, 23456, 34567, 2, 8, 5, 6666, 23456,
        "test_pool_2", 34567, "ServerGroup2", 65536, false, 800222, 678, 1, 20345, 3, false, 5000);

      bool check3 = checkPoolAttributes(poolOfSubRegion, null, servers, 23456, 34567, 2, 8, 5, 6666, 23456,
        "test_pool_2", 34567, "ServerGroup2", 65536, false, 800222, 678, 1, 20345, 3, false, 5000);

      Assert.IsTrue(check1, "Attribute check 1 failed");
      Assert.IsTrue(check2, "Attribute check 2 failed");
      Assert.IsTrue(check3, "Attribute check 3 failed");

      cache.Close();
      try
      {
        Util.Log("Testing invalid_cache_pool.xml");
        xmlLocation = CacheHelper.TestDir + Path.DirectorySeparatorChar + "invalid_cache_pool.xml";
        duplicateXMLFile = CacheHelper.TestDir + Path.DirectorySeparatorChar + Util.Rand(3432898).ToString() + "invalid_cache_pool.xml";
        CacheHelper.createDuplicateXMLFile(xmlLocation, duplicateXMLFile);
        xmlLocation = duplicateXMLFile;
        cache = CacheFactory.CreateCacheFactory()
            .Set("cache-xml-file", xmlLocation)
            .Create();
        Assert.Fail("invalid_cache_pool.xml did not throw exception");
      }
      catch (GemFireException excp)
      {
        Util.Log("Expected {0}: {1}", excp.GetType().Name, excp.Message);
      }

      try
      {
        Util.Log("Testing invalid_cache_pool2.xml");
        xmlLocation = CacheHelper.TestDir + Path.DirectorySeparatorChar + "invalid_cache_pool2.xml";
        duplicateXMLFile = CacheHelper.TestDir + Path.DirectorySeparatorChar + Util.Rand(3432898).ToString() + "invalid_cache_pool2.xml";
        CacheHelper.createDuplicateXMLFile(xmlLocation, duplicateXMLFile);
        xmlLocation = duplicateXMLFile;
        cache = CacheFactory.CreateCacheFactory()
            .Set("cache-xml-file", xmlLocation)
            .Create();
        Assert.Fail("invalid_cache_pool2.xml did not throw exception");
      }
      catch (GemFireException excp)
      {
        Util.Log("Expected {0}: {1}", excp.GetType().Name, excp.Message);
      }

      try
      {
        Util.Log("Testing invalid_cache_pool3.xml");
        xmlLocation = CacheHelper.TestDir + Path.DirectorySeparatorChar + "invalid_cache_pool3.xml";
        duplicateXMLFile = CacheHelper.TestDir + Path.DirectorySeparatorChar + Util.Rand(3432898).ToString() + "invalid_cache_pool3.xml";
        CacheHelper.createDuplicateXMLFile(xmlLocation, duplicateXMLFile);
        xmlLocation = duplicateXMLFile;
        cache = CacheFactory.CreateCacheFactory()
            .Set("cache-xml-file", xmlLocation)
            .Create();
        Assert.Fail("invalid_cache_pool3.xml did not throw exception");
      }
      catch (GemFireException excp)
      {
        Util.Log("Expected {0}: {1}", excp.GetType().Name, excp.Message);
      }

      try
      {
        Util.Log("Testing invalid_cache_pool4.xml");
        xmlLocation = CacheHelper.TestDir + Path.DirectorySeparatorChar + "invalid_cache_pool4.xml";
        duplicateXMLFile = CacheHelper.TestDir + Path.DirectorySeparatorChar + Util.Rand(3432898).ToString() + "invalid_cache_pool4.xml";
        CacheHelper.createDuplicateXMLFile(xmlLocation, duplicateXMLFile);
        xmlLocation = duplicateXMLFile;
        cache = CacheFactory.CreateCacheFactory()
            .Set("cache-xml-file", xmlLocation)
            .Create();
        Assert.Fail("invalid_cache_pool4.xml did not throw exception");
      }
      catch (GemFireException excp)
      {
        Util.Log("Expected {0}: {1}", excp.GetType().Name, excp.Message);
      }

    }

    public void createPoolAndTestAttrs(string poolName)
    {
      CacheHelper.Init();

      PoolFactory factory = PoolManager.CreateFactory();
      factory.SetFreeConnectionTimeout(10000);
      factory.SetLoadConditioningInterval(1);
      factory.SetSocketBufferSize(1024);
      factory.SetReadTimeout(10);
      factory.SetMinConnections(2);
      factory.SetMaxConnections(5);
      factory.SetIdleTimeout(5);
      factory.SetRetryAttempts(5);
      factory.SetPingInterval(1);
      factory.SetUpdateLocatorListInterval(122000);
      factory.SetStatisticInterval(1);
      factory.SetServerGroup("ServerGroup1");
      factory.SetSubscriptionEnabled(true);
      factory.SetSubscriptionRedundancy(1);
      factory.SetSubscriptionMessageTrackingTimeout(5);
      factory.SetSubscriptionAckInterval(1);
      factory.AddLocator("localhost", CacheHelper.LOCATOR_PORT_1);
      factory.SetPRSingleHopEnabled(false);

      Pool pool = factory.Create(poolName);

      Assert.AreEqual(10000, pool.FreeConnectionTimeout, "FreeConnectionTimeout");
      Assert.AreEqual(1, pool.LoadConditioningInterval, "LoadConditioningInterval");
      Assert.AreEqual(1024, pool.SocketBufferSize, "SocketBufferSize");
      Assert.AreEqual(10, pool.ReadTimeout, "ReadTimeout");
      Assert.AreEqual(2, pool.MinConnections, "MinConnections");
      Assert.AreEqual(5, pool.MaxConnections, "MaxConnections");
      Assert.AreEqual(5, pool.IdleTimeout, "IdleTimeout");
      Assert.AreEqual(5, pool.RetryAttempts, "RetryAttempts");
      Assert.AreEqual(1, pool.PingInterval, "PingInterval");
      Assert.AreEqual(122000, pool.UpdateLocatorListInterval, "UpdateLocatorListInterval");
      Assert.AreEqual(1, pool.StatisticInterval, "StatisticInterval");
      Assert.AreEqual("ServerGroup1", pool.ServerGroup, "ServerGroup");
      Assert.AreEqual(true, pool.SubscriptionEnabled, "SubscriptionEnabled");
      Assert.AreEqual(1, pool.SubscriptionRedundancy, "SubscriptionRedundancy");
      Assert.AreEqual(5, pool.SubscriptionMessageTrackingTimeout, "SubscriptionMessageTrackingTimeout");
      Assert.AreEqual(1, pool.SubscriptionAckInterval, "SubscriptionAckInterval");
      Assert.AreEqual(false, pool.PRSingleHopEnabled, "PRSingleHopEnabled");
    }

    public void testPoolAttrs(string poolName)
    {
      string xmlFile = CacheHelper.TestDir;
      xmlFile += "/cacheserver_pool_client.xml";
      string duplicateXMLFile = CacheHelper.TestDir + "/" + Util.Rand(3432898).ToString() + "cacheserver_pool_client.xml";
      CacheHelper.createDuplicateXMLFile(xmlFile, duplicateXMLFile);
      xmlFile = duplicateXMLFile;
      Properties<string, string> config = Properties<string, string>.Create<string, string>();
      config.Insert("cache-xml-file", xmlFile);
      CacheHelper.InitConfig(config);

      Pool pool = PoolManager.Find(poolName);

      Assert.AreEqual("clientPool", pool.Name, "Pool Name");
      Assert.AreEqual(10000, pool.FreeConnectionTimeout, "FreeConnectionTimeout");
      Assert.AreEqual(1, pool.LoadConditioningInterval, "LoadConditioningInterval");
      Assert.AreEqual(1024, pool.SocketBufferSize, "SocketBufferSize");
      Assert.AreEqual(10, pool.ReadTimeout, "ReadTimeout");
      Assert.AreEqual(2, pool.MinConnections, "MinConnections");
      Assert.AreEqual(5, pool.MaxConnections, "MaxConnections");
      Assert.AreEqual(5, pool.IdleTimeout, "IdleTimeout");
      Assert.AreEqual(5, pool.RetryAttempts, "RetryAttempts");
      Assert.AreEqual(1, pool.PingInterval, "PingInterval");
      Assert.AreEqual(25000, pool.UpdateLocatorListInterval, "UpdateLocatorListInterval");
      Assert.AreEqual(1, pool.StatisticInterval, "StatisticInterval");
      Assert.AreEqual("ServerGroup1", pool.ServerGroup, "ServerGroup");
      Assert.AreEqual(true, pool.SubscriptionEnabled, "SubscriptionEnabled");
      Assert.AreEqual(1, pool.SubscriptionRedundancy, "SubscriptionRedundancy");
      Assert.AreEqual(5, pool.SubscriptionMessageTrackingTimeout, "SubscriptionMessageTrackingTimeout");
      Assert.AreEqual(1, pool.SubscriptionAckInterval, "SubscriptionAckInterval");
      Assert.AreEqual(false, pool.PRSingleHopEnabled, "PRSingleHopEnabled");
    }

    public void testExistingPool(string poolName)
    {
      PoolFactory factory = PoolManager.CreateFactory();
      try
      {
        factory.Create(poolName);
        Assert.Fail("Did not get expected IllegalStateException");
      }
      catch (IllegalStateException /*excp*/)
      {
        Util.Log("Got expected IllegalStateException");
      }
      catch (Exception excp)
      {
        Assert.Fail("Got unexpected {0}: {1}", excp.GetType().Name, excp.Message);
      }
    }

    public void createRegionAndAttachPool(string regionName, string poolName)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(regionName, true, true, null, null, poolName, false);
    }

    public void testPoolLevels()
    {
      // Currently we do not have a way to get CacheImpl test helpers in C#
      Util.Log("testPoolLevels: Currently we do not have a way to get CacheImpl test helpers in C#");

      // Step 1: Check pool level == min

      // Step 2: Spawn multiple threads doing ops

      // Step 3: Check pool level == max since threads are active.

      // Step 4: Wait for pool.IdleTimeout to pass.

      // Step 5: Check pool level again == min.

      return;
    }

    void runPoolAttributes()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver1_pool.xml", "cacheserver2_pool.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);

      m_client1.Call(CacheHelper.setPorts, CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2, CacheHelper.HOST_PORT_3, CacheHelper.LOCATOR_PORT_1, CacheHelper.LOCATOR_PORT_2);
      m_client1.Call(createPoolAndTestAttrs, "__TEST_POOL1__");
      m_client1.Call(testExistingPool, "__TEST_POOL1__");
      m_client1.Call(createRegionAndAttachPool, "PoolRegion1", "__TEST_POOL1__");

      m_client2.Call(CacheHelper.setPorts, CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2, CacheHelper.HOST_PORT_3, CacheHelper.LOCATOR_PORT_1, CacheHelper.LOCATOR_PORT_2);
      m_client2.Call(testPoolAttrs, "clientPool");

      m_client1.Call(testPoolLevels);

      m_client1.Call(Close);
      Util.Log("Client 1 closed");
      m_client2.Call(Close);
      Util.Log("Client 2 closed");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
      CacheHelper.StopJavaServer(2);
      Util.Log("Cacheserver 2 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator 1 stopped.");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    public void createPooledRegion(string regionName, string poolName, string locators)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(regionName, true, true, null, locators, poolName, false);
    }

    public void createPooledRegionWithNotification(string regionName, string poolName, string locators)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(regionName, true, true, null, locators, poolName, true);
    }

    public void createPool(string name, string locators, string serverGroup,
      int redundancy, bool subscription)
    {
      CacheHelper.CreatePool<object, object>(name, locators, serverGroup, redundancy, subscription);
    }

    public void checkLocators1()
    {
      try
      {
        InvalidateEntry(RegionName, m_keys[1]);
        UpdateEntry(RegionName, m_keys[0], m_vals[0], false);
        Assert.Fail("Did not get expected NotConnectedException");
      }
      catch (NotConnectedException /*excp*/)
      {
        Util.Log("Got expected NotConnectedException");
      }
    }

    public void checkLocators2()
    {
      try
      {
        InvalidateEntry(RegionName, m_keys[0]);
        UpdateEntry(RegionName, m_keys[1], m_vals[1], false);
        Assert.Fail("Did not get expected NotConnectedException with cause NoAvailableLocatorsException");
      }
      catch (NotConnectedException excp)
      {
        if (excp.InnerException is NoAvailableLocatorsException)
        {
          Util.Log("Got expected NoAvailableLocatorsException");
        }
        else
        {
          Assert.Fail("Did not get expected NotConnectedException with cause NoAvailableLocatorsException");
        }
      }
    }

    void runPoolLocator()
    {
      CacheHelper.createRandomPorts();
      string locators = "localhost:" + (CacheHelper.LOCATOR_PORT_1);
      locators = locators + ",localhost:" + (CacheHelper.LOCATOR_PORT_2);
      m_client1.Call(createPooledRegion, RegionName, "Pool1", locators.Split(',')[0]);
      m_client2.Call(createPooledRegion, RegionName, "Pool2", locators.Split(',')[0]);

      m_client1.Call(createPooledRegionWithNotification, RegionNames[1], "Pool3", locators.Split(',')[0]);
      m_client2.Call(createPooledRegionWithNotification, RegionNames[1], "Pool3", locators.Split(',')[0]);

      m_client1.Call(CreateEntryWithLocatorException, RegionName, m_keys[0], m_vals[0]);
      m_client1.Call(CreateEntryWithLocatorException, RegionNames[1], m_keys[0], m_vals[0]);

      CacheHelper.SetupJavaServers(true, "cacheserver.xml", "cacheserver2.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC1");
      CacheHelper.StartJavaLocator(2, "GFELOC2");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);

      m_client1.Call(CreateEntry, RegionName, m_keys[0], m_vals[0]);
      m_client2.Call(DoNetsearch, RegionName, m_keys[0], m_vals[0], true);
      m_client2.Call(CreateEntry, RegionName, m_keys[1], m_vals[1]);

      m_client1.Call(CreateEntry, RegionNames[1], m_keys[0], m_vals[0]);

      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 2);
      CacheHelper.StopJavaServer(1);

      m_client2.Call(DoNetsearch, RegionNames[1], m_keys[0], m_vals[0], false);

      m_client1.Call(DoNetsearch, RegionName, m_keys[1], m_vals[1], true);
      m_client1.Call(UpdateEntry, RegionName, m_keys[0], m_nvals[0], true);

      m_client2.Call(InvalidateEntry, RegionName, m_keys[0]);
      m_client2.Call(DoNetsearch, RegionName, m_keys[0], m_nvals[0], false);
      m_client2.Call(UpdateEntry, RegionName, m_keys[1], m_nvals[1], true);

      CacheHelper.StopJavaLocator(1);
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      CacheHelper.StopJavaServer(2);

      m_client1.Call(checkLocators1);

      CacheHelper.StopJavaLocator(2);
      m_client2.Call(checkLocators2);

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    public void checkCacheServerException(string region, string key, string val)
    {
      try
      {
        CreateEntry(region, key, val);
        Assert.Fail("Did not get expected RegionNotFoundException");
      }
      catch (CacheServerException excp)
      {
        Util.Log("Got expected {0}: {1}", excp.GetType().Name, excp.Message);
      }
    }

    void runPoolServer()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver1_pool.xml", "cacheserver2_pool.xml", "cacheserver3_pool.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC1");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);

      m_client1.Call(CacheHelper.setPorts, CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2, CacheHelper.HOST_PORT_3, CacheHelper.LOCATOR_PORT_1, CacheHelper.LOCATOR_PORT_2);
      m_client1.Call(createPool, "__TEST_POOL1__", CacheHelper.Locators, "ServerGroup1", 0, false);
      m_client1.Call(createRegionAndAttachPool, "PoolRegion1", "__TEST_POOL1__");
      m_client1.Call(createRegionAndAttachPool, "PoolRegion2", "__TEST_POOL1__");
      m_client1.Call(createRegionAndAttachPool, "PoolRegion3", "__TEST_POOL1__");

      m_client1.Call(CacheHelper.setPorts, CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2, CacheHelper.HOST_PORT_3, CacheHelper.LOCATOR_PORT_1, CacheHelper.LOCATOR_PORT_2);
      m_client2.Call(createPool, "__TEST_POOL1__", CacheHelper.Locators, (string)null, 0, false);
      m_client2.Call(createRegionAndAttachPool, "PoolRegion1", "__TEST_POOL1__");
      m_client2.Call(createRegionAndAttachPool, "PoolRegion2", "__TEST_POOL1__");
      m_client2.Call(createRegionAndAttachPool, "PoolRegion3", "__TEST_POOL1__");

      m_client1.Call(CreateEntry, "PoolRegion1", m_keys[0], m_vals[0]);

      m_client1.Call(checkCacheServerException, "PoolRegion2", m_keys[0], m_vals[0]);

      m_client2.Call(CreateEntry, "PoolRegion1", m_keys[0], m_vals[0]);
      m_client2.Call(CreateEntry, "PoolRegion2", m_keys[0], m_vals[0]);

      m_client2.Call(checkCacheServerException, "PoolRegion3", m_keys[0], m_vals[0]);

      m_client1.Call(Close);
      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      CacheHelper.StopJavaServer(2);
      CacheHelper.StopJavaServer(3);

      CacheHelper.StopJavaLocator(1);
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    public void feedEntries(int keyIndex, bool newValue, bool update)
    {
      if (!update)
      {
        CreateEntry("PoolRegion1", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex]);
        CreateEntry("PoolRegion2", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex]);
        CreateEntry("PoolRegion3", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex]);
      }
      else
      {
        UpdateEntry("PoolRegion1", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex], false);
        UpdateEntry("PoolRegion2", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex], false);
        UpdateEntry("PoolRegion3", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex], false);
      }
    }

    public void verifyEntries(int keyIndex, bool netSearch, bool newValue)
    {
      if (!netSearch)
      {
        VerifyEntry("PoolRegion1", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex]);
        VerifyEntry("PoolRegion2", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex]);
        VerifyEntry("PoolRegion3", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex]);
      }
      else
      {
        DoNetsearch("PoolRegion1", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex], false);
        DoNetsearch("PoolRegion2", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex], false);
        DoNetsearch("PoolRegion3", m_keys[keyIndex], newValue ? m_nvals[keyIndex] : m_vals[keyIndex], false);
      }
    }

    public void verifyEntries2()
    {
      Region region = CacheHelper.GetVerifyRegion<object, object>("PoolRegion3");
      string cKey = m_keys[1];

      string cVal = (string)region[cKey];

      Assert.IsNotNull(cVal, "Value should not be null.");
      if (cVal != m_nvals[1])
        return;//ServerGroup2 is no more up

      Assert.Fail("Looks like value from ServerGroup2 found, even no server is up");
    }

    public void registerAllKeys()
    {
      Region region1 = CacheHelper.GetVerifyRegion<object, object>("PoolRegion1");
      region1.GetSubscriptionService().RegisterAllKeys(false, null, true);
      Region region2 = CacheHelper.GetVerifyRegion<object, object>("PoolRegion2");
      region2.GetSubscriptionService().RegisterAllKeys(false, null, true);
      Region region3 = CacheHelper.GetVerifyRegion<object, object>("PoolRegion3");
      region3.GetSubscriptionService().RegisterAllKeys(false, null, true);
    }

    void runPoolRedundancy()
    {
      for (int runIndex = 0; runIndex < 2; runIndex++)
      {
        CacheHelper.SetupJavaServers(true, "CacheServPoolRedun1.xml", "CacheServPoolRedun2.xml",
          "CacheServPoolRedun3.xml");
        CacheHelper.StartJavaLocator(1, "GFELOC1");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);
        m_client1.Call(CacheHelper.setPorts, CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2, CacheHelper.HOST_PORT_3, CacheHelper.LOCATOR_PORT_1, CacheHelper.LOCATOR_PORT_2);
        m_client2.Call(CacheHelper.setPorts, CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2, CacheHelper.HOST_PORT_3, CacheHelper.LOCATOR_PORT_1, CacheHelper.LOCATOR_PORT_2);

        if (runIndex == 0)
        {
          m_client1.Call(createPool, "Pool1", CacheHelper.Locators, "ServerGroup1", 2, true);
          m_client1.Call(createRegionAndAttachPool, "PoolRegion1", "Pool1");
          m_client1.Call(createPool, "Pool2", CacheHelper.Locators, "ServerGroup1", 1, true);
          m_client1.Call(createRegionAndAttachPool, "PoolRegion2", "Pool2");
          m_client1.Call(createPool, "Pool3", CacheHelper.Locators, "ServerGroup1", 0, true);
          m_client1.Call(createRegionAndAttachPool, "PoolRegion3", "Pool3");
          m_client1.Call(feedEntries, 0, false, false);
          m_client1.Call(registerAllKeys);

          m_client2.Call(createPool, "Pool1", CacheHelper.Locators, (string)null, 2, true);
          m_client2.Call(createRegionAndAttachPool, "PoolRegion1", "Pool1");
          m_client2.Call(createPool, "Pool2", CacheHelper.Locators, (string)null, 1, true);
          m_client2.Call(createRegionAndAttachPool, "PoolRegion2", "Pool2");
          m_client2.Call(createPool, "Pool3", CacheHelper.Locators, (string)null, 0, true);
          m_client2.Call(createRegionAndAttachPool, "PoolRegion3", "Pool3");
          m_client2.Call(feedEntries, 1, false, false);
          m_client2.Call(registerAllKeys);
          m_client2.Call(verifyEntries, 0, true, false);
        }
        else
        {
          m_client1.Call(createPool, "Pool1", CacheHelper.Locators, "ServerGroup1", 2, true);
          m_client1.Call(createRegionAndAttachPool, "PoolRegion1", "Pool1");
          m_client1.Call(createPool, "Pool2", CacheHelper.Locators, "ServerGroup2", 1, true);
          m_client1.Call(createRegionAndAttachPool, "PoolRegion2", "Pool2");
          m_client1.Call(createPool, "Pool3", CacheHelper.Locators, "ServerGroup3", 0, true);
          m_client1.Call(createRegionAndAttachPool, "PoolRegion3", "Pool3");
          m_client1.Call(feedEntries, 0, false, false);
          m_client1.Call(registerAllKeys);

          m_client2.Call(createPool, "Pool1", CacheHelper.Locators, (string)null, 2, true);
          m_client2.Call(createRegionAndAttachPool, "PoolRegion1", "Pool1");
          m_client2.Call(createPool, "Pool2", CacheHelper.Locators, (string)null, 1, true);
          m_client2.Call(createRegionAndAttachPool, "PoolRegion2", "Pool2");
          m_client2.Call(createPool, "Pool3", CacheHelper.Locators, (string)null, 0, true);
          m_client2.Call(createRegionAndAttachPool, "PoolRegion3", "Pool3");
          m_client2.Call(feedEntries, 1, false, false);
          m_client2.Call(registerAllKeys);
          m_client2.Call(verifyEntries, 0, true, false);
        }

        m_client1.Call(verifyEntries, 1, false, false);

        if (runIndex == 0)
        {
          CacheHelper.StopJavaServer(1);
          CacheHelper.StopJavaServer(2);
        }

        /*CacheHelper.StopJavaServer(3);
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
        CacheHelper.StartJavaServerWithLocators(3, "GFECS3", 1);*/

        Thread.Sleep(20000);
        m_client1.Call(feedEntries, 0, true, true);
        m_client2.Call(verifyEntries, 0, false, true);
        if (runIndex == 1)
        {
          CacheHelper.StopJavaServer(1);
        }
        Thread.Sleep(20000);
        m_client2.Call(feedEntries, 1, true, true);
        if (runIndex == 0)
          m_client1.Call(verifyEntries, 1, false, true);
        else
          m_client1.Call(verifyEntries2);

        m_client1.Call(Close);
        m_client2.Call(Close);

        //CacheHelper.StopJavaServer(1);
        if (runIndex == 1)
          CacheHelper.StopJavaServer(2);
        CacheHelper.StopJavaServer(3);

        CacheHelper.StopJavaLocator(1);

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runPoolRegisterInterest()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver1_pool.xml", "cacheserver3_pool.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC1");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
      m_client1.Call(CacheHelper.setPorts, CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2, CacheHelper.HOST_PORT_3, CacheHelper.LOCATOR_PORT_1, CacheHelper.LOCATOR_PORT_2);
      m_client1.Call(createPool, "__TEST_POOL1__", CacheHelper.Locators, "ServerGroup1", 0, true);
      m_client1.Call(createRegionAndAttachPool, "PoolRegion1", "__TEST_POOL1__");
      m_client1.Call(RegisterAllKeys, new string[] { "PoolRegion1" });

      Util.Log("TODO: Code to check client notification connection with Server1 and not Server2");

      m_client1.Call(Close);

      CacheHelper.StopJavaServer(1);
      CacheHelper.StopJavaServer(2);

      CacheHelper.StopJavaLocator(1);
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    #region Tests

    [Test]
    public void PoolXmlCreation()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver1_pool.xml", "cacheserver2_pool.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      // ARB:
      // TODO: in validation, check if server list contains the two endpoints (currently using null argument for servers list)
      // Following lines should be uncommented for checking the above.
      //CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      //CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);

      m_client1.Call(CacheHelper.setPorts, CacheHelper.HOST_PORT_1, CacheHelper.HOST_PORT_2, CacheHelper.HOST_PORT_3, CacheHelper.LOCATOR_PORT_1, CacheHelper.LOCATOR_PORT_2);
      m_client1.Call(runPoolXmlCreation);

      //CacheHelper.StopJavaServer(1);
      //CacheHelper.StopJavaServer(2);
      CacheHelper.StopJavaLocator(1);
    }

    [Test]
    public void PoolAttributes()
    {
      runPoolAttributes();
    }

    [Test]
    public void PoolLocator()
    {
      runPoolLocator();
    }

    [Test]
    public void PoolServer()
    {
      runPoolServer();
    }

    [Test]
    public void PoolRedundancy()
    {
      runPoolRedundancy();
    }

    [Test]
    public void PoolRegisterInterest()
    {
      runPoolRegisterInterest();
    }

    #endregion
  }
}
