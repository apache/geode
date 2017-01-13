//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;


  using AssertionException = GemStone.GemFire.Cache.Generic.AssertionException;
  [TestFixture]
  [Category("group2")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ThinClientDurableTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1, m_client2, m_feeder;
    private string[] m_regexes = { "D-Key-.*", "Key-.*" };
    private string[] m_mixKeys = { "Key-1", "D-Key-1", "L-Key", "LD-Key" };
    private string[] keys = { "Key-1", "Key-2", "Key-3", "Key-4", "Key-5" };

    private static string DurableClientId1 = "DurableClientId1";
    private static string DurableClientId2 = "DurableClientId2";

    private static DurableListener<object, object> m_checker1, m_checker2;

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_feeder = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_feeder };
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
        m_client1.Call(CacheHelper.Close);
        m_client2.Call(CacheHelper.Close);
        m_feeder.Call(CacheHelper.Close);
        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
      finally
      {
        CacheHelper.StopJavaServers();
      }
      base.EndTest();
    }

    #region Common Functions

    public void InitFeeder(string locators, int redundancyLevel)
    {
      CacheHelper.CreatePool<object, object>("__TESTPOOL1_", locators, (string)null, redundancyLevel, false);
      CacheHelper.CreateTCRegion_Pool<object, object>(RegionNames[0], false, true, null,
        locators, "__TESTPOOL1_", false);
    }

    public void InitFeeder2(string locators, int redundancyLevel)
    {
      CacheHelper.CreatePool<object, object>("__TESTPOOL1_", locators, (string)null, redundancyLevel, false);
      CacheHelper.CreateTCRegion_Pool<object, object>(RegionNames[0], false, true, null,
        locators, "__TESTPOOL1_", false);

      CacheHelper.CreatePool<object, object>("__TESTPOOL2_", locators, (string)null, redundancyLevel, false);
      CacheHelper.CreateTCRegion_Pool<object, object>(RegionNames[1], false, true, null,
        locators, "__TESTPOOL2_", false);
    }

    public void InitDurableClientWithTwoPools(string locators,
    int redundancyLevel, string durableClientId, int durableTimeout, int expectedQ0, int expectedQ1)
    {
      DurableListener<object, object> checker = null;
      CacheHelper.InitConfigForDurable_Pool2(locators, redundancyLevel,
          durableClientId, durableTimeout, 35000, "__TESTPOOL1_");
      CacheHelper.CreateTCRegion_Pool(RegionNames[0], false, true, checker,
          CacheHelper.Locators, "__TESTPOOL1_", true);

      CacheHelper.InitConfigForDurable_Pool2(locators, redundancyLevel,
          durableClientId, durableTimeout, 35000, "__TESTPOOL2_");
      CacheHelper.CreateTCRegion_Pool(RegionNames[1], false, true, checker,
          CacheHelper.Locators, "__TESTPOOL2_", true);

      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[1]);

      try
      {
        region0.GetSubscriptionService().RegisterAllKeys(true);
        region1.GetSubscriptionService().RegisterAllKeys(true);
      }
      catch (Exception other)
      {
        Assert.Fail("RegisterAllKeys threw unexpected exception: {0}", other.Message);
      }

      Pool pool0 = PoolManager.Find(region0.Attributes.PoolName);
      int pendingEventCount0 = pool0.PendingEventCount;
      Util.Log("pendingEventCount0 for pool = {0} {1} ", pendingEventCount0, region0.Attributes.PoolName);
      string msg = string.Format("Expected Value ={0}, Actual = {1}", expectedQ0, pendingEventCount0);
      Assert.AreEqual(expectedQ0, pendingEventCount0, msg);

      Pool pool1 = PoolManager.Find(region1.Attributes.PoolName);
      int pendingEventCount1 = pool1.PendingEventCount;
      Util.Log("pendingEventCount1 for pool = {0} {1} ", pendingEventCount1, region1.Attributes.PoolName);
      string msg1 = string.Format("Expected Value ={0}, Actual = {1}", expectedQ1, pendingEventCount1);
      Assert.AreEqual(expectedQ1, pendingEventCount1, msg1);

      CacheHelper.DCache.ReadyForEvents();
      Thread.Sleep(10000);

      CacheHelper.DCache.Close(true);
    }

    public void ClearChecker(int client)
    {
      if (client == 1)
      {
        ThinClientDurableTests.m_checker1 = null;
      }
      else // client == 2
      {
        ThinClientDurableTests.m_checker2 = null;
      }
    }

    public void InitDurableClient(int client, string locators, int redundancyLevel,
      string durableClientId, int durableTimeout)
    {
      // Create DurableListener for first time and use same afterward.
      DurableListener<object, object> checker = null;
      if (client == 1)
      {
        if (ThinClientDurableTests.m_checker1 == null)
        {
          ThinClientDurableTests.m_checker1 = DurableListener<object, object>.Create();
        }
        checker = ThinClientDurableTests.m_checker1;
      }
      else // client == 2 
      {
        if (ThinClientDurableTests.m_checker2 == null)
        {
          ThinClientDurableTests.m_checker2 = DurableListener<object, object>.Create();
        }
        checker = ThinClientDurableTests.m_checker2;
      }
      CacheHelper.InitConfigForDurable_Pool(locators, redundancyLevel,
        durableClientId, durableTimeout);
      CacheHelper.CreateTCRegion_Pool<object, object>(RegionNames[0], false, true, checker,
        CacheHelper.Locators, "__TESTPOOL1_", true);

      CacheHelper.DCache.ReadyForEvents();
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);
      region1.GetSubscriptionService().RegisterRegex(m_regexes[0], true);
      region1.GetSubscriptionService().RegisterRegex(m_regexes[1], false);
      //CacheableKey[] ldkeys = { new CacheableString(m_mixKeys[3]) };
      ICollection<object> lkeys = new List<object>();
      lkeys.Add((object)m_mixKeys[3]);
      region1.GetSubscriptionService().RegisterKeys(lkeys, true, false);

      ICollection<object> ldkeys = new List<object>();;
      ldkeys.Add((object)m_mixKeys[2]);
      region1.GetSubscriptionService().RegisterKeys(ldkeys, false, false);
    }

    public void InitClientXml(string cacheXml)
    {
      CacheHelper.InitConfig(cacheXml);
    }

    public void ReadyForEvents()
    {
      CacheHelper.DCache.ReadyForEvents();
    }

    public void PendingEventCount(IRegion<object, object> region, int expectedPendingQSize, bool exception)
    {
      Util.Log("PendingEventCount regionName = {0} ", region);
      string poolName = region.Attributes.PoolName;
      if (poolName != null)
      {
        Util.Log("PendingEventCount poolName = {0} ", poolName);
        Pool pool = PoolManager.Find(poolName);
        if (exception)
        {
          try
          {
            int pendingEventCount = pool.PendingEventCount;
            Util.Log("PendingEventCount Should have got exception ");
            Assert.Fail("PendingEventCount Should have got exception");
          }
          catch (IllegalStateException ex)
          {
            Util.Log("Got expected exception for PendingEventCount {0} ", ex.Message);
          }
        }
        else
        {
          int pendingEventCount = pool.PendingEventCount;
          Util.Log("pendingEventCount = {0} ", pendingEventCount);
          string msg = string.Format("Expected Value ={0}, Actual = {1}", expectedPendingQSize, pendingEventCount);
          Assert.AreEqual(expectedPendingQSize, pendingEventCount, msg);
        }
      }
    }

    public void FeederUpdate(int value, int sleep)
    {
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);

      region1[m_mixKeys[0]] = value;
      Thread.Sleep(sleep);
      region1[m_mixKeys[1]] = value;
      Thread.Sleep(sleep);
      region1[m_mixKeys[2]] = value;
      Thread.Sleep(sleep);
      region1[m_mixKeys[3]] = value;
      Thread.Sleep(sleep);

      region1.Remove(m_mixKeys[0]);
      Thread.Sleep(sleep);
      region1.Remove(m_mixKeys[1]);
      Thread.Sleep(sleep);
      region1.Remove(m_mixKeys[2]);
      Thread.Sleep(sleep);
      region1.Remove(m_mixKeys[3]);
      Thread.Sleep(sleep);
    }

    public void FeederUpdate2(int pool1, int pool2)
    {
      IRegion<object, object> region0 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[1]);

      for (int i = 0; i < pool1; i++)
      {
        region0[i] = i;
      }

      for (int i = 0; i < pool2; i++)
      {
        region1[i] = i;
      }
    }

    public void ClientDown(bool keepalive)
    {
      if (keepalive)
      {
        CacheHelper.CloseKeepAlive();
      }
      else
      {
        CacheHelper.Close();
      }
    }

    public void CrashClient()
    {
      // TODO:  crash client here.
    }

    public void KillServer()
    {
      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");
    }

    public delegate void KillServerDelegate();

    #endregion


    public void VerifyTotal(int client, int keys, int total)
    {
      DurableListener<object, object> checker = null;
      if (client == 1)
      {
        checker = ThinClientDurableTests.m_checker1;
      }
      else // client == 2
      {
        checker = ThinClientDurableTests.m_checker2;
      }

      if (checker != null)
      {
        checker.validate(keys, total);
      }
      else
      {
        Assert.Fail("Checker is NULL!");
      }
    }

    public void VerifyBasic(int client, int keyCount, int eventCount, int durableValue, int nonDurableValue)
    {//1 4 8 1 1 
      DurableListener<object, object> checker = null;
      if (client == 1)
      {
        checker = ThinClientDurableTests.m_checker1;
      }
      else // client == 2
      {
        checker = ThinClientDurableTests.m_checker2;
      }

      if (checker != null)
      {
        try
        {
          checker.validateBasic(keyCount, eventCount, durableValue, nonDurableValue);//4 8 1 1 
        }
        catch (AssertionException e)
        {
          Util.Log("VERIFICATION FAILED for client {0}: {1} ",client,e);
          throw e;
        }
      }
      else
      {
        Assert.Fail("Checker is NULL!");
      }
    }

    #region Basic Durable Test


    void runDurableAndNonDurableBasic()
    {
      CacheHelper.SetupJavaServers(true,
        "cacheserver_notify_subscription.xml", "cacheserver_notify_subscription2.xml");
       CacheHelper.StartJavaLocator(1, "GFELOC");

      for (int redundancy = 0; redundancy <= 1; redundancy++)
      {
        for (int closeType = 1; closeType <= 2; closeType++)  
        {
          for (int downtime = 0; downtime <= 1; downtime++) // downtime updates
          {
            Util.Log("Starting loop with closeType = {0}, redundancy = {1}, downtime = {2} ",closeType,redundancy, downtime );

            CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
            Util.Log("Cacheserver 1 started.");

            if (redundancy == 1)
            {
              CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
              Util.Log("Cacheserver 2 started.");
            }

            m_feeder.Call(InitFeeder, CacheHelper.Locators, 0);
            Util.Log("Feeder initialized.");

            m_client1.Call(ClearChecker, 1);
            m_client2.Call(ClearChecker, 2);   

            m_client1.Call(InitDurableClient, 1, CacheHelper.Locators, redundancy, DurableClientId1, 300);
            m_client2.Call(InitDurableClient, 2, CacheHelper.Locators, redundancy, DurableClientId2, 3);
            
            Util.Log("Clients initialized.");

            m_feeder.Call(FeederUpdate, 1, 10);
            
            Util.Log("Feeder performed first update.");
            Thread.Sleep(45000); // wait for HA Q to drain and notify ack to go out.

            switch (closeType)
            {
              case 1:
                
                m_client1.Call(ClientDown, true);
                m_client2.Call(ClientDown, true);
                
                Util.Log("Clients downed with keepalive true.");
                break;
              case 2:
                
                m_client1.Call(ClientDown, false); 
                m_client2.Call(ClientDown, false);
                
                Util.Log("Clients downed with keepalive false.");
                break;
              case 3:
                
                m_client1.Call(CrashClient);
                
                m_client2.Call(CrashClient);
                
                Util.Log("Clients downed as crash.");
                break;
              default:
                break;
            }

            if (downtime == 1)
            {
              m_feeder.Call(FeederUpdate, 2, 10);
              
              Util.Log("Feeder performed update during downtime.");
              Thread.Sleep(20000); // wait for HA Q to drain and notify ack to go out.
            }

            m_client1.Call(InitDurableClient, 1, CacheHelper.Locators, redundancy, DurableClientId1, 300);
            
            // Sleep for 45 seconds since durable timeout is 30 seconds so that client2 times out
            Thread.Sleep(45000);

            m_client2.Call(InitDurableClient, 2, CacheHelper.Locators, redundancy, DurableClientId2, 30);
            
            Util.Log("Clients brought back up.");

            if (closeType != 2 && downtime == 1)
            {
              m_client1.Call(VerifyBasic, 1, 4, 12, 2, 1);
              
              m_client2.Call(VerifyBasic, 2, 4, 8, 1, 1);
              
            }
            else
            {
              
              m_client1.Call(VerifyBasic, 1, 4, 8, 1, 1);
              
              m_client2.Call(VerifyBasic, 2, 4, 8, 1, 1);
              
            }

            Util.Log("Verification completed.");

            m_feeder.Call(ClientDown, false);
            
            m_client1.Call(ClientDown, false);
            
            m_client2.Call(ClientDown, false);
            
            Util.Log("Feeder and Clients closed.");

            CacheHelper.StopJavaServer(1);
            Util.Log("Cacheserver 1 stopped.");

            if (redundancy == 1)
            {
              CacheHelper.StopJavaServer(2);
              Util.Log("Cacheserver 2 stopped.");
            }

            Util.Log("Completed loop with closeType = {0}, redundancy = {1}, downtime = {2} ", closeType, redundancy, downtime);

          } // end for int downtime
        } // end for int closeType
      } // end for int redundancy
      CacheHelper.StopJavaLocator(1);
    }

    // Basic Durable Test to check durable event recieving for different combination
    // of Close type ( Keep Alive = true / false ) , Intermediate update and rudundancy
    
    [Test]
    public void DurableAndNonDurableBasic()
    {
      runDurableAndNonDurableBasic();
    } // end [Test] DurableAndNonDurableBasic

    #endregion

    #region Durable Intrest Test

    public void InitDurableClientRemoveInterest(int client, string locators,
      int redundancyLevel, string durableClientId, int durableTimeout)
    {
      // Client Registered Durable Intrest on two keys. We need to unregister them all here.

      DurableListener<object, object> checker = null;
      if (client == 1)
      {
        if (ThinClientDurableTests.m_checker1 == null)
        {
          ThinClientDurableTests.m_checker1 = DurableListener<object, object>.Create();
        }
        checker = ThinClientDurableTests.m_checker1;
      }
      else // client == 2
      {
        if (ThinClientDurableTests.m_checker2 == null)
        {
          ThinClientDurableTests.m_checker2 = DurableListener<object, object>.Create();
        }
        checker = ThinClientDurableTests.m_checker2;
      }
      CacheHelper.InitConfigForDurable_Pool(locators, redundancyLevel,
        durableClientId, durableTimeout);
      CacheHelper.CreateTCRegion_Pool(RegionNames[0], false, true, checker,
        CacheHelper.Locators, "__TESTPOOL1_", true);

      CacheHelper.DCache.ReadyForEvents();
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);

      // Unregister Regex only durable
      region1.GetSubscriptionService().RegisterRegex(m_regexes[0], true);
      region1.GetSubscriptionService().UnregisterRegex(m_regexes[0]);

      // Unregister list only durable
      string[] ldkeys =  new string[]{m_mixKeys[3] };
      region1.GetSubscriptionService().RegisterKeys(ldkeys, true, false);
      region1.GetSubscriptionService().UnregisterKeys(ldkeys);
    }

    public void InitDurableClientNoInterest(int client, string locators,
      int redundancyLevel, string durableClientId, int durableTimeout)
    {
      // we use "client" to either create a DurableListener or use the existing ones
      // if the clients are initialized for the second time
      DurableListener<object, object> checker = null;
      if (client == 1)
      {
        if (ThinClientDurableTests.m_checker1 == null)
        {
          ThinClientDurableTests.m_checker1 = DurableListener<object, object>.Create();
        }
        checker = ThinClientDurableTests.m_checker1;
      }
      else // client == 2
      {
        if (ThinClientDurableTests.m_checker2 == null)
        {
          ThinClientDurableTests.m_checker2 = DurableListener<object, object>.Create();
        }
        checker = ThinClientDurableTests.m_checker2;
      }
      CacheHelper.InitConfigForDurable_Pool(locators, redundancyLevel,
        durableClientId, durableTimeout);
      CacheHelper.CreateTCRegion_Pool(RegionNames[0], false, true, checker,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      CacheHelper.DCache.ReadyForEvents();
    }

    void runDurableInterest()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_feeder.Call(InitFeeder, CacheHelper.Locators, 0);
      Util.Log("Feeder started.");

      m_client1.Call(ClearChecker, 1);
      m_client2.Call(ClearChecker, 2);
      m_client1.Call(InitDurableClient, 1, CacheHelper.Locators,
        0, DurableClientId1, 60);
      m_client2.Call(InitDurableClient, 2, CacheHelper.Locators,
        0, DurableClientId2, 60);
      Util.Log("Clients started.");

      m_feeder.Call(FeederUpdate, 1, 10);
      Util.Log("Feeder performed first update.");

      Thread.Sleep(15000);

      m_client1.Call(ClientDown, true);
      m_client2.Call(ClientDown, true);
      Util.Log("Clients downed with keepalive true.");

      m_client1.Call(InitDurableClientNoInterest, 1, CacheHelper.Locators,
        0, DurableClientId1, 60);
      Util.Log("Client 1 started with no interest.");

      m_client2.Call(InitDurableClientRemoveInterest, 2, CacheHelper.Locators,
        0, DurableClientId2, 60);
      Util.Log("Client 2 started with remove interest.");

      m_feeder.Call(FeederUpdate, 2, 10);
      Util.Log("Feeder performed second update.");

      Thread.Sleep(10000);

      // only durable Intrest will remain.
      m_client1.Call(VerifyBasic, 1, 4, 12, 2, 1);

      // no second update should be recieved.
      m_client2.Call(VerifyBasic, 2, 4, 8, 1, 1);
      Util.Log("Verification completed.");

      m_feeder.Call(ClientDown, false);
      m_client1.Call(ClientDown, false);
      m_client2.Call(ClientDown, false);
      Util.Log("Feeder and Clients closed.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    //This is to test whether durable registered intrests remains on reconnect. and 
    // Unregister works on reconnect.

    [Test]
    public void DurableInterest()
    {
      runDurableInterest();
    } // end [Test] DurableInterest
    #endregion

    #region Durable Failover Test


    public void InitDurableClientForFailover(int client, string locators,
      int redundancyLevel, string durableClientId, int durableTimeout)
    {
      // we use "client" to either create a DurableListener or use the existing ones
      // if the clients are initialized for the second time
      DurableListener<object, object> checker = null;
      if (client == 1)
      {
        if (ThinClientDurableTests.m_checker1 == null)
        {
          ThinClientDurableTests.m_checker1 = DurableListener<object, object>.Create();
        }
        checker = ThinClientDurableTests.m_checker1;
      }
      else // client == 2
      {
        if (ThinClientDurableTests.m_checker2 == null)
        {
          ThinClientDurableTests.m_checker2 = DurableListener<object, object>.Create();
        }
        checker = ThinClientDurableTests.m_checker2;
      }
      CacheHelper.InitConfigForDurable_Pool(locators, redundancyLevel,
        durableClientId, durableTimeout, 35000);
      CacheHelper.CreateTCRegion_Pool(RegionNames[0], false, true, checker,
        CacheHelper.Locators, "__TESTPOOL1_", true);
      CacheHelper.DCache.ReadyForEvents();
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(RegionNames[0]);

      try
      {
        region1.GetSubscriptionService().RegisterRegex(m_regexes[0], true);
        region1.GetSubscriptionService().RegisterRegex(m_regexes[1], false);
      }
      catch (Exception other)
      {
        Assert.Fail("RegisterKeys threw unexpected exception: {0}", other.Message);
      }
    }
    
    public void FeederUpdateForFailover(string region, int value, int sleep)
    {
      //update only 2 keys.
      IRegion<object, object> region1 = CacheHelper.GetVerifyRegion<object, object>(region);

      region1[m_mixKeys[0]] = value;
      Thread.Sleep(sleep);
      region1[m_mixKeys[1]] = value;
      Thread.Sleep(sleep);

    }

    void runDurableFailover()
    {
      CacheHelper.SetupJavaServers(true,
        "cacheserver_notify_subscription.xml", "cacheserver_notify_subscription2.xml");

      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");

      for (int clientDown = 0; clientDown <= 1; clientDown++)
      {
        for (int redundancy = 0; redundancy <= 1; redundancy++)
        {
          Util.Log("Starting loop with clientDown = {0}, redundancy = {1}", clientDown, redundancy );

          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
          Util.Log("Cacheserver 1 started.");

          m_feeder.Call(InitFeeder, CacheHelper.Locators, 0);
          Util.Log("Feeder started with redundancy level as 0.");

          m_client1.Call(ClearChecker, 1);
          m_client1.Call(InitDurableClientForFailover, 1, CacheHelper.Locators,
            redundancy, DurableClientId1, 300);
          Util.Log("Client started with redundancy level as {0}.", redundancy);

          m_feeder.Call(FeederUpdateForFailover, RegionNames[0], 1, 10);
          Util.Log("Feeder updates 1 completed.");

          CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1);
          Util.Log("Cacheserver 2 started.");
          
          //Time for redundancy thread to detect.
          Thread.Sleep(35000);

          if (clientDown == 1)
          {
            m_client1.Call(ClientDown, true);
          }

          CacheHelper.StopJavaServer(1);
          Util.Log("Cacheserver 1 stopped.");

          //Time for failover
          Thread.Sleep(5000);

          m_feeder.Call(FeederUpdateForFailover, RegionNames[0], 2, 10);
          Util.Log("Feeder updates 2 completed.");

          //Restart Client
          if (clientDown == 1)
          {
            m_client1.Call(InitDurableClientForFailover, 1,CacheHelper.Locators,
              redundancy, DurableClientId1, 300);
            Util.Log("Client Restarted with redundancy level as {0}.", redundancy);
          }

          //Verify
          if (clientDown == 1 )
          {
            if (redundancy == 0) // Events missed
            {
              m_client1.Call(VerifyBasic, 1, 2, 2, 1, 1);
            }
            else // redundancy == 1 Only Durable Events should be recieved.
            {
              m_client1.Call(VerifyBasic, 1, 2, 3, 2, 1);
            }
          }
          else  // In normal failover all events should be recieved.
          {
            m_client1.Call(VerifyBasic, 1, 2, 4, 2, 2);
          }

          Util.Log("Verification completed.");

          m_feeder.Call(ClientDown, false);
          m_client1.Call(ClientDown, false);
          Util.Log("Feeder and Client closed.");

          CacheHelper.StopJavaServer(2);
          Util.Log("Cacheserver 2 stopped.");

          Util.Log("Completed loop with clientDown = {0}, redundancy = {1}", clientDown, redundancy);
        }// for redundancy
      } // for clientDown
      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void RunDurableClient(int expectedPendingQSize)
    {
      Properties<string, string> pp = Properties<string, string>.Create<string, string>();
      pp.Insert("durable-client-id", "DurableClientId");
      pp.Insert("durable-timeout", "30");

      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory(pp);
      Cache cache = cacheFactory.SetSubscriptionEnabled(true)
                                .SetSubscriptionAckInterval(5000)
                                .SetSubscriptionMessageTrackingTimeout(5000)
                                .Create();
      Util.Log("Created the GemFire Cache Programmatically");

      RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.CACHING_PROXY);
      IRegion<object, object> region = regionFactory.Create<object, object>("DistRegionAck");
      Util.Log("Created the DistRegionAck Region Programmatically");

      QueryService<object, object> qService = cache.GetQueryService<object, object>();
      CqAttributesFactory<object, object> cqFac = new CqAttributesFactory<object, object>();

      ICqListener<object, object> cqLstner = new MyCqListener1<object, object>();
      cqFac.AddCqListener(cqLstner);
      CqAttributes<object, object> cqAttr = cqFac.Create();
      Util.Log("Attached CqListener");
      String query = "select * from /DistRegionAck";
      CqQuery<object, object> qry = qService.NewCq("MyCq", query, cqAttr, true);
      Util.Log("Created new CqQuery");

      qry.Execute();
      Util.Log("Executed new CqQuery");
      Thread.Sleep(10000);

      PendingEventCount(region, expectedPendingQSize, false);

      //Send ready for Event message to Server( only for Durable Clients ).
      //Server will send queued events to client after recieving this.
      cache.ReadyForEvents();

      Util.Log("Sent ReadyForEvents message to server");
      Thread.Sleep(10000);
      // Close the GemFire Cache with keepalive = true.  Server will queue events for
      // durable registered keys and will deliver all events when client will reconnect
      // within timeout period and send "readyForEvents()"

      PendingEventCount(region, 0, true);

      cache.Close(true);

      Util.Log("Closed the GemFire Cache with keepalive as true");
    }

    void runDurableClientWithTwoPools()
    {
      CacheHelper.SetupJavaServers(true, "cacheserver_notify_subscription.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      m_feeder.Call(InitFeeder2, CacheHelper.Locators, 0);
      Util.Log("Feeder started.");
        
      m_client1.Call(InitDurableClientWithTwoPools, CacheHelper.Locators, 0, DurableClientId1, 30, -2, -2);
      Util.Log("DurableClient with Two Pools Initialized");

      m_feeder.Call(FeederUpdate2, 5, 10);
      Util.Log("Feeder performed first update.");
      Thread.Sleep(15000);

      m_client1.Call(InitDurableClientWithTwoPools, CacheHelper.Locators, 0, DurableClientId1, 30, 6, 11); //+1 for marker, so 5+1, 10+1 etc
      Util.Log("DurableClient with Two Pools after first update");

      m_feeder.Call(FeederUpdate2, 10, 5);
      Util.Log("Feeder performed second update.");
      Thread.Sleep(15000);

      m_client1.Call(InitDurableClientWithTwoPools, CacheHelper.Locators, 0, DurableClientId1, 30, 16, 16);
      Util.Log("DurableClient with Two Pools after second update");

      Thread.Sleep(45000); //45 > 30 secs.
      m_client1.Call(InitDurableClientWithTwoPools, CacheHelper.Locators, 0, DurableClientId1, 30, -1, -1);
      Util.Log("DurableClient with Two Pools after timeout");

      m_feeder.Call(ClientDown, false);
      Util.Log("Feeder and Clients closed.");

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void RunFeeder()
    {
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
      Util.Log("Feeder connected to the GemFire Distributed System");

      Cache cache = cacheFactory.Create();
      Util.Log("Created the GemFire Cache");

      RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.PROXY);
      Util.Log("Created the RegionFactory");

      // Create the Region Programmatically.
      IRegion<object, object> region = regionFactory.Create<object, object>("DistRegionAck");
      Util.Log("Created the Region Programmatically.");

      PendingEventCount(region, 0, true);

      for ( int i =0; i < 10; i++) 
      {        
        region[ i ] = i;
      }
      Thread.Sleep(10000);
      Util.Log("put on 0-10 keys done.");

      // Close the GemFire Cache
      cache.Close();
      Util.Log("Closed the GemFire Cache");
    }

    void RunFeeder1()
    {
      CacheFactory cacheFactory = CacheFactory.CreateCacheFactory();
      Util.Log("Feeder connected to the GemFire Distributed System");

      Cache cache = cacheFactory.Create();
      Util.Log("Created the GemFire Cache");

      RegionFactory regionFactory = cache.CreateRegionFactory(RegionShortcut.PROXY);
      Util.Log("Created the RegionFactory");

      // Create the Region Programmatically.
      IRegion<object, object> region = regionFactory.Create<object, object>("DistRegionAck");
      Util.Log("Created the Region Programmatically.");

      PendingEventCount(region, 0, true);

      for (int i = 10; i < 20; i++) {
        region[i] = i;
      }
      Thread.Sleep(10000);
      Util.Log("put on 10-20 keys done.");

      // Close the GemFire Cache
      cache.Close();
      Util.Log("Closed the GemFire Cache");
    }

    void VerifyEvents()
    {
      Util.Log("MyCqListener1.m_cntEvents = {0} ", MyCqListener1<object, object>.m_cntEvents);
      Assert.AreEqual(MyCqListener1<object, object>.m_cntEvents, 20, "Incorrect events, expected 20");
    }

    void runCQDurable()
    {
      CacheHelper.SetupJavaServers(false, "serverDurableClient.xml");
      CacheHelper.StartJavaServer(1, "GFECS1");
      m_client1.Call(RunDurableClient, -2); // 1st time no Q, hence check -2 as PendingEventCount.
      m_client2.Call(RunFeeder);
      m_client1.Call(RunDurableClient, 10);
      m_client2.Call(RunFeeder1);
      m_client1.Call(RunDurableClient, 10);
      m_client1.Call(VerifyEvents);
      Thread.Sleep(45 * 1000); // sleep 45 secs > 30 secs, check -1 as PendingEventCount.
      m_client1.Call(RunDurableClient, -1);
      CacheHelper.StopJavaServer(1);
    }

    [Test]
    public void DurableFailover()
    {
      runDurableFailover();
    } // end [Test] DurableFailover

    [Test]
    public void CQDurable()
    {
      runCQDurable();

      runDurableClientWithTwoPools();
    }
    #endregion
  }
}
