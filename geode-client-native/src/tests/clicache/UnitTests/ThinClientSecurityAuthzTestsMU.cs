//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;

  public class MyCqListener2 : ICqListener
  {
    private int m_create = 0;
    private int m_update = 0;
    private int m_id = 0;

    public MyCqListener2(int id)
    {
      this.m_id = id;
    }

    public int Creates
    {
      get { return m_create; }
    }

    public int Updates
    {
      get { return m_update; }
    }

    #region ICqListener Members

    void ICqListener.Close()
    {
      Util.Log("CqListener closed  with ID = " + m_id);
    }

    void ICqListener.OnError(CqEvent ev)
    {
      Util.Log("CqListener OnError called ");
    }

    void ICqListener.OnEvent(CqEvent ev)
    {
      Util.Log("CqListener OnEvent ops = " + ev.getBaseOperation());
      if (ev.getBaseOperation() == CqOperationType.OP_TYPE_CREATE)
        m_create++;
      else if (ev.getBaseOperation() == CqOperationType.OP_TYPE_UPDATE)
        m_update++;
    }

    #endregion
  }

  [TestFixture]
  [Category("group4")]
  [Category("unicast_only")]
  [Category("deprecated")]
  public class ThinClientSecurityAuthzTestsMU : ThinClientSecurityAuthzTestBase
  {
    #region Private members
    Region region;
    Region region1;
    private UnitProcess m_client1;
    private UnitProcess m_client2;
    private UnitProcess m_client3;
    private TallyListener m_listener;
    private TallyWriter m_writer;
    private ICacheableKey keys = new CacheableString("Key");
    private CacheableString value = new CacheableString("Value");

      
    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      m_client3 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2, m_client3 };
    }

    public void CreateRegion(string endpoints, string locators, bool caching,
      bool listener, bool writer, bool pool, bool locator)
    {
      if (listener)
      {
        m_listener = new TallyListener();
      }
      else
      {
        m_listener = null;
      }
      Region region = null;
      if (pool)
      {
        if (locator)
        {
          region = CacheHelper.CreateTCRegion_Pool(RegionName, true, caching,
            m_listener, (string)null, locators, "__TESTPOOL1_", true);
        }
        else
        {
          region = CacheHelper.CreateTCRegion_Pool(RegionName, true, caching,
            m_listener, endpoints, (string)null, "__TESTPOOL1_", true);
        }
      }
      else
      {
        region = CacheHelper.CreateTCRegion(RegionName, true, caching,
          m_listener, endpoints, true);
      }
      
      if (writer)
      {
        m_writer = new TallyWriter();
      }
      else
      {
        m_writer = null;
      }
      AttributesMutator at = region.GetAttributesMutator();
      at.SetCacheWriter(m_writer);
    }

    public void DoPut()
    {
      region = CacheHelper.GetRegion(RegionName);
      region.Put(keys.ToString(), value);
    }

    public void CheckAssert()
    {
      Assert.AreEqual(true, m_writer.IsWriterInvoked, "Writer Should be invoked");
      Assert.AreEqual(false, m_listener.IsListenerInvoked, "Listener Should not be invoked");
      Assert.IsFalse(region.ContainsKey(keys.ToString()), "Key should have been found in the region");
    }

    public void DoLocalPut()
    {
      region1 = CacheHelper.GetRegion(RegionName);
      region1.LocalPut(m_keys[2], m_vals[2]);
      Assert.IsTrue(region1.ContainsKey(m_keys[2]), "Key should have been found in the region");
      Assert.AreEqual(true, m_writer.IsWriterInvoked, "Writer Should be invoked");
      Assert.AreEqual(true, m_listener.IsListenerInvoked, "Listener Should be invoked");

      //try Update
      try
      {
        Util.Log("Trying UpdateEntry");
        m_listener.ResetListenerInvokation();
        UpdateEntry(RegionName, m_keys[2], m_nvals[2], false);
        Assert.Fail("Should have got NotAuthorizedException during updateEntry");
      }
      catch (NotAuthorizedException)
      {
        Util.Log("NotAuthorizedException Caught");
        Util.Log("Success");
      }
      catch (Exception other)
      {
        Util.Log("Stack trace: {0} ", other.StackTrace);
        Util.Log("Got  exception : {0}", other.Message);
      }
      Assert.AreEqual(true, m_writer.IsWriterInvoked, "Writer should be invoked");
      Assert.AreEqual(false, m_listener.IsListenerInvoked, "Listener should not be invoked");
      Assert.IsTrue(region1.ContainsKey(m_keys[2]), "Key should have been found in the region");
      VerifyEntry(RegionName, m_keys[2], m_vals[2]);
      m_writer.SetWriterFailed();

      //test CacheWriter
      try
      {
        Util.Log("Testing CacheWriterException");
        UpdateEntry(RegionName, m_keys[2], m_nvals[2], false);
        Assert.Fail("Should have got NotAuthorizedException during updateEntry");
      }
      catch (CacheWriterException)
      {
        Util.Log("CacheWriterException Caught");
        Util.Log("Success");
      }
    
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
        if (m_clients != null)
        {
          foreach (ClientBase client in m_clients)
          {
            client.Call(CacheHelper.Close);
          }
        }
        CacheHelper.Close();
        CacheHelper.ClearEndpoints();
      }
      finally
      {
        CacheHelper.StopJavaServers();
      }
      base.EndTest();
    }

    protected const string RegionName_CQ = "Portfolios";
    static QueryService gQueryService = null;

    static string [] QueryStrings = {
                        "select * from /Portfolios p where p.ID < 1",
                        "select * from /Portfolios p where p.ID < 2",
                        "select * from /Portfolios p where p.ID = 2",
                        "select * from /Portfolios p where p.ID >= 3",//this should pass
                        "select * from /Portfolios p where p.ID = 4",//this should pass
                        "select * from /Portfolios p where p.ID = 5",
                        "select * from /Portfolios p where p.ID = 6",
                        "select * from /Portfolios p where p.ID = 7"
                      };
 
    public void registerCQ(Properties credentials, bool durableCQ)
    {
      Util.Log("registerCQ");

      try
      {
        Serializable.RegisterType(Portfolio.CreateDeserializable);
        Serializable.RegisterType(Position.CreateDeserializable);
        Util.Log("registerCQ portfolio registered");
      }
      catch (IllegalStateException)
      {
        Util.Log("registerCQ portfolio NOT registered");
        // ignore since we run multiple iterations for pool and non pool configs
      }

      gQueryService = CacheHelper.getMultiuserCache(credentials).GetQueryService();

      for (int i = 0; i < QueryStrings.Length; i++)
      {
        CqAttributesFactory cqAttrFact = new CqAttributesFactory();
        cqAttrFact.AddCqListener(new MyCqListener2(i));
        CqQuery cq = gQueryService.NewCq("cq_" + i, QueryStrings[i], cqAttrFact.Create(), durableCQ);
        cq.Execute();
      }
      Util.Log("registerCQ Done.");
    }

    public void doCQPut(Properties credentials)
    {
      Util.Log("doCQPut");

      try
      {
        Serializable.RegisterType(Portfolio.CreateDeserializable);
        Serializable.RegisterType(Position.CreateDeserializable);
        Util.Log("doCQPut portfolio registered");
      }
      catch (IllegalStateException)
      {
        Util.Log("doCQPut portfolio NOT registered");
        // ignore since we run multiple iterations for pool and non pool configs
      }
      //Region region = CacheHelper.GetVerifyRegion(RegionName_CQ, credentials);

      IRegionService userRegionService = CacheHelper.getMultiuserCache(credentials);
      Region[] regions = userRegionService.RootRegions();

      Region region = null;

      Console.Out.WriteLine("Number of regions " + regions.Length);
      for (int i = 0; i < regions.Length; i++)
      {
        if (regions[i].Name.Equals(RegionName_CQ))
        {
          region = regions[i];
          break;
        }
      }

        for (int i = 0; i < QueryStrings.Length; i++)
        {
          string key = "port1-" + i;

          Portfolio p = new Portfolio(i);

          region.Put(key, p);
        }

        IRegionService rgServ = region.RegionService;

        Cache cache = rgServ as Cache;

        Assert.IsNull(cache);

      Thread.Sleep(20000);
      Util.Log("doCQPut Done.");
    }

    public void verifyCQEvents(bool whetherResult, CqOperationType opType )
    {
      Util.Log("verifyCQEvents " + gQueryService);
      Assert.IsNotNull(gQueryService);

      CqQuery cq = gQueryService.GetCq("cq_" + 3);
      ICqListener[] cqL = cq.GetCqAttributes().getCqListeners();
      MyCqListener2 mcqL = (MyCqListener2)cqL[0];

      Util.Log("got result for cq listener3 " + cq.Name + " : " + mcqL.Creates);

      if (opType == CqOperationType.OP_TYPE_CREATE)
      {
        if (whetherResult)
          Assert.AreEqual(1, mcqL.Creates, "CQ listener 3 should get one create event ");
        else
          Assert.AreEqual(0, mcqL.Creates, "CQ listener 3 should not get any create event ");
      }
      else if (opType == CqOperationType.OP_TYPE_UPDATE)
      {
        if (whetherResult)
          Assert.AreEqual(1, mcqL.Updates, "CQ listener 3 should get one update event ");
        else
          Assert.AreEqual(0, mcqL.Updates, "CQ listener 3 should not get any update event ");
      }

      cq = gQueryService.GetCq("cq_" + 4);
      cqL = cq.GetCqAttributes().getCqListeners();
      mcqL = (MyCqListener2)cqL[0];

      Util.Log("got result for cq listener4 " + cq.Name + " : " + mcqL.Creates);

      if (opType == CqOperationType.OP_TYPE_CREATE)
      {
        if (whetherResult)
          Assert.AreEqual(1, mcqL.Creates, "CQ listener 4 should get one create event ");
        else
          Assert.AreEqual(0, mcqL.Creates, "CQ listener 4 should not get any create event ");
      }
      else if (opType == CqOperationType.OP_TYPE_UPDATE)
      {
        if (whetherResult)
          Assert.AreEqual(1, mcqL.Updates, "CQ listener 4 should get one update event ");
        else
          Assert.AreEqual(0, mcqL.Updates, "CQ listener 4 should not get any update event ");
      }

      cq = gQueryService.GetCq("cq_" + 0);
      cqL = cq.GetCqAttributes().getCqListeners();
      mcqL = (MyCqListener2)cqL[0];

      Util.Log("got result for cq listener0 " + cq.Name + " : " + mcqL.Creates);

      Assert.AreEqual(0, mcqL.Creates, "CQ listener 0 should get one create event ");


    //  CacheHelper.getMultiuserCache(null).Close();

      gQueryService = null;
          
    }

    void runCQTest(bool pool, bool locator)
    {
      CacheHelper.SetupJavaServers(pool && locator, "remotequery.xml");

      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
      }
      DummyAuthorization3 da = new DummyAuthorization3();

      string authenticator = da.Authenticator;
      string authInit = da.AuthInit;
      string accessorPP = da.AuthenticatorPP;

      Util.Log("testAllowPutsGets: Using authinit: " + authInit);
      Util.Log("testAllowPutsGets: Using authenticator: " + authenticator);
      Util.Log("testAllowPutsGets: Using accessorPP: " + accessorPP);

        // Start servers with all required properties
      string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
        null, accessorPP, null, null);

        // Start the two servers.
        if (pool && locator)
        {
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
          Util.Log("Cacheserver 1 started.");
          CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
          Util.Log("Cacheserver 2 started.");
        }
        else
        {
          CacheHelper.StartJavaServer(1, "GFECS1", serverArgs);
          Util.Log("Cacheserver 1 started.");
         // CacheHelper.StartJavaServer(2, "GFECS2", serverArgs);
          Util.Log("Cacheserver 2 started.");
        }

        // Start client1 with valid CREATE credentials, this index will be used to authorzie the user
        Properties createCredentials = da.GetValidCredentials(4);

        Util.Log("runCQTest: ");
        m_client1.Call(SecurityTestUtil.CreateClientMU2, RegionName_CQ,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true,true);

        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName_CQ,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true);

        // Perform some put operations from client1
        m_client1.Call(registerCQ, createCredentials, false);
       
        // Verify that the gets succeed
        m_client2.Call(doCQPut, createCredentials);

        m_client1.Call(verifyCQEvents, true, CqOperationType.OP_TYPE_CREATE);

        m_client1.Call(CloseUserCache, false);
        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);
       // CacheHelper.StopJavaServer(2);
      

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    public void CloseUserCache(bool keepAlive)
    {
      Util.Log("CloseUserCache keepAlive: " + keepAlive);
      CacheHelper.CloseUserCache(keepAlive);
    }

    private static string DurableClientId1 = "DurableClientId1";
    //private static string DurableClientId2 = "DurableClientId2";

    void runDurableCQTest(bool pool, bool locator, bool logicalCacheClose, bool durableCQ, bool whetherResult)
    {
      CacheHelper.SetupJavaServers(pool && locator, "remotequery.xml");

      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
      }
      DummyAuthorization3 da = new DummyAuthorization3();

      string authenticator = da.Authenticator;
      string authInit = da.AuthInit;
      string accessorPP = da.AuthenticatorPP;

      Util.Log("testAllowPutsGets: Using authinit: " + authInit);
      Util.Log("testAllowPutsGets: Using authenticator: " + authenticator);
      Util.Log("testAllowPutsGets: Using accessorPP: " + accessorPP);

      // Start servers with all required properties
      string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
        null, accessorPP, null, null);

      // Start the two servers.
      if (pool && locator)
      {
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
        Util.Log("Cacheserver 2 started.");
      }
      else
      {
        CacheHelper.StartJavaServer(1, "GFECS1", serverArgs);
        Util.Log("Cacheserver 1 started.");
        // CacheHelper.StartJavaServer(2, "GFECS2", serverArgs);
        Util.Log("Cacheserver 2 started.");
      }

      // Start client1 with valid CREATE credentials, this index will be used to authorzie the user
      Properties createCredentials = da.GetValidCredentials(4);

      Util.Log("runCQTest: ");
      /*
      regionName, string endpoints, string locators,
      authInit, Properties credentials, bool pool, bool locator, bool isMultiuser, bool notificationEnabled, string durableClientId)
       */
      m_client1.Call(SecurityTestUtil.CreateMUDurableClient, RegionName_CQ,
        CacheHelper.Endpoints, CacheHelper.Locators, authInit, DurableClientId1, pool, locator, true, true );

      m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName_CQ,
        CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true);

      m_client1.Call(ReadyForEvents2);

      // Perform some put operations from client1
      m_client1.Call(registerCQ, createCredentials, durableCQ);


      Properties createCredentials2 = da.GetValidCredentials(3);
      // Verify that the gets succeed
      m_client2.Call(doCQPut, createCredentials2);

      //close cache client-1
      m_client1.Call(verifyCQEvents, true, CqOperationType.OP_TYPE_CREATE);

      Thread.Sleep(10000);

      Util.Log("Before calling CloseUserCache: " + logicalCacheClose);
      if (logicalCacheClose)
        m_client1.Call(CloseUserCache, logicalCacheClose);

      m_client1.Call(CloseKeepAlive);
      //put again from other client
      m_client2.Call(doCQPut, createCredentials2);

      //client-1 will up again
      m_client1.Call(SecurityTestUtil.CreateMUDurableClient, RegionName_CQ,
        CacheHelper.Endpoints, CacheHelper.Locators, authInit, DurableClientId1, pool, locator, true, true);

      // Perform some put operations from client1
      m_client1.Call(registerCQ, createCredentials, durableCQ);

      m_client1.Call(ReadyForEvents2);
      Thread.Sleep(10000);
      m_client1.Call(verifyCQEvents, whetherResult, CqOperationType.OP_TYPE_UPDATE);


      m_client1.Call(Close);

      m_client2.Call(Close);

      CacheHelper.StopJavaServer(1);
      // CacheHelper.StopJavaServer(2);


      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runAllowPutsGets(bool pool, bool locator)
    {
      CacheHelper.SetupJavaServers(pool && locator, CacheXml1, CacheXml2);

      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
      }

      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(true))
      {
        CredentialGenerator cGen = authzGen.GetCredentialGenerator();        
        Properties extraAuthProps = cGen.SystemProperties;
        Properties javaProps = cGen.JavaProperties;
        Properties extraAuthzProps = authzGen.SystemProperties;
        string authenticator = cGen.Authenticator;
        string authInit = cGen.AuthInit;
        string accessor = authzGen.AccessControl;

        Util.Log("testAllowPutsGets: Using authinit: " + authInit);
        Util.Log("testAllowPutsGets: Using authenticator: " + authenticator);
        Util.Log("testAllowPutsGets: Using accessor: " + accessor);

        // Start servers with all required properties
        string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
          accessor, null, SecurityTestUtil.ConcatProperties(extraAuthProps,
          extraAuthzProps), javaProps);

        // Start the two servers.
        if (pool && locator)
        {
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
          Util.Log("Cacheserver 1 started.");
          CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
          Util.Log("Cacheserver 2 started.");
        }
        else
        {
          CacheHelper.StartJavaServer(1, "GFECS1", serverArgs);
          Util.Log("Cacheserver 1 started.");
          CacheHelper.StartJavaServer(2, "GFECS2", serverArgs);
          Util.Log("Cacheserver 2 started.");
        }

        // Start client1 with valid CREATE credentials
        Properties createCredentials = authzGen.GetAllowedCredentials(
          new OperationCode[] { OperationCode.Put },
          new string[] { RegionName }, 1);
        javaProps = cGen.JavaProperties;
        Util.Log("AllowPutsGets: For first client PUT credentials: " +
          createCredentials);
        m_client1.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true);

        // Start client2 with valid GET credentials
        Properties getCredentials = authzGen.GetAllowedCredentials(
          new OperationCode[] { OperationCode.Get },
          new string[] { RegionName }, 2);
        javaProps = cGen.JavaProperties;
        Util.Log("AllowPutsGets: For second client GET credentials: " +
          getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true);

        // Perform some put operations from client1
        m_client1.Call(DoPutsMU, 10 , createCredentials, true);

        // Verify that the gets succeed
        m_client2.Call(DoGetsMU, 10, getCredentials, true);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);
        CacheHelper.StopJavaServer(2);
      }

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runDisallowPutsGets(bool pool, bool locator)
    {
      CacheHelper.SetupJavaServers(pool && locator, CacheXml1, CacheXml2);

      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
      }

      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(true))
      {
        CredentialGenerator cGen = authzGen.GetCredentialGenerator();        
        Properties extraAuthProps = cGen.SystemProperties;
        Properties javaProps = cGen.JavaProperties;
        Properties extraAuthzProps = authzGen.SystemProperties;
        string authenticator = cGen.Authenticator;
        string authInit = cGen.AuthInit;
        string accessor = authzGen.AccessControl;

        Util.Log("DisallowPutsGets: Using authinit: " + authInit);
        Util.Log("DisallowPutsGets: Using authenticator: " + authenticator);
        Util.Log("DisallowPutsGets: Using accessor: " + accessor);

        // Check that we indeed can obtain valid credentials not allowed to do
        // gets
        Properties createCredentials = authzGen.GetAllowedCredentials(
          new OperationCode[] { OperationCode.Put },
          new string[] { RegionName }, 1);
        Properties createJavaProps = cGen.JavaProperties;
        Properties getCredentials = authzGen.GetDisallowedCredentials(
          new OperationCode[] { OperationCode.Get },
          new string[] { RegionName }, 2);
        Properties getJavaProps = cGen.JavaProperties;
        if (getCredentials == null || getCredentials.Size == 0)
        {
          Util.Log("DisallowPutsGets: Unable to obtain valid credentials " +
            "with no GET permission; skipping this combination.");
          continue;
        }

        // Start servers with all required properties
        string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
          accessor, null, SecurityTestUtil.ConcatProperties(extraAuthProps,
          extraAuthzProps), javaProps);

        // Start the two servers.
        if (pool && locator)
        {
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
          Util.Log("Cacheserver 1 started.");
          CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
          Util.Log("Cacheserver 2 started.");
        }
        else
        {
          CacheHelper.StartJavaServer(1, "GFECS1", serverArgs);
          Util.Log("Cacheserver 1 started.");
          CacheHelper.StartJavaServer(2, "GFECS2", serverArgs);
          Util.Log("Cacheserver 2 started.");
        }

        // Start client1 with valid CREATE credentials
        createCredentials = authzGen.GetAllowedCredentials(
            new OperationCode[] { OperationCode.Put },
            new string[] { RegionName }, 1);
        javaProps = cGen.JavaProperties;
        Util.Log("DisallowPutsGets: For first client PUT credentials: " +
          createCredentials);
        m_client1.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true);

        // Start client2 with invalid GET credentials
        getCredentials = authzGen.GetDisallowedCredentials(
            new OperationCode[] { OperationCode.Get },
            new string[] { RegionName }, 2);
        javaProps = cGen.JavaProperties;
        Util.Log("DisallowPutsGets: For second client invalid GET " +
          "credentials: " + getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true);

        // Perform some put operations from client1
        m_client1.Call(DoPutsMU, 10, createCredentials, true);

        // Verify that the gets throw exception
        m_client2.Call(DoGetsMU, 10, getCredentials, true, ExpectedResult.NotAuthorizedException);

        // Try to connect client2 with reader credentials
        getCredentials = authzGen.GetAllowedCredentials(
            new OperationCode[] { OperationCode.Get },
            new string[] { RegionName }, 5);
        javaProps = cGen.JavaProperties;
        Util.Log("DisallowPutsGets: For second client valid GET " +
          "credentials: " + getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true);

        // Verify that the gets succeed
        m_client2.Call(DoGetsMU, 10, getCredentials, true);

        // Verify that the puts throw exception
        m_client2.Call(DoPutsMU, 10, getCredentials, true, ExpectedResult.NotAuthorizedException);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);
        CacheHelper.StopJavaServer(2);
      }

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runInvalidAccessor(bool pool, bool locator)
    {
      CacheHelper.SetupJavaServers(pool && locator, CacheXml1, CacheXml2);
      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
      }
      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(true))
      {
        CredentialGenerator cGen = authzGen.GetCredentialGenerator();        
        Properties extraAuthProps = cGen.SystemProperties;
        Properties javaProps = cGen.JavaProperties;
        Properties extraAuthzProps = authzGen.SystemProperties;
        string authenticator = cGen.Authenticator;
        string authInit = cGen.AuthInit;
        string accessor = authzGen.AccessControl;

        Util.Log("InvalidAccessor: Using authinit: " + authInit);
        Util.Log("InvalidAccessor: Using authenticator: " + authenticator);

        // Start server1 with invalid accessor
        string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
          "com.gemstone.none", null, SecurityTestUtil.ConcatProperties(extraAuthProps,
          extraAuthzProps), javaProps);
        if (pool && locator)
        {
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
        }
        else
        {
          CacheHelper.StartJavaServer(1, "GFECS1", serverArgs);
        }
        Util.Log("Cacheserver 1 started.");

        // Client creation should throw exceptions
        Properties createCredentials = authzGen.GetAllowedCredentials(
            new OperationCode[] { OperationCode.Put },
            new string[] { RegionName }, 3);
        javaProps = cGen.JavaProperties;
        Util.Log("InvalidAccessor: For first client PUT credentials: " +
          createCredentials);
        m_client1.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null,
           pool, locator, true);

        // Now perform some put operations from client1
        m_client1.Call(DoPutsMU, 10, createCredentials, true, ExpectedResult.OtherException);

        Properties getCredentials = authzGen.GetAllowedCredentials(
            new OperationCode[] { OperationCode.Get },
            new string[] { RegionName }, 7);
        javaProps = cGen.JavaProperties;
        Util.Log("InvalidAccessor: For second client GET credentials: " +
          getCredentials);
        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null,
           pool, locator, true);

        // Now perform some put operations from client1
        m_client2.Call(DoGetsMU, 10, getCredentials, true, ExpectedResult.OtherException);

        // Now start server2 that has valid accessor
        Util.Log("InvalidAccessor: Using accessor: " + accessor);
        serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
          accessor, null, SecurityTestUtil.ConcatProperties(extraAuthProps,
          extraAuthzProps), javaProps);
        if (pool && locator)
        {
          CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, serverArgs);
        }
        else
        {
          CacheHelper.StartJavaServer(2, "GFECS2", serverArgs);
        }
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StopJavaServer(1);

        // Client creation should be successful now
        m_client1.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true);
        m_client2.Call(SecurityTestUtil.CreateClientMU, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, (Properties)null, pool, locator, true);

        // Now perform some put operations from client1
        m_client1.Call(DoPutsMU, 10, createCredentials, true);

        // Verify that the gets succeed
        m_client2.Call(DoGetsMU, 10, getCredentials, true);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(2);
      }

      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
      }

      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    void runAllOpsWithFailover(bool pool, bool locator)
    {
      OperationWithAction[] allOps = {
        // Test CREATE and verify with a GET
        new OperationWithAction(OperationCode.Put, 3, OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.Put),
        new OperationWithAction(OperationCode.Get, 3, OpFlags.CheckNoKey
            | OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.CheckNoKey, 4),
        // OPBLOCK_END indicates end of an operation block; the above block of
        // three operations will be first executed on server1 and then on
        // server2 after failover
        
        OperationWithAction.OpBlockEnd,

        // Test GetServerKeys (KEY_SET) operation.
        new OperationWithAction(OperationCode.GetServerKeys),
        new OperationWithAction(OperationCode.GetServerKeys, 3, OpFlags.CheckNotAuthz, 4),

        OperationWithAction.OpBlockEnd,

        // Test UPDATE and verify with a GET
        new OperationWithAction(OperationCode.Put, 3, OpFlags.UseNewVal
            | OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.Put, 1, OpFlags.UseNewVal, 4),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.UseNewVal, 4),

        OperationWithAction.OpBlockEnd,

        // Test DESTROY and verify with a GET and that key should not exist
        new OperationWithAction(OperationCode.Destroy, 3, OpFlags.UseNewVal
            | OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.Destroy),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.CheckFail, 4),
        // Repopulate the region
        new OperationWithAction(OperationCode.Put, 1, OpFlags.UseNewVal, 4),

        OperationWithAction.OpBlockEnd,

        // Check QUERY
        new OperationWithAction(OperationCode.Put),
        new OperationWithAction(OperationCode.Query, 3,
          OpFlags.CheckNotAuthz, 4),
        new OperationWithAction(OperationCode.Query),

        OperationWithAction.OpBlockEnd,

        // UPDATE and test with GET
        new OperationWithAction(OperationCode.Put),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.Put, 1, OpFlags.UseOldConn
            | OpFlags.UseNewVal, 4),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),
         OperationWithAction.OpBlockEnd,

        /// PutAll, GetAll, ExecuteCQ and ExecuteFunction ops
        new OperationWithAction(OperationCode.PutAll),
        // NOTE: GetAll depends on previous PutAll so it should happen right after.
        new OperationWithAction(OperationCode.GetAll),
        //new OperationWithAction(OperationCode.ExecuteCQ),
        new OperationWithAction(OperationCode.ExecuteFunction),

        OperationWithAction.OpBlockEnd,

        // UPDATE and test with GET
        new OperationWithAction(OperationCode.Put, 2),
        new OperationWithAction(OperationCode.Get, 1, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.Put, 2, OpFlags.UseOldConn
            | OpFlags.UseNewVal, 4),
        new OperationWithAction(OperationCode.Get, 1, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        OperationWithAction.OpBlockEnd,

        // UPDATE and test with GET
        new OperationWithAction(OperationCode.Put),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.Put, 1, OpFlags.UseOldConn
            | OpFlags.UseNewVal, 4),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.LocalOp, 4),

        OperationWithAction.OpBlockEnd,

        // Do REGION_DESTROY of the sub-region and check with GET
        new OperationWithAction(OperationCode.Put, 1, OpFlags.UseSubRegion,
            8),
        new OperationWithAction(OperationCode.RegionDestroy, 3,
            OpFlags.UseSubRegion | OpFlags.CheckNotAuthz, 1),
        new OperationWithAction(OperationCode.RegionDestroy, 1,
            OpFlags.UseSubRegion, 1),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseSubRegion
            | OpFlags.CheckNoKey | OpFlags.CheckException, 8),

        // Do REGION_DESTROY of the region and check with GET
        new OperationWithAction(OperationCode.RegionDestroy, 3,
            OpFlags.CheckNotAuthz, 1),
        new OperationWithAction(OperationCode.RegionDestroy, 1, OpFlags.None,
            1),
        new OperationWithAction(OperationCode.Get, 2, OpFlags.UseOldConn
            | OpFlags.CheckNoKey | OpFlags.CheckException, 8),

        // Skip failover for region destroy since it shall fail
        // without restarting the server
        OperationWithAction.OpBlockNoFailover
      };
      RunOpsWithFailover(allOps, "AllOpsWithFailover", pool, locator, true);
    }

    void runThinClientWriterExceptionTest(bool pool, bool locator)
    {
      CacheHelper.SetupJavaServers(pool && locator, CacheXml1);
      if (pool && locator)
      {
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
      }
      foreach (AuthzCredentialGenerator authzGen in GetAllGeneratorCombos(true))
      {
        for (int i = 1; i <= 2; ++i)
        {
          CredentialGenerator cGen = authzGen.GetCredentialGenerator();
          //TODO:hitesh its not working for multiuser mode.. need to fix later
          if (cGen.GetClassCode() == CredentialGenerator.ClassCode.PKCS)
            continue;
          Properties extraAuthProps = cGen.SystemProperties;
          Properties javaProps = cGen.JavaProperties;
          Properties extraAuthzProps = authzGen.SystemProperties;
          string authenticator = cGen.Authenticator;
          string authInit = cGen.AuthInit;
          string accessor = authzGen.AccessControl;

          Util.Log("ThinClientWriterException: Using authinit: " + authInit);
          Util.Log("ThinClientWriterException: Using authenticator: " + authenticator);
          Util.Log("ThinClientWriterException: Using accessor: " + accessor);

          // Start servers with all required properties
          string serverArgs = SecurityTestUtil.GetServerArgs(authenticator,
            accessor, null, SecurityTestUtil.ConcatProperties(extraAuthProps,
            extraAuthzProps), javaProps);

          // Start the server.
          if (pool && locator)
          {
            CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, serverArgs);
          }
          else
          {
            CacheHelper.StartJavaServer(1, "GFECS1", serverArgs);
          }
          Util.Log("Cacheserver 1 started.");

          // Start client1 with valid CREATE credentials
          Properties createCredentials = authzGen.GetDisallowedCredentials(
            new OperationCode[] { OperationCode.Put },
            new string[] { RegionName }, 1);
          javaProps = cGen.JavaProperties;
          Util.Log("DisallowPuts: For first client PUT credentials: " +
            createCredentials);
          m_client1.Call(SecurityTestUtil.CreateClientR0, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, createCredentials, pool, locator);

          Util.Log("Creating region in client1 , no-ack, no-cache, with listener and writer");
          m_client1.Call(CreateRegion, CacheHelper.Endpoints, CacheHelper.Locators,
            true, true, true, pool, locator);
          m_client1.Call(RegisterAllKeys, new string[] { RegionName });

          try
          {
            Util.Log("Trying put Operation");
            m_client1.Call(DoPut);
            Util.Log(" Put Operation Successful");
            Assert.Fail("Should have got NotAuthorizedException during put");
          }
          catch (NotAuthorizedException)
          {
            Util.Log("NotAuthorizedException Caught");
            Util.Log("Success");
          }
          catch (Exception other)
          {
            Util.Log("Stack trace: {0} ", other.StackTrace);
            Util.Log("Got  exception : {0}",
             other.Message);
          }
          m_client1.Call(CheckAssert);
          // Do LocalPut
          m_client1.Call(DoLocalPut);

          m_client1.Call(Close);

          CacheHelper.StopJavaServer(1);
        }
      }
      if (pool && locator)
      {
        CacheHelper.StopJavaLocator(1);
      }
      CacheHelper.ClearEndpoints();
      CacheHelper.ClearLocators();
    }

    #region Tests

    [Test]
	  public void TestCQ()
    { 
      runCQTest(true, false);
    }

    //[Test]
    public void TestDurableCQ()
    {
      //for all run real cache will be true
      //logical cache true/false and durable cq true/false combination....
      //result..whether events should be there or not
      runDurableCQTest(true, false, true, true, true);
      runDurableCQTest(true, false, true, false, false);
      runDurableCQTest(true, false, false, true, true);
      runDurableCQTest(true, false, false, false, false);
    }
	
   [Test]
    public void AllowPutsGets()
    {
      //runAllowPutsGets(false, false); // region config
      runAllowPutsGets(true, false); // pool with server endpoints
      runAllowPutsGets(true, true); // pool with locator
    }

    [Test]
    public void DisallowPutsGets()
    {
      //runDisallowPutsGets(false, false); // region config
      runDisallowPutsGets(true, false); // pool with server endpoints
      runDisallowPutsGets(true, true); // pool with locator
    }

    //[Test]
    public void InvalidAccessor()
    {
      //runInvalidAccessor(false, false); // region config
      runInvalidAccessor(true, false); // pool with server endpoints
      runInvalidAccessor(true, true); // pool with locator
    }

    [Test]
    public void AllOpsWithFailover()
    {
      //runAllOpsWithFailover(false, false); // region config
      runAllOpsWithFailover(true, false); // pool with server endpoints
      runAllOpsWithFailover(true, true); // pool with locator
    }

    [Test]
    public void ThinClientWriterExceptionTest()
    {
      //runThinClientWriterExceptionTest(false, false); // region config
      runThinClientWriterExceptionTest(true, false); // pool with server endpoints
      runThinClientWriterExceptionTest(true, true); // pool with locator
    }
    #endregion
  }
}
