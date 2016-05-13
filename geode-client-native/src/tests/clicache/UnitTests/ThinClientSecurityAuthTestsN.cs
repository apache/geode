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
  using GemStone.GemFire.Cache.Tests.NewAPI;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("generics")]
  public class ThinClientSecurityAuthTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1;
    private UnitProcess m_client2;
    private const string CacheXml1 = "cacheserver_notify_subscription.xml";
    private const string CacheXml2 = "cacheserver_notify_subscription2.xml";

    #endregion

    protected override ClientBase[] GetClients()
    {
      m_client1 = new UnitProcess();
      m_client2 = new UnitProcess();
      return new ClientBase[] { m_client1, m_client2 };
    }

    [TearDown]
    public override void EndTest()
    {
      try
      {
        m_client1.Call(CacheHelper.Close);
        m_client2.Call(CacheHelper.Close);
        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
      finally
      {
        CacheHelper.StopJavaServers();
      }
      base.EndTest();
    }

    public void DoTxPuts()
    {
      CacheHelper.CSTXManager.Begin();

      IRegion<object, object> reg = CacheHelper.GetVerifyRegion<object, object>(RegionName);
      reg[10000] = new PdxTests.PdxTypes8();
      reg[100001] = "qqqqqqqqqqqqqq";
      CacheHelper.CSTXManager.Commit();
    }

    public void DoGetsTx()
    {
      Util.Log("In DoGetsTx get after Tx");
      IRegion<object, object> reg = CacheHelper.GetVerifyRegion<object, object>(RegionName);
      Assert.AreEqual(reg[100001], "qqqqqqqqqqqqqq", "Tx get value did not match for reg[100001].");
      object ret1 = reg[10000];
      Assert.IsTrue(ret1 != null && ret1 is PdxTests.PdxTypes8);
      Util.Log("In DoGetsTx get after Tx Done");
    }

    void runValidCredentials(bool pool, bool locator)
    {
      foreach (CredentialGenerator gen in SecurityTestUtil.getAllGenerators(false))
      {
        Properties<string, string> extraProps = gen.SystemProperties;
        Properties<string, string> javaProps = gen.JavaProperties;
        string authenticator = gen.Authenticator;
        string authInit = gen.AuthInit;

        Util.Log("ValidCredentials: Using scheme: " + gen.GetClassCode());
        Util.Log("ValidCredentials: Using authenticator: " + authenticator);
        Util.Log("ValidCredentials: Using authinit: " + authInit);

        // Start the server
        if (pool && locator)
        {
          CacheHelper.SetupJavaServers(true, CacheXml1);
          CacheHelper.StartJavaLocator(1, "GFELOC");
          Util.Log("Locator started");
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        else
        {
          CacheHelper.SetupJavaServers(false, CacheXml1);
          CacheHelper.StartJavaServer(1, "GFECS1", SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        Util.Log("Cacheserver 1 started.");

        // Start the clients with valid credentials
        Properties<string, string> credentials1 = gen.GetValidCredentials(1);
        Properties<string, string> javaProps1 = gen.JavaProperties;
        Util.Log("ValidCredentials: For first client credentials: " +
          credentials1 + " : " + javaProps1);
        Properties<string, string> credentials2 = gen.GetValidCredentials(2);
        Properties<string, string> javaProps2 = gen.JavaProperties;
        Util.Log("ValidCredentials: For second client credentials: " +
          credentials2 + " : " + javaProps2);

        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials1, pool, locator);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials2, pool, locator);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 4);

        m_client1.Call(DoTxPuts);

        // Verify that the puts succeeded
        m_client2.Call(DoGets, 4);

        m_client2.Call(DoGetsTx);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);

        if (pool && locator)
        {
          CacheHelper.StopJavaLocator(1);
        }

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runNoCredentials(bool pool, bool locator)
    {
      foreach (CredentialGenerator gen in SecurityTestUtil.getAllGenerators(false))
      {
        Properties<string, string> extraProps = gen.SystemProperties;
        Properties<string, string> javaProps = gen.JavaProperties;
        string authenticator = gen.Authenticator;
        string authInit = gen.AuthInit;

        Util.Log("NoCredentials: Using scheme: " + gen.GetClassCode());
        Util.Log("NoCredentials: Using authenticator: " + authenticator);
        Util.Log("NoCredentials: Using authinit: " + authInit);

        // Start the server
        if (pool && locator)
        {
          CacheHelper.SetupJavaServers(true, CacheXml1);
          CacheHelper.StartJavaLocator(1, "GFELOC");
          Util.Log("Locator started");
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        else
        {
          CacheHelper.SetupJavaServers(false, CacheXml1);
          CacheHelper.StartJavaServer(1, "GFECS1", SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        Util.Log("Cacheserver 1 started.");

        // Start the clients with valid credentials
        Properties<string, string> credentials1 = gen.GetValidCredentials(3);
        Properties<string, string> javaProps1 = gen.JavaProperties;
        Util.Log("NoCredentials: For first client credentials: " +
          credentials1 + " : " + javaProps1);

        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials1, pool, locator);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 4);

        // Creating region on client2 should throw authentication
        // required exception
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, (string)null, (Properties<string, string>)null,
          ExpectedResult.AuthRequiredException, pool, locator);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);

        if (pool && locator)
        {
          CacheHelper.StopJavaLocator(1);
        }

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runInvalidCredentials(bool pool, bool locator)
    {
      foreach (CredentialGenerator gen in SecurityTestUtil.getAllGenerators(false))
      {
        Properties<string, string> extraProps = gen.SystemProperties;
        Properties<string, string> javaProps = gen.JavaProperties;
        string authenticator = gen.Authenticator;
        string authInit = gen.AuthInit;

        Util.Log("InvalidCredentials: Using scheme: " + gen.GetClassCode());
        Util.Log("InvalidCredentials: Using authenticator: " + authenticator);
        Util.Log("InvalidCredentials: Using authinit: " + authInit);

        // Start the server
        if (pool && locator)
        {
          CacheHelper.SetupJavaServers(true, CacheXml1);
          CacheHelper.StartJavaLocator(1, "GFELOC");
          Util.Log("Locator started");
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        else
        {
          CacheHelper.SetupJavaServers(false, CacheXml1);
          CacheHelper.StartJavaServer(1, "GFECS1", SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        Util.Log("Cacheserver 1 started.");

        // Start the clients with valid credentials
        Properties<string, string> credentials1 = gen.GetValidCredentials(8);
        Properties<string, string> javaProps1 = gen.JavaProperties;
        Util.Log("InvalidCredentials: For first client credentials: " +
          credentials1 + " : " + javaProps1);
        Properties<string, string> credentials2 = gen.GetInvalidCredentials(7);
        Properties<string, string> javaProps2 = gen.JavaProperties;
        Util.Log("InvalidCredentials: For second client credentials: " +
          credentials2 + " : " + javaProps2);

        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials1, pool, locator);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 4);

        // Creating region on client2 should throw authentication
        // failure exception
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials2,
          ExpectedResult.AuthFailedException, pool, locator);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);

        if (pool && locator)
        {
          CacheHelper.StopJavaLocator(1);
        }

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runInvalidAuthInit(bool pool, bool locator)
    {
      foreach (CredentialGenerator gen in SecurityTestUtil.getAllGenerators(false))
      {
        Properties<string, string> extraProps = gen.SystemProperties;
        Properties<string, string> javaProps = gen.JavaProperties;
        string authenticator = gen.Authenticator;

        Util.Log("InvalidAuthInit: Using scheme: " + gen.GetClassCode());
        Util.Log("InvalidAuthInit: Using authenticator: " + authenticator);

        // Start the server
        if (pool && locator)
        {
          CacheHelper.SetupJavaServers(true, CacheXml1);
          CacheHelper.StartJavaLocator(1, "GFELOC");
          Util.Log("Locator started");
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        else
        {
          CacheHelper.SetupJavaServers(false, CacheXml1);
          CacheHelper.StartJavaServer(1, "GFECS1", SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        Util.Log("Cacheserver 1 started.");

        // Creating region on client1 with invalid authInit callback should
        // throw authentication required exception
        Properties<string, string> credentials = gen.GetValidCredentials(5);
        javaProps = gen.JavaProperties;
        Util.Log("InvalidAuthInit: For first client credentials: " +
          credentials + " : " + javaProps);
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, "GemStone.GemFire.Templates.Cache.Security.none",
          credentials, ExpectedResult.AuthRequiredException, pool, locator);

        m_client1.Call(Close);

        CacheHelper.StopJavaServer(1);

        if (pool && locator)
        {
          CacheHelper.StopJavaLocator(1);
        }

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runInvalidAuthenticator(bool pool, bool locator)
    {
      foreach (CredentialGenerator gen in SecurityTestUtil.getAllGenerators(false))
      {
        Properties<string, string> extraProps = gen.SystemProperties;
        Properties<string, string> javaProps = gen.JavaProperties;
        string authInit = gen.AuthInit;

        if (authInit == null || authInit.Length == 0)
        {
          // Skip a scheme which does not have an authInit in the first place
          // (e.g. SSL) since that will fail with AuthReqEx before
          // authenticator is even invoked
          Util.Log("InvalidAuthenticator: Skipping scheme [" +
            gen.GetClassCode() + "] which has no authInit");
          continue;
        }

        Util.Log("InvalidAuthenticator: Using scheme: " + gen.GetClassCode());
        Util.Log("InvalidAuthenticator: Using authinit: " + authInit);

        // Start the server with invalid authenticator
        if (pool && locator)
        {
          CacheHelper.SetupJavaServers(true, CacheXml1);
          CacheHelper.StartJavaLocator(1, "GFELOC");
          Util.Log("Locator started");
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
            "com.gemstone.gemfire.none", extraProps, javaProps));
        }
        else
        {
          CacheHelper.SetupJavaServers(false, CacheXml1);
          CacheHelper.StartJavaServer(1, "GFECS1", SecurityTestUtil.GetServerArgs(
            "com.gemstone.gemfire.none", extraProps, javaProps));
        }
        Util.Log("Cacheserver 1 started.");

        // Starting the client with valid credentials should throw
        // authentication failed exception
        Properties<string, string> credentials1 = gen.GetValidCredentials(1);
        Properties<string, string> javaProps1 = gen.JavaProperties;
        Util.Log("InvalidAuthenticator: For first client valid credentials: " +
          credentials1 + " : " + javaProps1);
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials1,
          ExpectedResult.AuthFailedException, pool, locator);

        // Also try with invalid credentials
        Properties<string, string> credentials2 = gen.GetInvalidCredentials(1);
        Properties<string, string> javaProps2 = gen.JavaProperties;
        Util.Log("InvalidAuthenticator: For first client invalid " +
          "credentials: " + credentials2 + " : " + javaProps2);
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials2,
          ExpectedResult.AuthFailedException, pool, locator);

        m_client1.Call(Close);

        CacheHelper.StopJavaServer(1);

        if (pool && locator)
        {
          CacheHelper.StopJavaLocator(1);
        }

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runNoAuthInitWithCredentials(bool pool, bool locator)
    {
      foreach (CredentialGenerator gen in SecurityTestUtil.getAllGenerators(false))
      {
        Properties<string, string> extraProps = gen.SystemProperties;
        Properties<string, string> javaProps = gen.JavaProperties;
        string authenticator = gen.Authenticator;
        string authInit = gen.AuthInit;

        if (authInit == null || authInit.Length == 0)
        {
          // If the scheme does not have an authInit in the first place
          // (e.g. SSL) then skip it
          Util.Log("NoAuthInitWithCredentials: Skipping scheme [" +
            gen.GetClassCode() + "] which has no authInit");
          continue;
        }

        Util.Log("NoAuthInitWithCredentials: Using scheme: " +
          gen.GetClassCode());
        Util.Log("NoAuthInitWithCredentials: Using authenticator: " +
          authenticator);

        // Start the server
        if (pool && locator)
        {
          CacheHelper.SetupJavaServers(true, CacheXml1);
          CacheHelper.StartJavaLocator(1, "GFELOC");
          Util.Log("Locator started");
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        else
        {
          CacheHelper.SetupJavaServers(false, CacheXml1);
          CacheHelper.StartJavaServer(1, "GFECS1", SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        Util.Log("Cacheserver 1 started.");

        // Start client1 with valid credentials and client2 with invalid
        // credentials; both should fail without an authInit
        Properties<string, string> credentials1 = gen.GetValidCredentials(6);
        Properties<string, string> javaProps1 = gen.JavaProperties;
        Util.Log("NoAuthInitWithCredentials: For first client valid " +
          "credentials: " + credentials1 + " : " + javaProps1);
        Properties<string, string> credentials2 = gen.GetInvalidCredentials(2);
        Properties<string, string> javaProps2 = gen.JavaProperties;
        Util.Log("NoAuthInitWithCredentials: For second client invalid " +
          "credentials: " + credentials2 + " : " + javaProps2);

        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, (string)null, credentials1,
          ExpectedResult.AuthRequiredException, pool, locator);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, (string)null, credentials2,
          ExpectedResult.AuthRequiredException, pool, locator);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);

        if (pool && locator)
        {
          CacheHelper.StopJavaLocator(1);
        }

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runNoAuthenticatorWithCredentials(bool pool, bool locator)
    {
      foreach (CredentialGenerator gen in SecurityTestUtil.getAllGenerators(false))
      {
        Properties<string, string> extraProps = gen.SystemProperties;
        Properties<string, string> javaProps = gen.JavaProperties;
        string authenticator = gen.Authenticator;
        string authInit = gen.AuthInit;

        if (authenticator == null || authenticator.Length == 0)
        {
          // If the scheme does not have an authenticator in the first place
          // (e.g. SSL) then skip it since this test is useless
          Util.Log("NoAuthenticatorWithCredentials: Skipping scheme [" +
            gen.GetClassCode() + "] which has no authenticator");
          continue;
        }

        Util.Log("NoAuthenticatorWithCredentials: Using scheme: " +
          gen.GetClassCode());
        Util.Log("NoAuthenticatorWithCredentials: Using authinit: " +
          authInit);

        // Start the servers
        if (pool && locator)
        {
          CacheHelper.SetupJavaServers(true, CacheXml1);
          CacheHelper.StartJavaLocator(1, "GFELOC");
          Util.Log("Locator started");
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
            null, extraProps, javaProps));
        }
        else
        {
          CacheHelper.SetupJavaServers(false, CacheXml1);
          CacheHelper.StartJavaServer(1, "GFECS1", SecurityTestUtil.GetServerArgs(
            null, extraProps, javaProps));
        }
        Util.Log("Cacheserver 1 started.");

        // Start client1 with valid credentials and client2 with invalid
        // credentials; both should succeed with no authenticator on server
        Properties<string, string> credentials1 = gen.GetValidCredentials(3);
        Properties<string, string> javaProps1 = gen.JavaProperties;
        Util.Log("NoAuthenticatorWithCredentials: For first client valid " +
          "credentials: " + credentials1 + " : " + javaProps1);
        Properties<string, string> credentials2 = gen.GetInvalidCredentials(12);
        Properties<string, string> javaProps2 = gen.JavaProperties;
        Util.Log("NoAuthenticatorWithCredentials: For second client invalid " +
          "credentials: " + credentials2 + " : " + javaProps2);

        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials1, pool, locator);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials2, pool, locator);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 4);

        // Verify that the puts succeeded
        m_client2.Call(DoGets, 4);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);

        if (pool && locator)
        {
          CacheHelper.StopJavaLocator(1);
        }

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runCredentialsWithFailover(bool pool, bool locator)
    {
      foreach (CredentialGenerator gen in SecurityTestUtil.getAllGenerators(false))
      {
        Properties<string, string> extraProps = gen.SystemProperties;
        Properties<string, string> javaProps = gen.JavaProperties;
        string authenticator = gen.Authenticator;
        string authInit = gen.AuthInit;

        Util.Log("CredentialsWithFailover: Using scheme: " +
          gen.GetClassCode());
        Util.Log("CredentialsWithFailover: Using authenticator: " +
          authenticator);
        Util.Log("CredentialsWithFailover: Using authinit: " + authInit);

        // Start the first server; do not start second server yet to force
        // the clients to connect to the first server.
        if (pool && locator)
        {
          CacheHelper.SetupJavaServers(true, CacheXml1, CacheXml2);
          CacheHelper.StartJavaLocator(1, "GFELOC");
          Util.Log("Locator started");
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        else
        {
          CacheHelper.SetupJavaServers(false, CacheXml1, CacheXml2);
          CacheHelper.StartJavaServer(1, "GFECS1", SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        Util.Log("Cacheserver 1 started.");

        // Start the clients with valid credentials
        Properties<string, string> credentials1 = gen.GetValidCredentials(5);
        Properties<string, string> javaProps1 = gen.JavaProperties;
        Util.Log("CredentialsWithFailover: For first client valid " +
          "credentials: " + credentials1 + " : " + javaProps1);
        Properties<string, string> credentials2 = gen.GetValidCredentials(6);
        Properties<string, string> javaProps2 = gen.JavaProperties;
        Util.Log("CredentialsWithFailover: For second client valid " +
          "credentials: " + credentials2 + " : " + javaProps2);
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials1, pool, locator);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials2, pool, locator);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 2);
        // Verify that the puts succeeded
        m_client2.Call(DoGets, 2);

        // Start the second server and stop the first one to force a failover
        if (pool && locator)
        {
          CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        else
        {
          CacheHelper.StartJavaServer(2, "GFECS2", SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StopJavaServer(1);

        // Perform some create/update operations from client1
        // and verify from client2
        m_client1.Call(DoPuts, 4, true, ExpectedResult.Success);
        m_client2.Call(DoGets, 4, true);

        // Try to connect client2 with no credentials
        // Verify that the creation of region throws security exception
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, (string)null, (Properties<string, string>)null,
          ExpectedResult.AuthRequiredException, pool, locator);

        // Now try to connect client1 with invalid credentials
        // Verify that the creation of region throws security exception
        credentials1 = gen.GetInvalidCredentials(7);
        javaProps1 = gen.JavaProperties;
        Util.Log("CredentialsWithFailover: For first client invalid " +
          "credentials: " + credentials1 + " : " + javaProps1);
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials1,
          ExpectedResult.AuthFailedException, pool, locator);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(2);

        if (pool && locator)
        {
          CacheHelper.StopJavaLocator(1);
        }

        CacheHelper.ClearLocators();
        CacheHelper.ClearEndpoints();
      }
    }

    public void registerPdxType8()
    {
      Serializable.RegisterPdxType(PdxTests.PdxTypes8.CreateDeserializable);
    }

    void runCredentialsForNotifications(bool pool, bool locator)
    {

      foreach (CredentialGenerator gen in SecurityTestUtil.getAllGenerators(false))
      {
        Properties<string, string> extraProps = gen.SystemProperties;
        Properties<string, string> javaProps = gen.JavaProperties;
        string authenticator = gen.Authenticator;
        string authInit = gen.AuthInit;

        Util.Log("CredentialsForNotifications: Using scheme: " +
          gen.GetClassCode());
        Util.Log("CredentialsForNotifications: Using authenticator: " +
          authenticator);
        Util.Log("CredentialsForNotifications: Using authinit: " + authInit);

        // Start the two servers.
        if (pool && locator)
        {
          CacheHelper.SetupJavaServers(true, CacheXml1, CacheXml2);
          CacheHelper.StartJavaLocator(1, "GFELOC");
          Util.Log("Locator started");
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
          Util.Log("Cacheserver 1 started.");
          CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
          Util.Log("Cacheserver 2 started.");
        }
        else
        {
          CacheHelper.SetupJavaServers(false, CacheXml1, CacheXml2);
          CacheHelper.StartJavaServer(1, "GFECS1", SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
          Util.Log("Cacheserver 1 started.");
          CacheHelper.StartJavaServer(2, "GFECS2", SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
          Util.Log("Cacheserver 2 started.");
        }

        // Start the clients with valid credentials
        Properties<string, string> credentials1 = gen.GetValidCredentials(5);
        Properties<string, string> javaProps1 = gen.JavaProperties;
        Util.Log("CredentialsForNotifications: For first client valid " +
          "credentials: " + credentials1 + " : " + javaProps1);
        Properties<string, string> credentials2 = gen.GetValidCredentials(6);
        Properties<string, string> javaProps2 = gen.JavaProperties;
        Util.Log("CredentialsForNotifications: For second client valid " +
          "credentials: " + credentials2 + " : " + javaProps2);
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials1, pool, locator);
        // Set up zero forward connections to check notification handshake only
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials2,
          0, ExpectedResult.Success, pool, locator);

        m_client1.Call(registerPdxType8);
        m_client2.Call(registerPdxType8);

        // Register interest for all keys on second client
        m_client2.Call(RegisterAllKeys, new string[] { RegionName });

        // Wait for secondary server to see second client
        Thread.Sleep(1000);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 2);
        // Verify that the puts succeeded
        m_client2.Call(DoLocalGets, 2);

        // Stop the first server
        CacheHelper.StopJavaServer(1);

        // Perform some create/update operations from client1
        m_client1.Call(DoPuts, 4, true, ExpectedResult.Success);
        // Verify that the creates/updates succeeded
        m_client2.Call(DoLocalGets, 4, true);

        // Start server1 again and try to connect client1 using zero forward
        // connections with no credentials.
        // Verify that the creation of region throws authentication
        // required exception
        if (pool && locator)
        {
          CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        else
        {
          CacheHelper.StartJavaServer(1, "GFECS1", SecurityTestUtil.GetServerArgs(
            authenticator, extraProps, javaProps));
        }
        Util.Log("Cacheserver 1 started.");
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, (string)null, (Properties<string, string>)null,
          0, ExpectedResult.AuthRequiredException, pool, locator);

        // Now try to connect client2 using zero forward connections
        // with invalid credentials.
        // Verify that the creation of region throws security exception
        credentials2 = gen.GetInvalidCredentials(3);
        javaProps2 = gen.JavaProperties;
        Util.Log("CredentialsForNotifications: For second client invalid " +
          "credentials: " + credentials2 + " : " + javaProps2);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, authInit, credentials2,
          0, ExpectedResult.AuthFailedException, pool, locator);
        // Now try to connect client2 with invalid auth-init method
        // Trying to create the region on client with valid credentials should
        // throw an authentication failed exception
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, "GemStone.GemFire.Templates.Cache.Security.none",
          credentials1, 0, ExpectedResult.AuthRequiredException, pool, locator);

        // Try connection with null auth-init on clients.
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, (string)null, credentials1,
          0, ExpectedResult.AuthRequiredException, pool, locator);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Endpoints, CacheHelper.Locators, (string)null, credentials2,
          0, ExpectedResult.AuthRequiredException, pool, locator);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);
        CacheHelper.StopJavaServer(2);

        if (pool && locator)
        {
          CacheHelper.StopJavaLocator(1);
        }
        CacheHelper.ClearLocators();
        CacheHelper.ClearEndpoints();
      }
    }

    [Test]
    public void ValidCredentials()
    {
      runValidCredentials(true, false); // pool with server endpoints
      runValidCredentials(true, true); // pool with locator
    }

    [Test]
    public void NoCredentials()
    {
      runNoCredentials(true, false); // pool with server endpoints
      runNoCredentials(true, true); // pool with locator
    }

    [Test]
    public void InvalidCredentials()
    {
      runInvalidCredentials(true, false); // pool with server endpoints
      runInvalidCredentials(true, true); // pool with locator
    }

    [Test]
    public void InvalidAuthInit()
    {
      runInvalidAuthInit(true, false); // pool with server endpoints
      runInvalidAuthInit(true, true); // pool with locator
    }

    [Test]
    public void InvalidAuthenticator()
    {
      runInvalidAuthenticator(true, false); // pool with server endpoints
      runInvalidAuthenticator(true, true); // pool with locator
    }

    [Test]
    public void NoAuthInitWithCredentials()
    {
      runNoAuthInitWithCredentials(true, false); // pool with server endpoints
      runNoAuthInitWithCredentials(true, true); // pool with locator
    }

    [Test]
    public void NoAuthenticatorWithCredentials()
    {
      runNoAuthenticatorWithCredentials(true, false); // pool with server endpoints
      runNoAuthenticatorWithCredentials(true, true); // pool with locator
    }

    [Test]
    public void CredentialsWithFailover()
    {
      runCredentialsWithFailover(true, false); // pool with server endpoints
      runCredentialsWithFailover(true, true); // pool with locator
    }
    
    [Test]
    public void CredentialsForNotifications()
    {
      runCredentialsForNotifications(true, false); // pool with server endpoints
      runCredentialsForNotifications(true, true); // pool with locator
    }
  }
}
