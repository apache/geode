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
using System.Collections.Generic;
using System.Threading;

namespace Apache.Geode.Client.UnitTests
{
  using NUnit.Framework;
  using Apache.Geode.Client.Tests;
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client;

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

    void runValidCredentials()
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
        CacheHelper.SetupJavaServers(true, CacheXml1);
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
          authenticator, extraProps, javaProps));
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
          CacheHelper.Locators, authInit, credentials1);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, credentials2);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 4);

        m_client1.Call(DoTxPuts);

        // Verify that the puts succeeded
        m_client2.Call(DoGets, 4);

        m_client2.Call(DoGetsTx);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);

        CacheHelper.StopJavaLocator(1);

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runNoCredentials()
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
        CacheHelper.SetupJavaServers(true, CacheXml1);
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
          authenticator, extraProps, javaProps));
        Util.Log("Cacheserver 1 started.");

        // Start the clients with valid credentials
        Properties<string, string> credentials1 = gen.GetValidCredentials(3);
        Properties<string, string> javaProps1 = gen.JavaProperties;
        Util.Log("NoCredentials: For first client credentials: " +
          credentials1 + " : " + javaProps1);

        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, credentials1);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 4);

        // Creating region on client2 should throw authentication
        // required exception
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, (string)null, (Properties<string, string>)null,
          ExpectedResult.AuthRequiredException);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);

        CacheHelper.StopJavaLocator(1);
 
        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runInvalidCredentials()
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
        CacheHelper.SetupJavaServers(true, CacheXml1);
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
          authenticator, extraProps, javaProps));
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
          CacheHelper.Locators, authInit, credentials1);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 4);

        // Creating region on client2 should throw authentication
        // failure exception
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, credentials2,
          ExpectedResult.AuthFailedException);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);

        CacheHelper.StopJavaLocator(1);

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runInvalidAuthInit()
    {
      foreach (CredentialGenerator gen in SecurityTestUtil.getAllGenerators(false))
      {
        Properties<string, string> extraProps = gen.SystemProperties;
        Properties<string, string> javaProps = gen.JavaProperties;
        string authenticator = gen.Authenticator;

        Util.Log("InvalidAuthInit: Using scheme: " + gen.GetClassCode());
        Util.Log("InvalidAuthInit: Using authenticator: " + authenticator);

        // Start the server
        CacheHelper.SetupJavaServers(true, CacheXml1);
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
          authenticator, extraProps, javaProps));
        Util.Log("Cacheserver 1 started.");

        // Creating region on client1 with invalid authInit callback should
        // throw authentication required exception
        Properties<string, string> credentials = gen.GetValidCredentials(5);
        javaProps = gen.JavaProperties;
        Util.Log("InvalidAuthInit: For first client credentials: " +
          credentials + " : " + javaProps);
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, "Apache.Geode.Templates.Cache.Security.none",
          credentials, ExpectedResult.AuthRequiredException);

        m_client1.Call(Close);

        CacheHelper.StopJavaServer(1);

        CacheHelper.StopJavaLocator(1);

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runInvalidAuthenticator()
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
        CacheHelper.SetupJavaServers(true, CacheXml1);
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
          "org.apache.geode.none", extraProps, javaProps));
        Util.Log("Cacheserver 1 started.");

        // Starting the client with valid credentials should throw
        // authentication failed exception
        Properties<string, string> credentials1 = gen.GetValidCredentials(1);
        Properties<string, string> javaProps1 = gen.JavaProperties;
        Util.Log("InvalidAuthenticator: For first client valid credentials: " +
          credentials1 + " : " + javaProps1);
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, credentials1,
          ExpectedResult.AuthFailedException);

        // Also try with invalid credentials
        Properties<string, string> credentials2 = gen.GetInvalidCredentials(1);
        Properties<string, string> javaProps2 = gen.JavaProperties;
        Util.Log("InvalidAuthenticator: For first client invalid " +
          "credentials: " + credentials2 + " : " + javaProps2);
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, credentials2,
          ExpectedResult.AuthFailedException);

        m_client1.Call(Close);

        CacheHelper.StopJavaServer(1);

        CacheHelper.StopJavaLocator(1);

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runNoAuthInitWithCredentials()
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
        CacheHelper.SetupJavaServers(true, CacheXml1);
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
          authenticator, extraProps, javaProps));
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
          CacheHelper.Locators, (string)null, credentials1,
          ExpectedResult.AuthRequiredException);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, (string)null, credentials2,
          ExpectedResult.AuthRequiredException);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);

        CacheHelper.StopJavaLocator(1);

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runNoAuthenticatorWithCredentials()
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
        CacheHelper.SetupJavaServers(true, CacheXml1);
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
          null, extraProps, javaProps));
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
          CacheHelper.Locators, authInit, credentials1);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, credentials2);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 4);

        // Verify that the puts succeeded
        m_client2.Call(DoGets, 4);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);

        CacheHelper.StopJavaLocator(1);

        CacheHelper.ClearEndpoints();
        CacheHelper.ClearLocators();
      }
    }

    void runCredentialsWithFailover()
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
        CacheHelper.SetupJavaServers(true, CacheXml1, CacheXml2);
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
          authenticator, extraProps, javaProps));
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
          CacheHelper.Locators, authInit, credentials1);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, credentials2);

        // Perform some put operations from client1
        m_client1.Call(DoPuts, 2);
        // Verify that the puts succeeded
        m_client2.Call(DoGets, 2);

        // Start the second server and stop the first one to force a failover
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, SecurityTestUtil.GetServerArgs(
          authenticator, extraProps, javaProps));
        Util.Log("Cacheserver 2 started.");
        CacheHelper.StopJavaServer(1);

        // Perform some create/update operations from client1
        // and verify from client2
        m_client1.Call(DoPuts, 4, true, ExpectedResult.Success);
        m_client2.Call(DoGets, 4, true);

        // Try to connect client2 with no credentials
        // Verify that the creation of region throws security exception
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
         CacheHelper.Locators, (string)null, (Properties<string, string>)null,
          ExpectedResult.AuthRequiredException);

        // Now try to connect client1 with invalid credentials
        // Verify that the creation of region throws security exception
        credentials1 = gen.GetInvalidCredentials(7);
        javaProps1 = gen.JavaProperties;
        Util.Log("CredentialsWithFailover: For first client invalid " +
          "credentials: " + credentials1 + " : " + javaProps1);
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, credentials1,
          ExpectedResult.AuthFailedException);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(2);

        CacheHelper.StopJavaLocator(1);

        CacheHelper.ClearLocators();
        CacheHelper.ClearEndpoints();
      }
    }

    public void registerPdxType8()
    {
      Serializable.RegisterPdxType(PdxTests.PdxTypes8.CreateDeserializable);
    }

    void runCredentialsForNotifications()
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
        CacheHelper.SetupJavaServers(true, CacheXml1, CacheXml2);
        CacheHelper.StartJavaLocator(1, "GFELOC");
        Util.Log("Locator started");
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
          authenticator, extraProps, javaProps));
        Util.Log("Cacheserver 1 started.");
        CacheHelper.StartJavaServerWithLocators(2, "GFECS2", 1, SecurityTestUtil.GetServerArgs(
          authenticator, extraProps, javaProps));
        Util.Log("Cacheserver 2 started.");

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
          CacheHelper.Locators, authInit, credentials1);
        // Set up zero forward connections to check notification handshake only
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, credentials2,
          0, ExpectedResult.Success);

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
        CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1, SecurityTestUtil.GetServerArgs(
          authenticator, extraProps, javaProps));
        Util.Log("Cacheserver 1 started.");
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, (string)null, (Properties<string, string>)null,
          0, ExpectedResult.AuthRequiredException);

        // Now try to connect client2 using zero forward connections
        // with invalid credentials.
        // Verify that the creation of region throws security exception
        credentials2 = gen.GetInvalidCredentials(3);
        javaProps2 = gen.JavaProperties;
        Util.Log("CredentialsForNotifications: For second client invalid " +
          "credentials: " + credentials2 + " : " + javaProps2);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, authInit, credentials2,
          0, ExpectedResult.AuthFailedException);
        // Now try to connect client2 with invalid auth-init method
        // Trying to create the region on client with valid credentials should
        // throw an authentication failed exception
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, "Apache.Geode.Templates.Cache.Security.none",
          credentials1, 0, ExpectedResult.AuthRequiredException);

        // Try connection with null auth-init on clients.
        m_client1.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, (string)null, credentials1,
          0, ExpectedResult.AuthRequiredException);
        m_client2.Call(SecurityTestUtil.CreateClient, RegionName,
          CacheHelper.Locators, (string)null, credentials2,
          0, ExpectedResult.AuthRequiredException);

        m_client1.Call(Close);
        m_client2.Call(Close);

        CacheHelper.StopJavaServer(1);
        CacheHelper.StopJavaServer(2);

        CacheHelper.StopJavaLocator(1);
        CacheHelper.ClearLocators();
        CacheHelper.ClearEndpoints();
      }
    }

    [Test]
    public void ValidCredentials()
    {
      runValidCredentials();
    }

    [Test]
    public void NoCredentials()
    {
      runNoCredentials();
    }

    [Test]
    public void InvalidCredentials()
    {
      runInvalidCredentials();
    }

    [Test]
    public void InvalidAuthInit()
    {
      runInvalidAuthInit();
    }

    [Test]
    public void InvalidAuthenticator()
    {
      runInvalidAuthenticator();
    }

    [Test]
    public void NoAuthInitWithCredentials()
    {
      runNoAuthInitWithCredentials();
    }

    [Test]
    public void NoAuthenticatorWithCredentials()
    {
      runNoAuthenticatorWithCredentials();
    }

    [Test]
    public void CredentialsWithFailover()
    {
      runCredentialsWithFailover();
    }
    
    [Test]
    public void CredentialsForNotifications()
    {
      runCredentialsForNotifications();
    }
  }
}
