//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.Cache.Tests;
  using GemStone.GemFire.DUnitFramework;

  /// <summary>
  /// Enumeration to indicate the result expected of an operation.
  /// </summary>
  public enum ExpectedResult
  {
    Success,
    AuthRequiredException,
    AuthFailedException,
    NotAuthorizedException,
    OtherException
  }

  /// <summary>
  /// Helper class to start/stop cache servers and other utility methods
  /// for security tests.
  /// </summary>
  public class SecurityTestUtil
  {
    public static List<CredentialGenerator> getAllGenerators(bool isMultiUser)
    {
      string dataDir = Util.GetEnvironmentVariable("CPP_TESTOUT");
      List<CredentialGenerator> generators = new List<CredentialGenerator>();
      foreach (CredentialGenerator.ClassCode code in Enum.GetValues(
        typeof(CredentialGenerator.ClassCode)))
      {
        CredentialGenerator gen = CredentialGenerator.Create(code, isMultiUser);
        if (gen != null)
        {
          gen.Init(dataDir, dataDir);
          generators.Add(gen);
        }
      }
      return generators;
    }

    public static string GetServerArgs(string authenticator,
      Properties extraProps, Properties javaProps)
    {
      return Utility.GetServerArgs(authenticator, null, null,
        extraProps, javaProps);
    }

    public static string GetServerArgs(string authenticator, string accessor,
      string accessorPP, Properties extraProps, Properties javaProps)
    {
      return Utility.GetServerArgs(authenticator, accessor, accessorPP,
        extraProps, javaProps);
    }

    public static Properties ConcatProperties(params Properties[] propsArray)
    {
      Properties result = null;
      if (propsArray != null)
      {
        result = new Properties();
        foreach (Properties props in propsArray)
        {
          result.AddAll(props);
        }
      }
      return result;
    }

    public static void CreateClientMU(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, bool pool, bool locator, bool isMultiuser)
    {
      CreateClient(regionName, endpoints, locators, authInit, credentials,
        ExpectedResult.Success, pool, locator, isMultiuser, null);
    }
    //for notification
    public static void CreateClientMU2(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, bool pool, bool locator, bool isMultiuser, bool notificationEnabled)
    {
      CreateClient(regionName, endpoints, locators,
      authInit, credentials, false,
      notificationEnabled, 0, -1,
      null, ExpectedResult.Success, pool, locator, isMultiuser, null, false, false);
    }
    //for durable client...
    public static void CreateMUDurableClient(string regionName, string endpoints, string locators,
      string authInit, string durableClientId, bool pool, bool locator, bool isMultiuser, bool notificationEnabled)
    {
      CreateClient(regionName, endpoints, locators,
      authInit, null, false,
      notificationEnabled, 0, -1,
      null, ExpectedResult.Success, pool, locator, isMultiuser, durableClientId, false,false);
    }

    public static void CreateClient(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, bool pool, bool locator)
    {
      CreateClient(regionName, endpoints, locators, authInit, credentials,
        ExpectedResult.Success, pool, locator);
    }

    public static void CreateClientSSL(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, bool pool, bool locator, bool ssl, bool withPassword )
    {
      CreateClient(regionName, endpoints, locators, authInit, credentials,
        true, true, 1, -1, null, ExpectedResult.Success, pool, locator, false, null, ssl, withPassword);
    }

    public static void CreateClientR0(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, bool pool, bool locator)
    {
      CreateClient(regionName, endpoints, locators, authInit, credentials,
        ExpectedResult.Success, 0, pool, locator);
    }
    
    public static void CreateClient(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, ExpectedResult expect, int redundancy, bool pool, bool locator)
    {
      CreateClient(regionName, endpoints, locators, authInit, credentials, true, true,
        redundancy, -1, null, expect, pool, locator);
    }

    public static void CreateClient(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, ExpectedResult expect, bool pool, bool locator, 
      bool isMultiuser, string durebleClientId)
    {
      CreateClient(regionName, endpoints, locators, authInit, credentials, false, false,
        0, -1, null, expect, pool, locator, isMultiuser, durebleClientId, false, false);
    }

    public static void CreateClient(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, ExpectedResult expect, bool pool, bool locator)
    {
      CreateClient(regionName, endpoints, locators, authInit, credentials, true, true,
        1, -1, null, expect, pool, locator);
    }
    
    public static void CreateClient(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, int numConnections,
      ExpectedResult expect, bool pool, bool locator)
    {
      CreateClient(regionName, endpoints, locators, authInit, credentials, true, true,
        1, numConnections, null, expect, pool, locator);
    }

    public static void CreateClient(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, bool caching,
      bool clientNotification, int redundancyLevel, int numConnections,
      ICacheListener listener, ExpectedResult expect, bool pool, bool locator)
    {
      CreateClient(regionName, endpoints, locators, authInit, credentials, true, true,
        redundancyLevel, numConnections, null, expect, pool, locator, false, null, false, false);
    }

    public static void CreateClient(string regionName, string endpoints, string locators,
      string authInit, Properties credentials, bool caching,
      bool clientNotification, int redundancyLevel, int numConnections,
      ICacheListener listener, ExpectedResult expect, bool pool, bool locator, bool isMultiUser, string durableClientId, bool ssl, bool withPassword)
    {
      Util.Log("Redundancy Level = {0}", redundancyLevel);
      CacheHelper.Close();
      Properties sysProps = new Properties();
      if(durableClientId != null)
        sysProps.Insert("durable-client-id", durableClientId);
      Utility.GetClientProperties(authInit, credentials, ref sysProps);
      if (numConnections >= 0)
      {
        sysProps.Insert("connection-pool-size", numConnections.ToString());
      }
      CacheAttributesFactory cFact = new CacheAttributesFactory();
      if (!pool)
      {
        cFact.SetEndpoints(endpoints);
        cFact.SetRedundancyLevel(redundancyLevel);
        Util.Log("CreateClient: setting endpoints[{0}] redundancyLevel[{1}]",
          endpoints, redundancyLevel);
      }
      if (ssl && withPassword)
      {
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sysProps.Insert("ssl-enabled", "true");
        sysProps.Insert("ssl-keystore", keystore + "/client_keystore.password.pem");
        sysProps.Insert("ssl-truststore", keystore + "/client_truststore.pem");
        sysProps.Insert("ssl-keystore-password", "gemstone");
      }
      else if (ssl)
      {
        string keystore = Util.GetEnvironmentVariable("CPP_TESTOUT") + "/keystore";
        sysProps.Insert("ssl-enabled", "true");        
        sysProps.Insert("ssl-keystore", keystore + "/client_keystore.pem");
        sysProps.Insert("ssl-truststore", keystore + "/client_truststore.pem");
      }
      try
      {
        CacheHelper.InitConfig("SecurityDS", "SecurityCache", sysProps,
            cFact.CreateCacheAttributes(), null);

        if (pool)
        {
          if (locator)
          {
            CacheHelper.CreatePool("__TESTPOOL1_", (string)null, locators, (string)null,
              redundancyLevel, clientNotification, numConnections, isMultiUser);
            CacheHelper.CreateTCRegion_Pool(regionName, true, caching,
              listener, (string)null, locators, "__TESTPOOL1_", clientNotification);
          }
          else
          {
            CacheHelper.CreatePool("__TESTPOOL1_", endpoints, (string)null, (string)null,
              redundancyLevel, clientNotification, numConnections, isMultiUser);
            CacheHelper.CreateTCRegion_Pool(regionName, true, caching,
              listener, endpoints, (string)null, "__TESTPOOL1_", clientNotification);
          }
        }
        else
        {
          CacheHelper.CreateTCRegion(regionName, true, caching,
            listener, null, clientNotification);
        }

        if (expect != ExpectedResult.Success)
        {
          Assert.Fail(
            "CreateClient: expected an exception in creating region.");
        }
      }
      catch (AssertionException)
      {
        throw;
      }
      catch (AuthenticationRequiredException ex)
      {
        if (expect == ExpectedResult.AuthRequiredException)
        {
          Util.Log("CreateClient: got expected exception in creating region: "
            + ex.Message);
        }
        else
        {
          throw;
        }
      }
      catch (AuthenticationFailedException ex)
      {
        if (expect == ExpectedResult.AuthFailedException)
        {
          Util.Log("CreateClient: got expected exception in creating region: "
            + ex.Message);
        }
        else
        {
          throw;
        }
      }
      catch (Exception ex)
      {
        if (expect == ExpectedResult.OtherException)
        {
          Util.Log("CreateClient: got expected exception in creating region: "
            + ex.GetType() + "::" + ex.Message);
        }
        else
        {
          throw;
        }
      }
    }
  }
}
