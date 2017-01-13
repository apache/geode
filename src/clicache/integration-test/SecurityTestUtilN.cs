//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.UnitTests.NewAPI
{
  using NUnit.Framework;
  using GemStone.GemFire.Cache.Tests.NewAPI;
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
  using AssertionException = GemStone.GemFire.Cache.Generic.AssertionException;

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
      Properties<string, string> extraProps, Properties<string, string> javaProps)
    {
      return Utility.GetServerArgs(authenticator, null, null,
        extraProps, javaProps);
    }

    public static string GetServerArgs(string authenticator, string accessor,
      string accessorPP, Properties<string, string> extraProps, Properties<string, string> javaProps)
    {
      return Utility.GetServerArgs(authenticator, accessor, accessorPP,
        extraProps, javaProps);
    }

    public static Properties<string, string> ConcatProperties(params Properties<string, string>[] propsArray)
    {
      Properties<string, string> result = null;
      if (propsArray != null)
      {
        result = new Properties<string, string>();
        foreach (Properties<string, string> props in propsArray)
        {          
          result.AddAll(props);
        }
      }
      return result;
    }

    public static void CreateClientMU(string regionName, string locators,
      string authInit, Properties<string, string> credentials, bool isMultiuser)
    {
      CreateClient(regionName, locators, authInit, credentials,
        ExpectedResult.Success, isMultiuser, null);
    }
    //for notification
    public static void CreateClientMU2(string regionName, string locators,
      string authInit, Properties<string, string> credentials, bool isMultiuser, bool notificationEnabled)
    {
      CreateClient<object, object>(regionName, locators,
      authInit, credentials, false,
      notificationEnabled, 0, -1,
      null, ExpectedResult.Success, isMultiuser, null, false, false);
    }
    //for durable client...
    public static void CreateMUDurableClient(string regionName, string locators,
      string authInit, string durableClientId, bool isMultiuser, bool notificationEnabled)
    {
      CreateClient<object, object>(regionName, locators,
      authInit, null, false,
      notificationEnabled, 0, -1,
      null, ExpectedResult.Success, isMultiuser, durableClientId, false, false);
    }

    public static void CreateClient(string regionName, string locators,
      string authInit, Properties<string, string> credentials)
    {
      CreateClient(regionName, locators, authInit, credentials,
        ExpectedResult.Success);
    }

    public static void CreateClientSSL(string regionName, string locators,
      string authInit, Properties<string, string> credentials, bool ssl, bool withPassword)
    {
      CreateClient<object, object>(regionName, locators, authInit, credentials,
        true, true, 1, -1, null, ExpectedResult.Success, false, null, ssl, withPassword);
    }

    public static void CreateClientR0(string regionName, string locators,
      string authInit, Properties<string, string> credentials)
    {
      CreateClient(regionName, locators, authInit, credentials,
        ExpectedResult.Success, 0);
    }

    public static void CreateClient(string regionName, string locators,
      string authInit, Properties<string, string> credentials, ExpectedResult expect, int redundancy)
    {
      CreateClient<object, object>(regionName, locators, authInit, credentials, true, true,
        redundancy, -1, null, expect);
    }

    public static void CreateClient(string regionName, string locators,
      string authInit, Properties<string, string> credentials, ExpectedResult expect,
      bool isMultiuser, string durebleClientId)
    {
      CreateClient<object, object>(regionName, locators, authInit, credentials, false, false,
        0, -1, null, expect, isMultiuser, durebleClientId, false, false);
    }

    public static void CreateClient(string regionName, string locators,
      string authInit, Properties<string, string> credentials, ExpectedResult expect)
    {
      CreateClient<object, object>(regionName, locators, authInit, credentials, true, true,
        1, -1, null, expect);
    }

    public static void CreateClient(string regionName, string locators,
      string authInit, Properties<string, string> credentials, int numConnections,
      ExpectedResult expect)
    {
      CreateClient<object, object>(regionName, locators, authInit, credentials, true, true,
        1, numConnections, null, expect);
    }

    public static void CreateClient<TKey, TValue>(string regionName, string locators,
      string authInit, Properties<string, string> credentials, bool caching,
      bool clientNotification, int redundancyLevel, int numConnections,
      ICacheListener<TKey, TValue> listener, ExpectedResult expect)
    {
      CreateClient<TKey, TValue>(regionName, locators, authInit, credentials, true, true,
        redundancyLevel, numConnections, null, expect, false, null, false, false);
    }

    public static void CreateClient<TKey, TValue>(string regionName, string locators,
      string authInit, Properties<string, string> credentials, bool caching,
      bool clientNotification, int redundancyLevel, int numConnections,
      ICacheListener<TKey, TValue> listener, ExpectedResult expect, bool isMultiUser, string durableClientId, bool ssl, bool withPassword)
    {
      Util.Log("Redundancy Level = {0}", redundancyLevel);
      CacheHelper.Close();
      Properties<string, string> sysProps = new Properties<string, string>();
      if (durableClientId != null)
        sysProps.Insert("durable-client-id", durableClientId);
      Utility.GetClientProperties(authInit, credentials, ref sysProps);
      if (numConnections >= 0)
      {
        sysProps.Insert("connection-pool-size", numConnections.ToString());
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
        CacheHelper.InitConfig(sysProps);

        CacheHelper.CreatePool<TKey, TValue>("__TESTPOOL1_", locators, (string)null,
          redundancyLevel, clientNotification, numConnections, isMultiUser);
        CacheHelper.CreateTCRegion_Pool<TKey, TValue>(regionName, true, caching,
          listener, locators, "__TESTPOOL1_", clientNotification);

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
