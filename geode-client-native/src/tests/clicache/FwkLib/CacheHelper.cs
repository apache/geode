//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;

#pragma warning disable 618

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  /// <summary>
  /// Helper class to create/destroy Distributed System, cache and regions.
  /// This class is intentionally not thread-safe.
  /// </summary>
  public class CacheHelper
  {
    #region Private static members and constants

    private static DistributedSystem m_dsys = null;
    private static Cache m_cache = null;
    private static Region m_currRegion = null;

    private const string DefaultDSName = "dstest";
    private const string DefaultCacheName = "cachetest";

    private static char PathSep = Path.DirectorySeparatorChar;
    
    private static bool m_SkipSetNewAndDelete = false;

    #endregion

    #region Private Utility functions
    
    // For native layer C++ runtime mismatch.
    private static void CheckEnvAndSetNewAndDelete()
    {
      if (!m_SkipSetNewAndDelete)
      {
        string envsetting = System.Environment.GetEnvironmentVariable("BUG481");
        if (envsetting != null && envsetting.Length > 0)
        {
          CacheFactory.SetNewAndDelete();
          Util.Log("SetNewAndDelete() called");
        }
        m_SkipSetNewAndDelete = true;
      }
    }

    private static void DSConnect(string dsName, Properties config)
    {
      CheckEnvAndSetNewAndDelete();
      
      if (Util.LogFile != null)
      {
        if (config == null)
        {
          config = new Properties();
        }
        string logFile = Regex.Replace(Util.LogFile, "\\....$", string.Empty);
        config.Insert("log-file", logFile);
      }
      try
      {
        m_dsys = DistributedSystem.Connect(dsName, config);
      }
      catch (AlreadyConnectedException)
      {
        m_dsys = DistributedSystem.GetInstance();
      }
    }

    private static void FwkInfo(string message)
    {
      if (FwkTest.CurrentTest != null)
      {
        FwkTest.CurrentTest.FwkInfo(message);
      }
    }

    private static void FwkInfo(string fmt, params object[] paramList)
    {
      if (FwkTest.CurrentTest != null)
      {
        FwkTest.CurrentTest.FwkInfo(fmt, paramList);
      }
    }

    private static void FwkAssert(bool condition, string message)
    {
      if (FwkTest.CurrentTest != null)
      {
        FwkTest.CurrentTest.FwkAssert(condition, message);
      }
    }

    private static void FwkAssert(bool condition, string fmt,
      params object[] paramList)
    {
      if (FwkTest.CurrentTest != null)
      {
        FwkTest.CurrentTest.FwkAssert(condition, fmt, paramList);
      }
    }

    #endregion

    #region Public accessors

    public static DistributedSystem DSYS
    {
      get
      {
        return m_dsys;
      }
    }

    public static Cache DCache
    {
      get
      {
        return m_cache;
      }
      set
      {
        m_cache = value;
      }
    }
    public static Region CurrentRegion
    {
      get
      {
        return m_currRegion;
      }
    }

    #endregion

    #region Functions to initialize or close a cache and distributed system

    public static void ConnectName(string dsName)
    {
      ConnectConfig(dsName, null);
    }

    public static void ConnectConfig(string dsName, Properties config)
    {
      if (DistributedSystem.IsConnected)
      {
        if (m_dsys == null)
        {
          m_dsys = DistributedSystem.GetInstance();
        }
      }
      else
      {
        DSConnect(dsName, config);
      }
    }

    public static void Init()
    {
      InitName(DefaultDSName, DefaultCacheName);
    }

    public static void InitName(string dsName, string cacheName)
    {
      InitConfig(dsName, cacheName, null, null, null);
    }

    public static void InitConfig(Properties config)
    {
      InitConfig(DefaultDSName, DefaultCacheName, config,
        null, null);
    }

    public static void InitConfig(Properties dsProps, string endpoints,
      int redundancyLevel)
    {
      CheckEnvAndSetNewAndDelete();
      
      CacheAttributesFactory cFact = new CacheAttributesFactory();
      cFact.SetEndpoints(endpoints);
      cFact.SetRedundancyLevel(redundancyLevel);
      InitConfig(DefaultDSName, DefaultCacheName, dsProps,
        cFact.CreateCacheAttributes(), null);
    }
    public static void InitConfigPool(Properties dsProps)
    {
      CheckEnvAndSetNewAndDelete();
      
      CacheAttributesFactory cFact = new CacheAttributesFactory();
      InitConfig(DefaultDSName, DefaultCacheName, dsProps,
        cFact.CreateCacheAttributes(), null);
    }
    public static void InitConfig(string cacheXml)
    {
      InitConfig(DefaultDSName, DefaultCacheName, null, null, cacheXml);
    }

    public static void InitConfig(string dsName, string cacheName,
      Properties config, CacheAttributes cAttrs,
      string cacheXml)
    {
      CheckEnvAndSetNewAndDelete();
      
      string gfcppPropsFile = Util.AssemblyDir + "/gfcpp.properties";
      if (File.Exists(gfcppPropsFile))
      {
        Properties newConfig = new Properties();
        newConfig.Load(gfcppPropsFile);
        if (config != null)
        {
          newConfig.AddAll(config);
        }
        config = newConfig;
      }
      ConnectConfig(dsName, config);
      if (m_cache == null || m_cache.IsClosed)
      {
        try
        {
          m_cache = CacheFactory.Create(cacheName, m_dsys, cacheXml, cAttrs);
        }
        catch (CacheExistsException)
        {
          m_cache = CacheFactory.GetInstance(m_dsys);
        }
      }
    }

    public static void InitConfigForDurable(string endpoints, int redundancyLevel, string durableClientId, int durableTimeout, string conflateEvents, bool isSslEnable)
    {
      CheckEnvAndSetNewAndDelete();
      
      CacheAttributesFactory cFact = new CacheAttributesFactory();
      cFact.SetEndpoints(endpoints);
      cFact.SetRedundancyLevel(redundancyLevel);
      Properties config = Properties.Create();
      config.Insert("durable-client-id", durableClientId);
      config.Insert("durable-timeout", durableTimeout);
      if (conflateEvents != null && conflateEvents.Length > 0)
      {
        config.Insert("conflate-events", conflateEvents);
      }
      if (isSslEnable)
      {
        config.Insert("ssl-enabled", "true");
        string keyStorePath = Util.GetFwkLogDir(Util.SystemType) + "/data/keystore";
        string pubkey = keyStorePath + "/client_truststore.pem";
        string privkey = keyStorePath + "/client_keystore.pem";
        config.Insert("ssl-keystore", privkey);
        config.Insert("ssl-truststore", pubkey);
      }
      InitConfig(DefaultDSName, DefaultCacheName, config,
        cFact.CreateCacheAttributes(), null);
    }
    public static void InitConfigForPoolDurable(string durableClientId, int durableTimeout, string conflateEvents, bool isSslEnable)
    {
      CheckEnvAndSetNewAndDelete();
      
      CacheAttributesFactory cFact = new CacheAttributesFactory();
      Properties config = Properties.Create();
      config.Insert("durable-client-id", durableClientId);
      config.Insert("durable-timeout", durableTimeout);
      if (conflateEvents != null && conflateEvents.Length > 0)
      {
        config.Insert("conflate-events", conflateEvents);
      }
      if (isSslEnable)
      {
        config.Insert("ssl-enabled", "true");
        string keyStorePath = Util.GetFwkLogDir(Util.SystemType) + "/data/keystore";
        string pubkey = keyStorePath + "/client_truststore.pem";
        string privkey = keyStorePath + "/client_keystore.pem";
        config.Insert("ssl-keystore", privkey);
        config.Insert("ssl-truststore", pubkey);
      }
      InitConfig(DefaultDSName, DefaultCacheName, config,
        cFact.CreateCacheAttributes(), null);

    }
    public static void InitClient()
    {
      CheckEnvAndSetNewAndDelete();
      
      CacheHelper.Close();
      Properties config = new Properties();
      CacheHelper.InitConfig(config);
    }

    public static void Close()
    {
      if (DistributedSystem.IsConnected)
      {
        CloseCache();
        DistributedSystem.Disconnect();
      }
      m_dsys = null;
    }

    public static void CloseCache()
    {
      if (m_cache != null && !m_cache.IsClosed)
      {
        m_cache.Close();
      }
      m_cache = null;
    }

    public static void CloseKeepAlive()
    {
      if (DistributedSystem.IsConnected)
      {
        CloseCacheKeepAlive();
        DistributedSystem.Disconnect();
      }
      m_dsys = null;
    }

    public static void CloseCacheKeepAlive()
    {
      if (m_cache != null && !m_cache.IsClosed)
      {
        m_cache.Close(true);
      }
      m_cache = null;
    }

    #endregion

    #region Functions to create or destroy a region

    public static Region CreateRegion(string name, RegionFactory attrs)
    {
      Init();
      DestroyRegion(name, true, false);
      Region region = attrs.Create(name);
      FwkAssert(region != null, "Region {0} was not created.", name);
      m_currRegion = region;
      return region;
     
    }

    public static void DestroyRegion(string name, bool local, bool verify)
    {
      Region region;
      if (verify)
      {
        region = GetVerifyRegion(name);
      }
      else
      {
        region = GetRegion(name);
      }
      if (region != null)
      {
        if (local)
        {
          region.LocalDestroyRegion();
        }
        else
        {
          region.DestroyRegion();
        }
      }
    }

    public static void DestroyAllRegions(bool local)
    {
      if (m_cache != null && !m_cache.IsClosed)
      {
        Region[] regions = m_cache.RootRegions();
        if (regions != null)
        {
          foreach (Region region in regions)
          {
            if (local)
            {
              region.LocalDestroyRegion();
            }
            else
            {
              region.DestroyRegion();
            }
          }
        }
      }
    }
    public static void SetDCacheNull()
    {
      m_cache = null;
    }
    #endregion

    #region Functions to obtain a region

    public static Region GetRegion(string path)
    {
      if (m_cache != null)
      {
        return m_cache.GetRegion(path);
      }
      return null;
    }

    public static Region GetVerifyRegion(string path)
    {
      Region region = GetRegion(path);
      FwkAssert(region != null, "Region [" + path + "] not found.");
      FwkInfo("Found region '{0}'", path);
      return region;
    }

    #endregion

    #region Utility functions

    public static void LogKeys(ICacheableKey[] cKeys)
    {
      if (cKeys != null)
      {
        for (int i = 0; i < cKeys.Length; i++)
        {
          FwkInfo("Key [{0}] = {1}", i, cKeys[i]);
        }
      }
    }

    public static void LogValues(IGFSerializable[] cValues)
    {
      if (cValues != null)
      {
        for (int i = 0; i < cValues.Length; i++)
        {
          FwkInfo("Value [{0}] = {1}", i, cValues[i]);
        }
      }
    }

    public static string RegionTag(RegionAttributes attrs)
    {
      string tag = attrs.Scope.ToString();
      tag += attrs.CachingEnabled ? "Caching" : "NoCache";
      tag += (attrs.CacheListener == null) ? "Nlstnr" : "Lstnr";
      return tag;
    }

    public static string RegionAttributesToString(RegionAttributes attrs)
    {
      StringBuilder attrsSB = new StringBuilder();
      attrsSB.Append(Environment.NewLine + "scope: " +
        attrs.Scope);
      attrsSB.Append(Environment.NewLine + "caching: " +
        attrs.CachingEnabled);
      attrsSB.Append(Environment.NewLine + "endpoints: " +
        attrs.Endpoints);
      attrsSB.Append(Environment.NewLine + "pool: " +
        attrs.PoolName);
      attrsSB.Append(Environment.NewLine + "clientNotification: " +
        attrs.ClientNotificationEnabled);
      attrsSB.Append(Environment.NewLine + "initialCapacity: " +
        attrs.InitialCapacity);
      attrsSB.Append(Environment.NewLine + "loadFactor: " +
        attrs.LoadFactor);
      attrsSB.Append(Environment.NewLine + "concurrencyLevel: " +
        attrs.ConcurrencyLevel);
      attrsSB.Append(Environment.NewLine + "lruEntriesLimit: " +
        attrs.LruEntriesLimit);
      attrsSB.Append(Environment.NewLine + "lruEvictionAction: " +
        attrs.LruEvictionAction);
      attrsSB.Append(Environment.NewLine + "entryTimeToLive: " +
        attrs.EntryTimeToLive);
      attrsSB.Append(Environment.NewLine + "entryTimeToLiveAction: " +
        attrs.EntryTimeToLiveAction);
      attrsSB.Append(Environment.NewLine + "entryIdleTimeout: " +
        attrs.EntryIdleTimeout);
      attrsSB.Append(Environment.NewLine + "entryIdleTimeoutAction: " +
        attrs.EntryIdleTimeoutAction);
      attrsSB.Append(Environment.NewLine + "regionTimeToLive: " +
        attrs.RegionTimeToLive);
      attrsSB.Append(Environment.NewLine + "regionTimeToLiveAction: " +
        attrs.RegionTimeToLiveAction);
      attrsSB.Append(Environment.NewLine + "regionIdleTimeout: " +
        attrs.RegionIdleTimeout);
      attrsSB.Append(Environment.NewLine + "regionIdleTimeoutAction: " +
        attrs.RegionIdleTimeoutAction);
      attrsSB.Append(Environment.NewLine);
      return attrsSB.ToString();
    }

    #endregion
  }
}
