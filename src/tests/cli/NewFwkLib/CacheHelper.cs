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
  using GemStone.GemFire.Cache.Generic;
  //using Region = GemStone.GemFire.Cache.Generic.IRegion<Object, Object>;
  //using IntRegion = GemStone.GemFire.Cache.Generic.IRegion<int, byte[]>;
  //using StringRegion = GemStone.GemFire.Cache.Generic.IRegion<string, byte[]>;
  using VMW = GemStone.GemFire.Cache;
  public class PropsStringToObject
  {
    public PropsStringToObject(Properties<string, object> target)
    {
      m_target = target;
    }

    public void Visit(string key, string val)
    {
      if (key == "security-signature")
      {
        string[] stringbytes = val.Split(' ');
        byte[] credentialbytes = new byte[stringbytes.Length - 1];
        int position = 0;
        foreach (string item in stringbytes)
        {
          if (string.IsNullOrEmpty(item)) continue;
          credentialbytes[position++] = byte.Parse(item);
        }
        m_target.Insert(key, credentialbytes);
      }
      else
      {
        m_target.Insert(key, val);
      }
    }
    private Properties<string, object> m_target;
  }

  public class PropsObjectToObject
  {
    public PropsObjectToObject(Properties<string, object> target)
    {
      m_target = target;
    }

    public void Visit(string key, object val)
    {
      if (key == "security-signature")
      {
        string strval = (string)val;
        string[] stringbytes = strval.Split(' ');
        byte[] credentialbytes = new byte[stringbytes.Length - 1];
        int position = 0;
        foreach (string item in stringbytes)
        {
          if (string.IsNullOrEmpty(item)) continue;
          credentialbytes[position++] = byte.Parse(item);
        }
        m_target.Insert(key, credentialbytes);
      }
      else
      {
        m_target.Insert(key, val);
      }
    }
    private Properties<string, object> m_target;
  }

  /// <summary>
  /// Helper class to create/destroy Distributed System, cache and regions.
  /// This class is intentionally not thread-safe.
  /// </summary>
  public class CacheHelper<TKey, TVal>
  {
    #region Private static members and constants

    private static DistributedSystem m_dsys = null;
    private static Cache m_cache = null;
    private static IRegion<TKey,TVal> m_currRegion = null;
    private static bool m_doDisconnect = true;

    private const string DefaultDSName = "dstest";
    private const string DefaultCacheName = "cachetest";

    private static char PathSep = Path.DirectorySeparatorChar;
    #endregion

    #region Private Utility functions


    private static void FwkInfo(string message)
    {
      if (FwkTest<TKey, TVal>.CurrentTest != null)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo(message);
      }
    }

    private static void FwkInfo(string fmt, params object[] paramList)
    {
      if (FwkTest<TKey, TVal>.CurrentTest != null)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkInfo(fmt, paramList);
      }
    }

    private static void FwkAssert(bool condition, string message)
    {
      if (FwkTest<TKey, TVal>.CurrentTest != null)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkAssert(condition, message);
      }
    }

    private static void FwkAssert(bool condition, string fmt,
      params object[] paramList)
    {
      if (FwkTest<TKey, TVal>.CurrentTest != null)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkAssert(condition, fmt, paramList);
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
    public static IRegion<TKey,TVal> CurrentRegion
    {
      get
      {
        return m_currRegion;
      }
    }

    #endregion

    #region Functions to initialize or close a cache and distributed system


    public static void Init()
    {
      InitConfig(null, null, false);
    }
    public static Properties<string, object> GetPkcsCredentialsForMU(Properties<string, string> credentials)
    {
      if (credentials == null)
        return null;
      Properties<string, object> target = Properties<string, object>.Create<string, object>();
      PropsStringToObject psto = new PropsStringToObject(target);
      credentials.ForEach(new PropertyVisitorGeneric<string, string>(psto.Visit));
      return target;
    }
    public static Properties<string, object> GetPkcsCredentialsForMU(Properties<string, object> credentials)
    {
      if (credentials == null)
        return null;
      Properties<string, object> target = Properties<string, object>.Create<string, object>();
      PropsObjectToObject poto = new PropsObjectToObject(target);
      credentials.ForEach(new PropertyVisitorGeneric<string, object>(poto.Visit));
      return target;
    }

    public static void InitConfig(Properties<string,string> config)
    {
      InitConfig(config, null, false);
    }

    public static void InitConfigPdxReadSerialized(Properties<string, string> dsProps, bool PdxReadSerialized)
    {
      InitConfig(dsProps, null, PdxReadSerialized);
    }

    public static void InitConfigPool(Properties<string,string> dsProps)
    {
      InitConfig(dsProps, null, false);
    }

    public static void InitConfig(string cacheXml)
    {
      InitConfig(null, cacheXml, false);
    }

    public static void InitConfig(Properties<string,string> config,
      string cacheXml, bool PdxReadSerialized)
    {
      string gfcppPropsFile = Util.AssemblyDir + "/gfcpp.properties";
      if (File.Exists(gfcppPropsFile))
      {
        Properties<string,string> newConfig = new Properties<string,string>();
        newConfig.Load(gfcppPropsFile);
        if (config != null)
        {
          newConfig.AddAll(config);
        }
        config = newConfig;
      }
      //ConnectConfig(dsName, config);
      if (m_cache == null || m_cache.IsClosed)
      {
        try
        {
          CacheHelper<TKey, TVal>.m_doDisconnect = false;

          CacheFactory cf = CacheFactory.CreateCacheFactory(config);

          if (cacheXml != null && cacheXml.Length > 0)
          {
            FwkInfo("seting cache-xml-file {0}", cacheXml);
            cf = cf.Set("cache-xml-file", cacheXml);
          }
          
          if (PdxReadSerialized)
          {
            FwkInfo("seting PdxReadSerialized {0}", PdxReadSerialized);
            cf = CacheFactory.CreateCacheFactory(config)
                .SetPdxReadSerialized(PdxReadSerialized);
          }
          
          m_cache = cf.Create();
        }
        catch (CacheExistsException)
        {
          m_cache = CacheFactory.GetAnyInstance();
        }
      }

      m_dsys = m_cache.DistributedSystem;
    }

    public static void InitConfigForPoolDurable(string durableClientId, int durableTimeout, string conflateEvents, bool isSslEnable)
    {
      Properties<string,string> config = new Properties<string,string>();
      config.Insert("durable-client-id", durableClientId);
      config.Insert("durable-timeout", durableTimeout.ToString());
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
      InitConfig(config, null,false);

    }
    public static void InitClient()
    {
      CacheHelper<TKey, TVal>.Close();
      Properties<string,string> config = new Properties<string,string>();
      CacheHelper<TKey, TVal>.InitConfig(config);
    }

    public static void Close()
    {
      if (DistributedSystem.IsConnected)
      {
        CloseCache();
        if (m_doDisconnect)
        {
          DistributedSystem.Disconnect();
        }
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
        if (m_doDisconnect)
        {
          DistributedSystem.Disconnect();
        }
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

    public static IRegion<TKey, TVal> CreateRegion(string name, RegionFactory attrs)
    {

      Init();
      DestroyRegion(name, true, false);
      IRegion<TKey, TVal> region = attrs.Create<TKey, TVal>(name);
      FwkAssert(region != null, "Region {0} was not created.", name);
      m_currRegion = region;
      return region;
    }
    
    public static void DestroyRegion(string name, bool local, bool verify)
    {
      IRegion<TKey, TVal> region;
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
          region.GetLocalView().DestroyRegion();
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
        IRegion<TKey, TVal>[] regions = m_cache.RootRegions<TKey, TVal>();
        if (regions != null)
        {
          foreach (IRegion<TKey, TVal> region in regions)
          {
            if (local)
            {
              region.GetLocalView().DestroyRegion();
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
   
    public static IRegion<TKey, TVal> GetRegion(string path)
    {

      if (m_cache != null)
      {
        return m_cache.GetRegion<TKey,TVal>(path);
      }
      return null;
    }
    
    public static IRegion<TKey, TVal> GetVerifyRegion(string path)
    {
      IRegion<TKey, TVal> region = GetRegion(path);
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

    public static string RegionTag(GemStone.GemFire.Cache.Generic.RegionAttributes<TKey, TVal> attrs)
    {
      string tag = string.Empty;
      tag += attrs.CachingEnabled ? "Caching" : "NoCache";
      tag += (attrs.CacheListener == null) ? "Nlstnr" : "Lstnr";
      return tag;
    }

    public static string RegionAttributesToString(GemStone.GemFire.Cache.Generic.RegionAttributes<TKey, TVal> attrs)
    {
      StringBuilder attrsSB = new StringBuilder();
      attrsSB.Append(Environment.NewLine + "caching: " +
        attrs.CachingEnabled);
      attrsSB.Append(Environment.NewLine + "pool: " +
        attrs.PoolName);
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
      attrsSB.AppendFormat("{0}cacheLoader: {1}",Environment.NewLine, attrs.CacheLoader != null ? "Enabled" : "Disabled");
      attrsSB.AppendFormat("{0}cacheWriter: {1}", Environment.NewLine,attrs.CacheWriter != null ? "Enabled" : "Disabled");
      attrsSB.AppendFormat("{0}cacheListener: {1}", Environment.NewLine, attrs.CacheListener != null ? "Enabled" : "Disabled");
      attrsSB.Append(Environment.NewLine + "concurrency-checks-enabled: " +
        attrs.ConcurrencyChecksEnabled);
      attrsSB.Append(Environment.NewLine);
      return attrsSB.ToString();
    }

    #endregion
  }
}
