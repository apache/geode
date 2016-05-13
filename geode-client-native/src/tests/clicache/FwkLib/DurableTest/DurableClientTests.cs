//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Text;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  public class DurableClientTests : PerfTests
  {
    #region Private constants

    private const string KeepAlive = "keepAlive";
    private const string IsDurable = "isDurable";
    private const string RestartTime = "restartTime";
    private const string UpdateReceiveTime = "updateReceiveTime";
    private const string SleepTime = "sleepTime";
    private const string RegisterType = "registerType";
    private const string MissedEvents = "missedEvents";
    private const string ValueStart = "valueStart";
    private const string ValueEnd = "valueEnd";

    private static ICacheListener g_listener = null;
    private static ICacheListener g_perflistener = null;
    //private static bool isReady = false;

    #endregion

    #region Public methods

    //bool isReady = false;

    public static ICacheListener CreateDurableCacheListener()
    {
      g_listener = new DurableListener();
      return g_listener;
    }
    public static new ICacheListener CreateConflationTestCacheListener()
    {
      g_listener = new ConflationTestCacheListenerDC();
      return g_listener;
    }
    public static ICacheListener CreateDurablePerfListener()
    {
      g_perflistener = new DurablePerfListener();
      return g_perflistener;
    }

    public void DoIncrementalPuts()
    {
      FwkInfo("In DoIncrementalPuts()");
      try
      {
        Region region = GetRegion();

        string keyType = GetStringValue(KeyType); // int is only type to use
        int keySize = GetUIntValue(KeySize);
        int numKeys = GetUIntValue(DistinctKeys);
        int valStart = GetUIntValue(ValueStart);
        int valEnd = GetUIntValue(ValueEnd);

        // validate the above tags

        char typ = keyType[0] == 'i' ? 'i' : 's';

        int currVal = valStart > 1 ? valStart : 1;

        if (numKeys <= 0)
        {
          FwkSevere("Error reading distinctKeys");
        }

        if (valEnd <= 0)
        {
          FwkSevere("Error reading valueEnd");
        }

        int keyIndex = 0;

        while (true)
        {
          if (keyIndex == numKeys)
          {
            keyIndex = 1; // start over
          }
          else
          {
            keyIndex++; // next key
          }

          if (typ == 'i')
          {
            CacheableInt32 key = new CacheableInt32(keyIndex);
            CacheableInt32 value = new CacheableInt32(currVal);
            region.Put(key, value);
          }
          else
          {
            keySize = keySize > 0 ? keySize : 10;
            string keyBase = new string('A', keySize);
            string keyFull = String.Format("{0}{1}", keyBase, keyIndex.ToString("D10"));
            CacheableString key = new CacheableString(keyFull);
            CacheableInt32 value = new CacheableInt32(currVal);
            region.Put(key, value);
          }

          if (++currVal > valEnd)
          {
            break;
          }

          if (currVal % 1000 == 1)
          {
            FwkInfo("DurableClientTest: Putting {0}st...", currVal);
          }
        }
      }
      catch (Exception ex)
      {
        FwkSevere("DoIncrementalPuts() Caught Exception: {0}", ex);
        throw;
      }
      FwkInfo("DoIncrementalPuts() complete.");
    }

    public void DoLogDurablePerformance()
    {
      DurablePerfListener perflistener = g_perflistener as DurablePerfListener;
      if (perflistener != null)
      {
        FwkInfo("Logging durable performance.");
        perflistener.logPerformance();
      }
    }

    public void DoDurableCloseCache()
    {
      FwkInfo("In DoDurableCloseCache()");
      try
      {
        bool keepalive = GetBoolValue(KeepAlive);
        bool isDurable = GetBoolValue(IsDurable);
        FwkInfo("KeepAlive is {0}, isDurable is {1}", keepalive, isDurable);
        if (isDurable && keepalive)
        {
          CacheHelper.CloseKeepAlive();
        }
        else
        {
          CacheHelper.Close();
        }
      }
      catch (Exception ex)
      {
        FwkSevere("DoDurableCloseCache() Caught Exception: {0}", ex);
        throw;
      }
      FwkInfo("DoDurableCloseCache() complete.");
    }

    public override void DoCreateRegion()
    {
      FwkInfo("In DoCreateRegion() Durable");

      ClearCachedKeys();

      string rootRegionData = GetStringValue("regionSpec");
      string tagName = GetStringValue("TAG");
      string endpoints = Util.BBGet(JavaServerBB, EndPointTag + tagName)
        as string;
      if (rootRegionData != null && rootRegionData.Length > 0)
      {
        string rootRegionName;
        rootRegionName = GetRegionName(rootRegionData);
        if (rootRegionName != null && rootRegionName.Length > 0)
        {
          Region region;
          if ((region = CacheHelper.GetRegion(rootRegionName)) == null)
          {
            try
            {
              bool isDC = GetBoolValue("isDurable");
              string m_isPool = null;
               // Check if this is a thin-client region; if so set the endpoints
              int redundancyLevel = 0;
              if (endpoints != null && endpoints.Length > 0)
              {
                redundancyLevel = GetUIntValue(RedundancyLevelKey);
                if (redundancyLevel < 0)
                  redundancyLevel = 0;
                if (redundancyLevel < 0)
                  redundancyLevel = 0;
                string conflateEvents = GetStringValue(ConflateEventsKey);
                string durableClientId = "";
                int durableTimeout = 300;
                if (isDC)
                {
                  durableTimeout = GetUIntValue("durableTimeout");
                  bool isFeeder = GetBoolValue("isFeeder");
                  if (isFeeder)
                  {
                    durableClientId = "Feeder";
                    // VJR: Setting FeederKey because listener cannot read boolean isFeeder
                    // FeederKey is used later on by Verify task to identify feeder's key in BB
                    Util.BBSet("DURABLEBB", "FeederKey", "ClientName_" + Util.ClientNum + "_Count");
                  }
                  else
                  {
                    durableClientId = String.Format("ClientName_{0}", Util.ClientNum);
                  }
                }
                FwkInfo("DurableClientID is {0} and DurableTimeout is {1}", durableClientId, durableTimeout);
                ResetKey("sslEnable");
                bool isSslEnable = GetBoolValue("sslEnable");
                CacheHelper.InitConfigForPoolDurable(durableClientId, durableTimeout, conflateEvents, isSslEnable);
                string mode = Util.GetEnvironmentVariable("POOLOPT");
              }
              RegionFactory rootAttrs = CacheHelper.DCache.CreateRegionFactory(RegionShortcut.PROXY);
              SetRegionAttributes(rootAttrs, rootRegionData, ref m_isPool);
              rootAttrs = CreatePool(rootAttrs, redundancyLevel);
              region = CacheHelper.CreateRegion(rootRegionName, rootAttrs);
              RegionAttributes regAttr = region.Attributes;
              FwkInfo("Region attributes for {0}: {1}", rootRegionName,
                CacheHelper.RegionAttributesToString(regAttr));

            }
            catch (Exception ex)
            {
              FwkSevere("Caught exception {0}",ex);
            }
          }
        }
        else
        {
          FwkSevere("DoCreateRegion() failed to create region");
        }
      }

      FwkInfo("DoCreateRegion() complete.");
    }

    public void DoCloseCacheAndReInitialize()
    {
      FwkInfo("In DoCloseCacheAndReInitialize()");

      DoDurableCloseCache();

      //isReady = false;
      DoRestartClientAndRegInt();

      FwkInfo("DoCloseCacheAndReInitialize() complete.");
    }

    public void DoRestartClientAndRegInt()
    {
      FwkInfo("In DoRestartClientAndRegInt()");

      ClearCachedKeys();

      int sleepSec = GetUIntValue(RestartTime);
      if (sleepSec > 0)
      {
        Thread.Sleep(sleepSec * 1000);
        FwkInfo("Sleeping for {0} seconds before DoCreateRegion()", sleepSec);
      }      

      DoCreateRegion();

      string regType = GetStringValue(RegisterType);

      if (regType == "All")
      {
        DoRegisterAllKeys();
      }
      else if (regType == "List")
      {
        DoRegisterInterestList();
      }
      else
      {
        DoRegisterRegexList();
      }
      //isReady = true;
      int waitSec = GetUIntValue(UpdateReceiveTime);
      if (waitSec > 0)
      {
        Thread.Sleep(waitSec * 1000);
        FwkInfo("Sleeping for {0} seconds after DoCreateRegion()", sleepSec);
      }

      FwkInfo("DoRestartClientAndRegInt() complete.");
    }

    public void DoVerifyEventCount()
    {
      FwkInfo("In DoVerifyEventCount()");

      try
      {
        bool failed = false;

        string name = String.Format("ClientName_{0}", Util.ClientNum);
        string operkey = name + "_Count";
        //string feedkey = "Feeder_Count";
        string feedkey = (string) Util.BBGet("DURABLEBB", "FeederKey");
        Int32 currcnt = 0;
        Int32 feedcnt = 0;

        try
        {
          currcnt = (Int32)Util.BBGet("DURABLEBB", operkey);
        }
        catch (KeyNotFoundException)
        {
          FwkInfo("Key not found for DURABLEBB {0}", operkey);
        }

        try
        {
          feedcnt = (Int32)Util.BBGet("DURABLEBB", feedkey);
        }
        catch (KeyNotFoundException)
        {
          FwkInfo("Key not found for DURABLEBB {0}", feedkey);
        }

        FwkInfo("DoVerifyEventCount: Feeder is " + feedcnt + ", Actual is " + currcnt);

        bool missedEvents = GetBoolValue(MissedEvents);

        if (!missedEvents && currcnt != feedcnt)
        {
          string clientErrorKey = name + "_ErrMsg";
          string errorMsg = null;
          try
          {
            errorMsg = (string)Util.BBGet("DURABLEBB", clientErrorKey);
          }
          catch (KeyNotFoundException)
          {
            errorMsg = "unknown";
            FwkInfo("Key not found for DURABLEBB {0}", clientErrorKey);
          }
          FwkSevere("DoVerifyEventCount: Failed. # Missed = {0}, Err: {1}", feedcnt-currcnt, errorMsg);
          failed = true;
        }
        else if (missedEvents && currcnt > feedcnt)
        {
          FwkSevere("DoVerifyEventCount: Failed: Few events should have been missed.");
          failed = true;
        }

        FwkAssert(!failed, "DoVerifyEventCount() verification failed");
      }
      catch (Exception ex)
      {
        FwkSevere("DoVerifyEventCount() Caught Exception: {0}", ex);
        throw;
      }

      FwkInfo("DoVerifyEventCount() complete.");
    }

    public void DoDummyTask()
    {
      FwkInfo("DoDummyTask() called.");
    }

    #endregion
  }
}
