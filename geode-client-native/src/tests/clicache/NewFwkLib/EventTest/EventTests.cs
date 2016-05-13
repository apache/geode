//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using System.Threading;
  using System.Xml.Serialization;
  using System.IO;
  using System.Reflection;

  public class EventTest : FwkTest
  {
    public const string EventCountersBB = "EventCountersBB";

    #region Private members

    private List<CacheableKey> m_keysVec = new List<CacheableKey>();

    #endregion

    #region Private utility methods

    private int percentDifferent()
    {
      string testRegionName = GetStringValue("regionName");
      if (testRegionName.Length == 0)
      {
        throw new FwkException("Data not provided for regionname failing");
      }
      int bbExpected = 0;
      int expected = GetUIntValue("expectedKeyCount");
      if (expected < 0)
      {
        bbExpected = (int)Util.BBGet(EventCountersBB, "CREATE_COUNT");
        if (bbExpected <= 0)
        {
          throw new FwkException("Data not provided for expectedKeyCount failing");
        }
        expected = bbExpected;
      }
      Region testReg = CacheHelper.GetRegion(testRegionName);
      IGFSerializable[] keys = testReg.GetKeys();
      int keyCount = keys.Length;
      double diff = 0;
      if (keyCount > expected)
      {
        diff = keyCount - expected;
      }
      else
      {
        diff = expected - keyCount;
      }
      int retval = (int)((diff / ((double)expected + 1.0)) * 100.0);
      FwkInfo("Expected to have " + expected + " keys, found " + keyCount + " keys, percent Difference: " + retval);
      return retval;
    }

    private void doEntryTest(string opcode)
    {
      FwkInfo("Calling doEntryTest inside getRandomregion");
      Region reg = GetRandomRegion(true);
      FwkInfo("After getrandomregion inside doentrytest");
      if (reg == null)
      {
        FwkInfo("Check in side doEntryTest ... No region to operate on");
        throw new FwkException("No reion to operate on");
      }

      lock (this)
      {
        FwkInfo("After getrandomregion inside doentrytest Check 1");
        if (reg.Attributes.CachingEnabled == false)
        {
          return;
        }
        IGFSerializable[] keys = reg.GetKeys();
        int maxkeys = GetUIntValue("distinctKeys");
        int balanceEntries = GetUIntValue("balanceEntries");
        FwkInfo("After getrandomregion inside doentrytest Check 2 balance entries = {0}", balanceEntries);
        if (balanceEntries > 0)
        {
          if (keys.Length >= maxkeys)
          {
            FwkInfo("Balancing cache content by destroying excess entries.");
            int dcnt = 0;
            for (uint i = 100; i > 0; i--)
            {
              try
              {
                destroyObject(reg, true);
              }
              catch (Exception e)
              {
                // ignore the exception ... TODO print the message
                FwkSevere(e.Message);
              }
            }
            FwkInfo("Cache balancing complete, did " + dcnt + " destroys.");
          }
          else if (keys.Length == 0)
          {
            FwkInfo("Balancing cache content by creating entries.");
            int ccnt = 0;
            for (int i = 100; i > 0; i--)
            {
              try
              {
                addObject(reg, true, null, null);
                ccnt++;
              }
              catch (TimeoutException e)
              {
                FwkSevere("Caught unexpected timeout exception during entry " + opcode
                  + " operation: " + e.Message + " continuing with test.");
              }
              catch (Exception e)
              {
                // Ignore other exception ... @TODO
                FwkInfo("Ignoring exception " + e.Message);
              }
            }
            FwkInfo("Cache balancing complete, did " + ccnt + " creates.");
          }
        }
        else
        {
          FwkInfo("Balance entries less than zero");
        }
      }
      FwkInfo("After getrandomregion inside doentrytest Check 4 before switch opcode = {0}", opcode);
      //opcode = "read";
      FwkInfo("opcode = " + opcode.ToString() + " " + opcode);


      switch (opcode)
      {
        case "add":
          addObject(reg, true, null, null);
          break;

        case "update":
          updateObject(reg);
          break;

        case "invalidate":
          invalidateObject(reg, false);
          break;

        case "destroy":
          destroyObject(reg, false);
          break;

        case "read":
          readObject(reg);
          break;

        case "localInvalidate":
          invalidateObject(reg, true);
          break;

        case "localDestroy":
          destroyObject(reg, true);
          break;

        default:
          FwkSevere("Invalid operation specified: " + opcode);
          break;

      }
    }

    private void doRegionTest(string opcode, int iMaxRegions)
    {
      Region randomRegion;
      lock (this)
      {
        int iRegionCount = getAllRegionCount();
        if (iRegionCount >= iMaxRegions)
        {
          while (iRegionCount > iMaxRegions / 2)
          {
            try
            {
              randomRegion = GetRandomRegion(true);
              if (randomRegion == null)
              {
                FwkException("expected to get a valid random region, get a null region instead");
              }
              else
              {
                destroyRegion(randomRegion, false);
              }
              iRegionCount = getAllRegionCount();
              FwkInfo("Inside doregiontest ... iregioncount = {0}", iRegionCount);
            }
            catch (Exception ignore)
            {
              FwkInfo(ignore.Message);
            }
          }
        }
        else if (iRegionCount <= 0)
        {
          for (Int32 i = iMaxRegions / 2; i > 0; i--)
          {
            try
            {
              addRegion();
            }
            catch (Exception ignore)
            {
              FwkInfo(ignore.Message);
            }
          }
        }
        FwkInfo("Inside doregiontest after else");
      }

      FwkInfo("Again GetRandomRegion");
      randomRegion = GetRandomRegion(true);
      if (randomRegion == null)
      {
        //need to create a region
        opcode = "addRegion";
      }
      FwkInfo("Do region test: " + opcode);
      switch (opcode)
      {
        case "addRegion":
          addRegion();
          break;
        case "clearRegion":
          clearRegion(randomRegion, false);
          break;

        case "invalidateRegion":
          invalidateRegion(randomRegion, false);
          break;

        case "destroyRegion":
          destroyRegion(randomRegion, false);
          break;

        case "localClearRegion":
          clearRegion(randomRegion, true);
          break;

        case "localInvalidateRegion":
          invalidateRegion(randomRegion, true);
          break;

        case "localDestroyRegion":
          destroyRegion(randomRegion, true);
          break;

        default:
          FwkSevere("Invalid operation specified: " + opcode);
          break;
      }
    }

    private string getNextRegionName(Region region)
    {
      string regionName = null;
      int count = 0;
      string path;
      do
      {
        path = GetStringValue("regionPaths");
        if (path.Length == 0)
        {
          FwkException("No regionPaths defined in the xml file. Needed for region event test");
        }
        do
        {
          int length = path.Length;
          try
          {
            region = CacheHelper.GetRegion(path);
          }
          catch (Exception e)
          {
            FwkSevere(e.Message);
          }
          if (region == null)
          {
            int pos = path.LastIndexOf('/');
            regionName = path.Substring(pos + 1, path.Length - pos);
            path = path.Substring(0, pos);
          }
        } while ((region == null) && path.Length == 0);
      } while ((++count < 5) && regionName.Length != 0);
      return regionName;
    }

    public void measureMemory(string s, double vs, double rs)
    {
    }

    public CacheableKey findKeyNotInCache(Region region)
    {
      CacheableKey key;
      if (m_keysVec.Count == 0)
      {
        lock (this)
        {
          int numKeys = GetUIntValue("distinctKeys");
          for (int i = 0; i < numKeys; i++)
          {
            string skey = i.ToString();
            key = new CacheableString(skey);
            //int pos = m_keysVec.Length;
            m_keysVec.Add(key);
          }
        }
      }
      key = null;

      int start = Util.Rand(m_keysVec.Count);
      bool wrapped = false;
      int cur = start;
      while ((cur != start) || !wrapped)
      {
        if (cur >= m_keysVec.Count)
        {
          cur = 0;
          wrapped = true;
        }
        else
        {
          if (!region.ContainsKey(m_keysVec[cur]))
          {
            key = m_keysVec[cur];
            cur = start;
            wrapped = true;
          }
          else
          {
            cur++;
          }
        }
      }
      return key;
    }

    #endregion

    #region Public methods

    public void DoEntryOperations()
    {
      Util.Log("Calling doeventoperations");
      doEventOperations();
    }


    public void doEventOperations()
    {
      UInt32 counter = 0;
      string taskID = "begin";
      // Clear up everything from previous test.
      // Make sure we have one root region.
      {
        // TODO: Lock and task id business
        // ACE_Guard<ACE_Thread_Mutex> guard( *testLock);
        //if ((taskID != null) && (this.getn != taskID))
        {
          // TODO
          CacheHelper.DestroyAllRegions(true);
          //destroyAllRegions();

          CreateRootRegion();
          if (taskID != null)
          {
            //(taskID);
          }
          //  FWKINFO( "DBG doEventOperations set id" );
          //taskID = strdup(getTaskId().c_str());
        }
        //  FWKINFO( "DBG doEventOperations release lock" );
      }
      int workTime = GetUIntValue("workTime");
      FwkInfo("doEventOperations will work for " + workTime + " seconds. ");

      int skipCounter = GetUIntValue("skipCount");
      skipCounter = (skipCounter > 0) ? skipCounter : 100;

      int iMaxRegions = GetUIntValue("maxRegions");
      // TODO: DEFAULT_MAX_REGION
      int DEFAULT_MAX_REGION = 10;
      iMaxRegions = (iMaxRegions > 0) ? iMaxRegions : DEFAULT_MAX_REGION;

      // TODO: check the correctness.
      DateTime endTime = DateTime.Now + TimeSpan.FromMilliseconds((double)workTime);

      int opsSecond = GetUIntValue("opsSecond");
      opsSecond = (opsSecond > 0) ? opsSecond : 0;
      PaceMeter pm = new PaceMeter(opsSecond);

      int logSize = GetUIntValue("logSize");

      int opCount = 0;

      DateTime now = new DateTime();
      string opcode = string.Empty;
      bool isDone = false;
      FwkInfo("Entering event loop.");
      do
      {
        FwkInfo("Before getRegionCount");
        if (logSize == 1)
        {
          int cnt = getRegionCount();
          FwkInfo(cnt + ((cnt == 1) ? " region " : " regions ") + opCount);
        }
        FwkInfo("After getRegionCount");
        int randomOP = GetUIntValue("randomOP");
        if (randomOP == 5)
        {
          opcode = GetStringValue("regionOps");
        }
        else
        {
          opcode = GetStringValue("entryOps");
        }
        FwkInfo("Check 1");
        if (opcode.Length != 0)
        {
          bool skipTest = false;
          if (opcode == "abort")
          {
            skipTest = true;
            if (--skipCounter == 0)
            {
              // TODO: definitely wrong. what is intended is unclear.
              //char * segv = NULL;
              //strcpy( segv, "Forcing segv" );
            }
          }
          else if (opcode == "exit")
          {
            skipTest = true;
            if (--skipCounter == 0)
            {
              Environment.Exit(0);
            }
          }
          else if (opcode == "done")
          {
            skipTest = true;
            if (--skipCounter == 0)
            {
              isDone = true;
            }
          }

          if (!skipTest)
          {
            FwkInfo("Check 2 doRegionTest");
            if (randomOP == 5)
            {
              doRegionTest(opcode, iMaxRegions);
            }
            else
            {
              FwkInfo("Check 3 doEntryTest");
              doEntryTest(opcode);
              FwkInfo("Check 4 doentrytest over");
            }
            opCount++;
            pm.CheckPace();
          }
          counter++;
          if ((counter % 1000) == 0)
          {
            FwkInfo("Performed " + counter + " operations.");
          }
          Util.BBIncrement(EventCountersBB, "CURRENT_OPS_COUNT");
        }
        else
        {
          FwkSevere("NULL operation specified." + "randomOP: " + randomOP);
        }
        now = DateTime.Now;
        FwkInfo("do while end in doeventoperations");
      } while ((now < endTime) && !isDone);

      FwkInfo("Event loop complete.");
      FwkInfo("doEventOperations() performed " + counter + " operations.");
    }

    public void doIterate()
    {
      FwkInfo("doIterate()");
      uint ulKeysInRegion = 0;
      uint ulNoneNullValuesInRegion = 0;
      string sError = null;

      Region[] rootRegionArray;
      Region rootRegion;
      RegionAttributes attr;

      rootRegionArray = CacheHelper.DCache.RootRegions();

      int ulRegionCount = rootRegionArray.Length;

      for (int ulIndex = 0; ulIndex < ulRegionCount; ulIndex++)
      {
        rootRegion = rootRegionArray[ulIndex];
        attr = rootRegion.Attributes;

        bool bHasInvalidateAction = attr.EntryIdleTimeoutAction == ExpirationAction.Invalidate ||
          (attr.EntryTimeToLiveAction == ExpirationAction.Invalidate);

        iterateRegion(rootRegion, true, bHasInvalidateAction, ulKeysInRegion, ulNoneNullValuesInRegion, sError);

        if (sError.Length > 0)
        {
          FwkException(sError);
        }
      }
    }

    public void doMemoryMeasurement()
    {
      // TODO Later

    }

    public void verifyKeyCount()
    {
      int percentDiff = percentDifferent();
      if (percentDiff > 10)
      {
        FwkSevere("Actual number of keys does not match expected number.");
      }
    }

    public void addEntry()
    {
      string testRegionName = GetStringValue("regionName");
      if (testRegionName.Length == 0)
      {
        FwkException("Data not provided for 'regionName', failing.");
      }
      Region region = CacheHelper.DCache.GetRegion(testRegionName);

      int usePid = GetUIntValue("usePID");
      int pid = Util.PID;

      int opsSecond = GetUIntValue("opsSecond");
      if (opsSecond < 0)
      {
        opsSecond = 0; // No throttle
      }
      PaceMeter pm = new PaceMeter(opsSecond);

      int entryCount = GetUIntValue("EntryCount");
      if (entryCount <= 0)
      {
        entryCount = 100;
      }
      FwkInfo("addEntry: Adding " + entryCount + " entries to the cache.");

      for (Int32 count = 0; count < entryCount; count++)
      {
        string sKey;
        Serializable sValue;

        if (usePid == 1)
        {
          sKey = pid.ToString();
        }
        else
        {
          sKey = string.Empty;
        }

        sKey += count.ToString();
        // get value size
        int vsize = GetUIntValue("valueSizes");
        if (vsize < 0)
        {
          vsize = 1000;
        }

        byte[] buffer = new byte[vsize];
        Util.RandBytes(buffer);
        sValue = CacheableBytes.Create(buffer);

        // TODO: check
        CacheableKey key = new CacheableString(sKey);
        Serializable value = sValue;

        if (key == null)
        {
          FwkSevere("EventTest::addObject null keyPtr generated.");
        }

        FwkInfo("created entry with key: " + key.ToString());
        region.Put(key as CacheableKey, value);
        Util.BBIncrement(EventCountersBB, "CREATE_COUNT");
        pm.CheckPace();
      }
      FwkInfo("addEntry: Complete.");
    }

    public void addOrDestroyEntry()
    {
      string testRegionName = GetStringValue("regionName");
      if (testRegionName.Length == 0)
      {
        FwkException("Data not provided for 'regionName', failing.");
      }
      Region region = CacheHelper.DCache.GetRegion(testRegionName);

      int usePid = GetUIntValue("usePID");
      int pid = Util.PID;

      int entryCount = GetUIntValue("EntryCount");
      if (entryCount <= 0)
      {
        entryCount = 100;
      }
      FwkInfo("addOrDestroyEntry: Adding or Destroying ( if present )" + entryCount + " entries to the cache.");
      for (int count = 0; count < entryCount; count++)
      {
        string sKey;

        if (usePid == 1)
        {
          sKey = pid.ToString();
        }
        else
        {
          sKey = string.Empty;
        }

        sKey += count.ToString();
        // get value size
        int vsize = GetUIntValue("valueSizes");
        if (vsize < 0)
        {
          vsize = 1000;
        }
        byte[] buffer = new byte[vsize];
        Util.RandBytes(buffer);
        CacheableKey key = new CacheableString(sKey);
        CacheableBytes value = CacheableBytes.Create(buffer);

        if (key == null)
        {
          FwkSevere("EventTest::addObject null keyPtr generated.");
        }

        string op = GetStringValue("popOp");
        if (op == "put")
        {
          region.Put(key as CacheableKey, value);
        }
        else
        {
          region.Destroy(key as CacheableKey);
        }

        Util.BBIncrement(EventCountersBB, "CREATE_COUNT");
      }
      FwkInfo("addOrDestroyEntry: Complete.");
    }

    public void validateCacheContent()
    {
      FwkInfo("validateCacheContent()");
      string testRegionName = GetStringValue("testRegion");
      string validateRegionName = GetStringValue("validateRegion");
      Region testRegion = CacheHelper.DCache.GetRegion(testRegionName);
      Region validateRegion = CacheHelper.DCache.GetRegion(validateRegionName);
      ICacheableKey[] keyVector;

      keyVector = testRegion.GetKeys();
      int ulKeysInRegion = keyVector.Length;
      if (ulKeysInRegion == 0)
      {
        FwkSevere("zero keys in testRegion " + testRegion.Name);
      }

      ICacheableKey key;
      IGFSerializable value;

      int entryPassCnt = 0;
      int entryFailCnt = 0;

      for (int ulIndex = 0; ulIndex < ulKeysInRegion; ulIndex++)
      {
        key = keyVector[ulIndex];
        value = testRegion.Get(key);

        if (TestEntryPropagation(validateRegion, key as CacheableString, value as CacheableBytes))
        {
          entryFailCnt++;
        }
        else
        {
          entryPassCnt++;
        }
      }
      FwkInfo("entryFailCnt is " + entryFailCnt + " entryPassCnt is " + entryPassCnt);
      if (entryFailCnt == 0)
      {
        FwkInfo("validateCacheContent() - TEST ENDED, RESULT = SUCCESSFUL ");
      }
      else
      {
        FwkSevere("validateCacheContent() - TEST ENDED, RESULT = FAILED ");
      }
    }

    public void validateRegionContent()
    {
      FwkInfo("validateRegionContent()");
      string testRegionName = GetStringValue("testRegion");
      string validateRegionName = GetStringValue("validateRegion");
      string regionName = GetStringValue("regionName");
      Region testRegion = CacheHelper.DCache.GetRegion(testRegionName);
      Region validateRegion = CacheHelper.DCache.GetRegion(validateRegionName);

      FwkInfo("localDestroyRegion region name is " + testRegion.Name);
      // destroy the region
      int iBeforeCounter = (int)Util.BBGet(EventCountersBB,
        "numAfterRegionDestroyEvents_isNotExp");

      testRegion.LocalDestroyRegion();
      CreateRootRegion();
      Region region = CacheHelper.DCache.GetRegion(regionName);

      FwkInfo(" Recreated Region name is " + region.Name);

      ICacheableKey[] keyVector;
      ICacheableKey[] keyVectorValidateRegion;

      keyVector = region.GetKeys();
      keyVectorValidateRegion = validateRegion.GetKeys();
      int ulKeysInRegion = keyVector.Length;
      int ulKeysInValidateRegion = keyVectorValidateRegion.Length;
      if (ulKeysInRegion != ulKeysInValidateRegion)
      {
        FwkSevere("Region Key count is not equal, Region " + region.Name + " key count is " + ulKeysInRegion + " and Region " +
           validateRegion.Name + " key count is " + ulKeysInValidateRegion);
      }

      ICacheableKey key;
      IGFSerializable value;

      int entryPassCnt = 0;
      int entryFailCnt = 0;
      for (int ulIndex = 0; ulIndex < ulKeysInRegion; ulIndex++)
      {
        key = keyVector[ulIndex];
        value = region.Get(key);

        if (TestEntryPropagation(validateRegion, key as CacheableString, value as CacheableBytes))
        {
          entryFailCnt++;
        }
        else
        {
          entryPassCnt++;
        }
      }
      FwkInfo("entryFailCnt is " + entryFailCnt + " entryPassCnt is " + entryPassCnt);
      if (entryFailCnt == 0)
      {
        FwkInfo("validateRegionContent() - TEST ENDED, RESULT = SUCCESSFUL ");
      }
      else
      {
        FwkSevere("validateRegionContent() - TEST ENDED, RESULT = FAILED ");
      }
    }

    public void doCreateObject()
    {
      // Not implemented.
    }

    public void doIterateOnEntry()
    {
      FwkInfo("doIterateOnEntry()");
      string testRegionName = GetStringValue("testRegion");
      string validateRegionName = GetStringValue("validateRegion");
      Region testRegion = CacheHelper.DCache.GetRegion(testRegionName);
      Region validateRegion = CacheHelper.DCache.GetRegion(validateRegionName);

      ICacheableKey[] keyVector = null;
      int keysInRegion = 1;
      int lastCount = 0;
      int tryCount = 30;
      int tries = 0;

      while ((keysInRegion != lastCount) && (tries++ < tryCount))
      {
        Thread.Sleep(10000); // sleep for 10 seconds.
        lastCount = keysInRegion;
        keyVector = testRegion.GetKeys();
        keysInRegion = keyVector.Length;
      }

      if ((keysInRegion == 0) || (tries >= tryCount))
      {
        FwkException("After " + tries + " tries, counted " + keysInRegion + " keys in the region.");
      }

      FwkInfo("After " + tries + " tries, counted " + keysInRegion + " keys in the region.");

      CacheableKey key;
      Serializable value;

      for (int index = 0; index < keysInRegion; index++)
      {
        key = keyVector[index] as CacheableKey;
        value = testRegion.Get(key) as Serializable;
        validateRegion.Create(key, value);
      }
    }

    public void feedEntries()
    {
      string testRegionName = GetStringValue("regionName");
      if (testRegionName.Length == 0)
      {
        FwkException("Data not provided for 'regionName', failing.");
      }
      Region region = CacheHelper.DCache.GetRegion(testRegionName);

      int opsSecond = GetUIntValue("opsSecond");
      if (opsSecond < 0)
      {
        opsSecond = 0; // No throttle
      }
      PaceMeter pm = new PaceMeter(opsSecond);

      int secondsToRun = GetUIntValue("workTime");
      secondsToRun = (secondsToRun < 1) ? 100 : secondsToRun;
      FwkInfo("feedEntries: Will add entries for " + secondsToRun + " seconds.");
      DateTime end = DateTime.Now + TimeSpan.FromSeconds((double)secondsToRun);
      DateTime now = DateTime.Now;

      int count = 0;
      while (now < end)
      {
        string key = (++count).ToString();

        // get value size
        int vsize = GetUIntValue("valueSizes");
        if (vsize < 0)
        {
          vsize = 1000;
        }
        byte[] buffer = new byte[vsize];
        Util.RandBytes(buffer);

        CacheableKey skey = new CacheableString(key);
        Serializable value = CacheableBytes.Create(buffer);

        if (key == null)
        {
          FwkSevere("EventTest::feedEntries null keyPtr generated.");
          now = end;
        }

        region.Put(skey, value);
        Util.BBIncrement(EventCountersBB, "CREATE_COUNT");
        pm.CheckPace();
        now = DateTime.Now;
      }
    }


    public void doBasicTest()
    {
      Region region = GetRandomRegion(true);
      int numKeys = GetUIntValue("distinctKeys");
      numKeys = numKeys > 0 ? numKeys : 1000;
      CacheableKey[] keys = new CacheableKey[numKeys];
      Serializable[] values = new Serializable[numKeys];

      for (int i = 0; i < numKeys; ++i)
      {
        int ksize = GetUIntValue("valueSizes");
        ksize = ksize > 0 ? ksize : 12;
        int vsize = GetUIntValue("valueSizes");
        vsize = vsize > 0 ? vsize : 100;

        string kStr = "key_";
        byte[] buffer = new byte[vsize];
        Util.RandBytes(buffer);
        CacheableKey key = new CacheableString(kStr);
        Serializable value = CacheableBytes.Create(buffer);
        keys[i] = key;
        values[i] = value;
        region.Create(key, value);
      }
      ICacheableKey[] expectKeys = region.GetKeys();

      if (expectKeys.Length != numKeys)
      {
        FwkSevere("Expect " + numKeys + " keys after create, got " + expectKeys.Length + " keys");
      }

      for (int i = 0; i < numKeys; ++i)
      {
        region.LocalInvalidate(keys[i]);
      }

      expectKeys = region.GetKeys();
      if (expectKeys.Length != numKeys)
      {
        FwkSevere("Expect " + numKeys + " keys after localInvalidate, got " + expectKeys.Length + " keys");
      }

      for (int i = 0; i < numKeys; ++i)
      {
        IGFSerializable val = region.Get(keys[i]);

        if (val.ToString() != values[i].ToString())
        {
          FwkSevere("Expect " + values[i].ToString() + ", got " + val.ToString());
        }
      }

      expectKeys = region.GetKeys();

      if (expectKeys.Length != numKeys)
      {
        FwkSevere("Expect " + numKeys + " keys after first get, got " + expectKeys.Length + " keys");
      }

      for (int i = 0; i < numKeys; ++i)
      {
        region.LocalDestroy(keys[i]);
      }

      expectKeys = region.GetKeys();
      if ((expectKeys.Length) != 0)
      {
        FwkSevere("Expect 0 keys after localDestroy, got " + expectKeys.Length + " keys");
      }

      for (int i = 0; i < numKeys; ++i)
      {
        IGFSerializable val = region.Get(keys[i]);         // get

        if (val.ToString() != values[i].ToString())
        {
          FwkSevere("Expect " + values[i].ToString() + ", got " + val.ToString());
        }
      }

      expectKeys = region.GetKeys();
      if (expectKeys.Length != numKeys)
      {
        FwkSevere("Expect " + numKeys + " keys after second get, got " + expectKeys.Length + " keys");
      }

      for (int i = 0; i < numKeys; ++i)
      {
        region.Invalidate(keys[i]);
      }

      expectKeys = region.GetKeys();
      if (expectKeys.Length != numKeys)
      {
        FwkSevere("Expect " + numKeys + " keys after invalidate, got " + expectKeys.Length + " keys");
      }

      for (int i = 0; i < numKeys; ++i)
      {
        region.Get(keys[i]);
      }

      expectKeys = region.GetKeys();
      if (expectKeys.Length != numKeys)
      {
        FwkSevere("Expect " + numKeys + " keys after invalidate all entries in server, got " + expectKeys.Length + " keys");
      }

      for (int i = 0; i < numKeys; ++i)
      {
        region.Put(keys[i], values[i]);
      }

      expectKeys = region.GetKeys();
      if (expectKeys.Length != numKeys)
      {
        FwkSevere("Expect " + numKeys + " keys after put, got " + expectKeys.Length + " keys");
      }

      for (int i = 0; i < numKeys; ++i)
      {
        region.Destroy(keys[i]);
      }

      expectKeys = region.GetKeys();
      if (expectKeys.Length != 0)
      {
        FwkSevere("Expect 0 keys after destroy, got " + expectKeys.Length + " keys");
      }

      int excepCount = 0;
      for (int i = 0; i < numKeys; ++i)
      {
        try
        {
          region.Get(keys[i]);
        }
        catch (EntryNotFoundException e)
        {
          FwkInfo(e.Message);
          ++excepCount;
        }
      }

      expectKeys = region.GetKeys();
      if (expectKeys.Length != 0)
      {
        FwkSevere("Expect 0 keys because all entries are destoyed in server, got " + expectKeys.Length + " keys");
      }

      if (excepCount != numKeys)
      {
        FwkSevere("Expect " + numKeys + " exceptions because all entries are destoyed in server, got " + excepCount + " exceptions");
      }

      for (int i = 0; i < numKeys; ++i)
      {
        region.Create(keys[i], values[i]);
      }

      expectKeys = region.GetKeys();
      if (expectKeys.Length != numKeys)
      {
        FwkSevere("Expect " + numKeys + " keys after second create, got " + expectKeys.Length + " keys");
      }

      for (int i = 0; i < numKeys; ++i)
      {
        region.Get(keys[i]);
      }

      expectKeys = region.GetKeys();
      if (expectKeys.Length != numKeys)
      {
        FwkSevere("Expect " + numKeys + " keys after invalidate all entries in server, got " + expectKeys.Length + " keys");
      }
    }


    public void doTwinkleRegion()
    {
      int secondsToRun = GetUIntValue("workTime");
      secondsToRun = (secondsToRun < 1) ? 10 : secondsToRun;
      FwkInfo("Seconds to run: " + secondsToRun);

      int end = DateTime.Now.Second + secondsToRun;
      bool done = false;
      bool regionDestroyed = false;
      int errCnt = 0;

      while (!done)
      {
        int sleepTime = GetUIntValue("sleepTime");
        sleepTime = ((sleepTime < 1) || regionDestroyed) ? 10 : sleepTime;
        FwkInfo("sleepTime is " + sleepTime + " seconds.");

        DateTime now = DateTime.Now;
        // TODO: 10 magic number to avoid compilation
        if ((now.Second > end) || ((now.Second + 10) > end))
        {
          // TODO : Check DateTime usage in the entire file.
          // FWKINFO( "Exiting loop, time is up." );
          done = true;
          continue;
        }

        FwkInfo("EventTest::doTwinkleRegion() sleeping for " + sleepTime + " seconds.");
        Thread.Sleep(sleepTime * 1000);

        if (regionDestroyed)
        {
          FwkInfo("EventTest::doTwinkleRegion() will create a region.");
          CreateRootRegion();
          regionDestroyed = false;
          FwkInfo("EventTest::doTwinkleRegion() region created.");
          int percentDiff = percentDifferent();
          if (percentDiff > 10)
          {
            errCnt++;
            FwkSevere("Actual number of keys is not within 10% of expected.");
          }
        }
        else
        {
          FwkInfo("EventTest::doTwinkleRegion() will destroy a region.");
          Region region = GetRandomRegion(true);
          if (region != null)
          {
            region.LocalDestroyRegion();
            region = null;
          }
          regionDestroyed = true;
          FwkInfo("EventTest::doTwinkleRegion() local region destroy is complete.");
        }
      } // while

      if (regionDestroyed)
      {
        CreateRootRegion();
        FwkInfo("EventTest::doTwinkleRegion() region created.");
      }

      FwkInfo("EventTest::doTwinkleRegion() completed.");
      if (errCnt > 0)
      {
        FwkException("Region key count was out of bounds on " + errCnt + " region creates.");
      }
    }

    // TODO Entire method check.
    public void checkTest(string taskId)
    {
      // TODO: For lock
      // SpinLockGuard guard( m_lck );
      // TODO: setTask(taskId)
      if (CacheHelper.DCache == null)
      {
        Properties pp = new Properties();
        //TODO: Initialize? cacheInitialize( pp );
        //string val = getStringValue( "EventBB" );
        //if ( !val.empty() )
        //{
        //  m_sEventBB = val;
        //}

      }
    }

    public void createRootRegion(string regionName)
    {
      FwkInfo("In createRootRegion region");
      Region rootRegion;

      if (regionName == null)
      {
        rootRegion = CreateRootRegion();
      }
      else
      {
        rootRegion = CacheHelper.CreateRegion(regionName, null);
      }

      Util.BBIncrement(EventCountersBB, rootRegion.FullPath);
      Util.BBIncrement(EventCountersBB, "ROOT_REGION_COUNT");

      FwkInfo("In createRootRegion, Created root region: " + rootRegion.FullPath);
    }

    public bool TestEntryPropagation(Region region, CacheableString szKey, CacheableBytes szValue)
    {
      bool bEntryError = false;
      bool bContainsKey = false;
      bool bContainsValue = false;

      bContainsKey = region.ContainsKey(szKey);
      bContainsValue = region.ContainsValueForKey(szKey);

      if (!bContainsKey || !bContainsValue)
      {
        FwkSevere("Key: " + szKey.Value + " not found in region " +
          region.FullPath + ", mirroring is enabled");
        bEntryError = true;
      }
      return bEntryError;
    }

    public void addObject(Region region, bool bLogAddition, string pszKey, string pszValue)
    {
      CacheableKey key;
      if (pszKey == null)
      {
        key = findKeyNotInCache(region);
      }
      else
      {
        key = new CacheableString(pszKey);
      }

      if (key == null)
      {
        FwkInfo("EventTest::addObject null key generated for " + pszKey);
        return;
      }

      Serializable value;

      if (pszValue == null)
      {
        int vsize = GetUIntValue("valueSizes");
        if (vsize < 0)
        {
          vsize = 1000;
        }
        byte[] buffer = new byte[vsize];
        Util.RandBytes(buffer);
        value = CacheableBytes.Create(buffer);
      }
      else
      {
        value = new CacheableString(pszValue);
      }

      if (value == null)
      {
        FwkInfo("EventTest::addObject null valuePtr generated.");
        return;
      }

      region.Create(key, value);
      Util.BBIncrement(EventCountersBB, "CREATE_COUNT");
    }

    public void invalidateObject(Region randomRegion, bool bIsLocalInvalidate)
    {
      CacheableKey keyP = getKey(randomRegion, false);
      if (keyP == null)
      {
        Util.BBIncrement(EventCountersBB, "OPS_SKIPPED_COUNT");
        return;
      }

      if (bIsLocalInvalidate)
      {
        randomRegion.LocalInvalidate(keyP);
        Util.BBIncrement(EventCountersBB, "LOCAL_INVALIDATE_COUNT");
      }
      else
      {
        randomRegion.Invalidate(keyP);
        Util.BBIncrement(EventCountersBB, "INVALIDATE_COUNT");
      }
    }

    public void destroyObject(Region randomRegion, bool bIsLocalDestroy)
    {
      FwkInfo("EventTest::destroyObject");

      CacheableKey keyP = getKey(randomRegion, true);

      if (keyP == null)
      {
        Util.BBIncrement(EventCountersBB, "OPS_SKIPPED_COUNT");
        return;
      }

      if (bIsLocalDestroy)
      {
        randomRegion.LocalDestroy(keyP);
        Util.BBIncrement(EventCountersBB, "LOCAL_DESTROY_COUNT");
      }
      else
      {
        randomRegion.Destroy(keyP);
        Util.BBIncrement(EventCountersBB, "DESTROY_COUNT");
      }
    }

    public void updateObject(Region randomRegion)
    {
      CacheableKey keyP = getKey(randomRegion, true);
      if (keyP == null)
      {
        FwkInfo("EventTest::updateObject key is null");
        Util.BBIncrement(EventCountersBB, "OPS_SKIPPED_COUNT");
        return;
      }

      IGFSerializable anObj = randomRegion.Get(keyP);

      int vsize = GetUIntValue("valueSizes");
      if (vsize < 0)
      {
        vsize = 1000;
      }

      byte[] buffer = new byte[vsize];
      Util.RandBytes(buffer);

      CacheableBytes newObj = CacheableBytes.Create(buffer);

      randomRegion.Put(keyP, newObj);
    }

    public void readObject(Region randomRegion)
    {
      FwkInfo("Inside readObject randomregion = {0}", randomRegion.FullPath);
      CacheableKey keyP = getKey(randomRegion, true);

      FwkInfo("After getkey");
      if (keyP == null)
      {
        Util.BBIncrement(EventCountersBB, "OPS_SKIPPED_COUNT");
        FwkInfo("skipped and returned");
        return;
      }
      FwkInfo("skipped and returned before Get");
      CacheableBytes anObj = randomRegion.Get(keyP) as CacheableBytes;
      FwkInfo("got anobj");
      //byte[] b = anObj.Value;
      //FwkInfo("byte array = " + b.ToString());
    }

    public void addRegion()
    {
      Region parentRegion = null;
      string sRegionName = getNextRegionName(parentRegion);
      if (sRegionName.Length == 0)
      {
        // nothing to do
        return;
      }

      Region region;
      FwkInfo("In addRegion, enter create region " + sRegionName);
      if (parentRegion == null)
      {
        // TODO Is this right.
        region = CacheHelper.CreateRegion(sRegionName, null);
      }
      else
      {

        string fullName = parentRegion.FullPath;
        RegionAttributes atts = parentRegion.Attributes;
        AttributesFactory fact = new AttributesFactory(atts);
        atts = fact.CreateRegionAttributes();
        region = parentRegion.CreateSubRegion(sRegionName, atts);
        Util.BBSet(EventCountersBB, sRegionName, fullName);
      }

      int iInitRegionNumObjects = GetUIntValue("initRegionNumObjects");
      // Create objects in the new region
      for (int iIndex = 0; iIndex < iInitRegionNumObjects; iIndex++)
      {
        string skey = iIndex.ToString();
        addObject(region, true, skey, null);
      }
      FwkInfo("In addRegion, exit create region " + sRegionName);
    }
    public void clearRegion(Region randomRegion, bool bIsLocalClear)
    {
      int iSubRegionCount = 0;
      // invalidate the region
      iSubRegionCount = getSubRegionCount(randomRegion) + 1;

      //bbGet("EventCountersBB", // TODO
      //  "numAfterRegionInvalidateEvents_isNotExp", &iBeforeCounter);
      FwkInfo("In clearRegion, enter clear region " + randomRegion.Name);

      string pszCounterName = "LOCAL_REGION_CLEAR_COUNT";
      if (bIsLocalClear)
      {
        randomRegion.LocalClear();
      }
      else
      {
        pszCounterName = "REGION_CLEAR_COUNT";
        randomRegion.Clear();
      }
      Util.BBAdd(EventCountersBB, pszCounterName, iSubRegionCount);
      FwkInfo("In clearRegion, exit invalidate region " + randomRegion.Name);
    }

    public void invalidateRegion(Region randomRegion, bool bIsLocalInvalidate)
    {
      int iSubRegionCount = 0;
      // invalidate the region
      iSubRegionCount = getSubRegionCount(randomRegion) + 1;

      //bbGet("EventCountersBB", // TODO
      //  "numAfterRegionInvalidateEvents_isNotExp", &iBeforeCounter);
      FwkInfo("In invalidateRegion, enter invalidate region " + randomRegion.Name);

      string pszCounterName = "LOCAL_REGION_INVALIDATE_COUNT";
      if (bIsLocalInvalidate)
      {
        randomRegion.LocalInvalidateRegion();
      }
      else
      {
        pszCounterName = "REGION_INVALIDATE_COUNT";
        randomRegion.InvalidateRegion();
      }
      Util.BBAdd(EventCountersBB, pszCounterName, iSubRegionCount);
      FwkInfo("In invalidateRegion, exit invalidate region " + randomRegion.Name);
    }

    public void destroyRegion(Region randomRegion, bool bIsLocalDestroy)
    {
      int iSubRegionCount = 0;
      // destroy the region
      //int iBeforeCounter = -1;
      //bbGet( "EventCountersBB", "numAfterRegionDestroyEvents_isNotExp", &iBeforeCounter );//TODO

      iSubRegionCount = getSubRegionCount(randomRegion) + 1;
      string pszCounterName = "LOCAL_REGION_DESTROY_COUNT";
      FwkInfo("In destroyRegion, enter destroy region " + randomRegion.Name);
      if (bIsLocalDestroy)
      {
        randomRegion.LocalDestroyRegion();
      }
      else
      {
        pszCounterName = "REGION_DESTROY_COUNT";
        randomRegion.DestroyRegion();
      }
      Util.BBIncrement(EventCountersBB, pszCounterName);
      FwkInfo("In destroyRegion, exit destroy region " + randomRegion.Name);
    }

    public Region GetRandomRegion(bool bAllowRootRegion)
    {
      FwkInfo("Inside GetRandomRegion ... Check 1");
      Region[] rootRegionVector = CacheHelper.DCache.RootRegions();
      int irootSize = rootRegionVector.Length;

      Region[] subRegionVector;
      FwkInfo("Inside GetRandomRegion ... Check 2");
      int iRootSize = rootRegionVector.Length;

      if (iRootSize == 0)
      {
        // TODO
        return null;
        //return RegionPtr();
      }
      FwkInfo("Inside GetRandomRegion ... Check 3 and irrotsize = {0}", irootSize);
      Region[] choseRegionVector = new Region[1];

      // if roots can be chosen, add them to candidates
      if (bAllowRootRegion)
      {
        for (int iRootIndex = 0; iRootIndex < iRootSize; iRootIndex++)
        {
          FwkInfo("Inside GetRandomRegion ... Check 4.{0}", iRootIndex);
          choseRegionVector[iRootIndex] = rootRegionVector[iRootIndex];
        }
      }

      FwkInfo("Inside GetRandomRegion ... Check 4");
      // add all subregions
      for (int iRootIndex = 0; iRootIndex < iRootSize; iRootIndex++)
      {
        subRegionVector = rootRegionVector[iRootIndex].SubRegions(true);
        int iSubSize = subRegionVector.Length;
        for (int iSubIndex = 0; iSubIndex < iSubSize; iSubIndex++)
        {
          choseRegionVector[choseRegionVector.Length] = subRegionVector[iSubIndex];
          //choseRegionVector.push_back(subRegionVector.at(iSubIndex));
        }
      }

      FwkInfo("Inside GetRandomRegion ... Check 5");
      int iChoseRegionSize = choseRegionVector.Length;
      if (iChoseRegionSize == 0)
      {
        // TODO
        return null;
        //return RegionPtr();
      }

      FwkInfo("Inside GetRandomRegion ... Check 6");
      int idx = Util.Rand(iChoseRegionSize);
      //string regionName = choseRegionVector.at(idx)->getFullPath();
      FwkInfo("Inside GetRandomRegion ... Check 7");
      return choseRegionVector[idx];
    }


    public void handleExpectedException(Exception e)
    {
      FwkInfo("Caught and ignored: " + e.Message);
    }

    public void verifyObjectInvalidated(Region region, CacheableKey key)
    {
      if ((region == null) && (key == null))
      {
        return;
      }
      string error = null;

      if (!region.ContainsKey(key))
      {
        error = "unexpected contains key";
      }

      if (region.ContainsValueForKey(key))
      {
        error = "Unexpected containsValueForKey ";
      }

      RegionEntry entry = region.GetEntry(key);

      if (entry == null)
      {
        error = "getEntry returned null";
      }
      else
      {
        if (entry.Key != key)
        {
          error = "Keys are different";
        }

        if (entry.Value != null)
        {
          error = "Expected value to be null";
        }
      }

      if (error.Length != 0)
      {
        FwkException(error);
      }
    }

    public void verifyObjectDestroyed(Region region, CacheableKey key)
    {
      if ((region == null) && (key == null))
      {
        return;
      }

      string osError;
      bool bContainsKey = region.ContainsKey(key);
      if (bContainsKey)
      {
        // TODO key.ToString()
        osError = "Unexpected containsKey " + bContainsKey +
        " for key " + key.ToString() + " in region " + region.FullPath +
        Environment.NewLine;
      }

      bool bContainsValueForKey = region.ContainsValueForKey(key);
      if (bContainsValueForKey)
      {
        osError = "Unexpected containsValueForKey " + bContainsValueForKey +
          " for key " + key.ToString() + " in region " + region.FullPath +
          Environment.NewLine;
      }

      RegionEntry entry = region.GetEntry(key);
      // TODO ... see this section
      //if (entry != null)
      //{
      //  CacheableString entryKey = key.;
      //  CacheableBytes entryValuePtr = entryPtr->getValue();

      //  osError << "getEntry for key " << CacheableStringPtr( keyPtr )->asChar() <<
      //" in region " << regionPtr->getFullPath() <<
      //" returned was non-null; getKey is " << entryKeyPtr->asChar() <<
      //", value is " << entryValuePtr->bytes() << "\n";
      //}

      //if (sError.size() > 0)
      //{
      //  FWKEXCEPTION(sError);
      //}
    }


    public void iterateRegion(Region aRegion, bool bAllowZeroKeys, bool bAllowZeroNonNullValues,
      uint ulKeysInRegion, uint ulNoneNullValuesInRegion,
      string sError)
    {
      if (aRegion == null)
      {
        return;
      }

      ulKeysInRegion = 0;
      ulNoneNullValuesInRegion = 0;

      ICacheableKey[] keyVector = aRegion.GetKeys();

      ulKeysInRegion = (uint)keyVector.Length;
      if (ulKeysInRegion == 0)
      {
        if (!bAllowZeroKeys)
        {
          sError = "Region " + aRegion.FullPath + " has " +
            ulKeysInRegion + " keys" + Environment.NewLine;
        }
      }

      CacheableKey key = null;
      IGFSerializable value = null;

      for (uint ulIndex = 0; ulIndex < ulKeysInRegion; ulIndex++)
      {
        key = keyVector[ulIndex] as CacheableKey;

        try
        {
          value = aRegion.Get(key);
        }
        catch (CacheLoaderException e)
        {
          FwkException("CacheLoaderException " + e.Message);
        }
        catch (TimeoutException e)
        {
          FwkException("TimeoutException " + e.Message);
        }

        if (value != null)
        {
          ulNoneNullValuesInRegion++;
        }
      }

      if (ulNoneNullValuesInRegion == 0)
      {
        if (!bAllowZeroNonNullValues)
        {
          sError += "Region " + aRegion.FullPath + " has " +
            ulNoneNullValuesInRegion + " non-null values" + Environment.NewLine;
        }
      }
    }


    public int getSubRegionCount(Region region)
    {
      Region[] subregions = region.SubRegions(true);
      return subregions.Length;
    }

    public int getAllRegionCount()
    {
      if (CacheHelper.DCache == null)
      {
        FwkSevere("Null cache pointer, no connection established.");
        return 0;
      }
      Region[] rootRegions = CacheHelper.DCache.RootRegions();
      int iRootSize = rootRegions.Length;
      int iTotalRegions = iRootSize;

      for (int iIndex = 0; iIndex < iRootSize; iIndex++)
      {
        // TODO getSubRegionCount implementation
        iTotalRegions += getSubRegionCount(rootRegions[iIndex]);
      }
      return iTotalRegions;
    }

    public CacheableKey getKey(Region region, bool bInvalidOK)
    {
      FwkInfo("random key check 1");
      //int randomKey = int.Parse((string)Util.ReadObject("randomKey"));
      int randomKey = GetUIntValue("randomKey");
      CacheableKey keyP = null;
      FwkInfo("random key check 2 ... randomkey = {0}", randomKey);
      if (randomKey > 0)
      {
        string sKey = randomKey.ToString();
        keyP = new CacheableString(sKey);
        FwkInfo("random key check 2.1 .. keyP.tostring = {0}", keyP.ToString());
        return keyP;
      }
      FwkInfo("random key check 3");

      ICacheableKey[] keys = region.GetKeys();
      int iKeySize = keys.Length;
      if (iKeySize == 0)
      {
        return keyP;
      }
      FwkInfo("random key check 4");
      int iStartAt = Util.Rand(iKeySize);
      if (bInvalidOK)
      {
        return keys[iStartAt] as CacheableKey;
      }
      int iKeyIndex = iStartAt;
      do
      {
        FwkInfo("random key check 5");
        bool hasValue = region.ContainsValueForKey(keys[iKeyIndex]);
        if (hasValue)
        {
          return keys[iKeyIndex] as CacheableKey;
        }
        iKeyIndex++;
        if (iKeyIndex >= iKeySize)
        {
          iKeyIndex = 0;
        }
      } while (iKeyIndex != iStartAt);

      FwkInfo("getKey: All values invalid in region");
      return keyP;
    }

    public void setEventError(string pszMsg)
    {
      Util.BBSet(EventCountersBB, "EventErrorMessage", pszMsg);
    }

    public void removeRegion(Region region)
    {
      string name = region.FullPath;
      FwkInfo("In removeRegion, local destroy on " + name);
      region.LocalDestroyRegion();
      Util.BBDecrement(EventCountersBB, name);
    }

    public Int32 getRegionCount()
    {
      FwkInfo("Check 1.1 Inside getRegionCount");
      Region[] roots = CacheHelper.DCache.RootRegions();
      FwkInfo("Check 1.1 root region count = {0}", roots.Length);
      return roots.Length;
    }

    #endregion

    #region Callback create methods
    public static ICacheWriter CreateETCacheWriter()
    {
      return new ETCacheWriter();
    }
    public static ICacheLoader CreateETCacheLoader()
    {
      return new ETCacheLoader();
    }

    public static ICacheListener CreateETCacheListener()
    {
      return new ETCacheListener();
    }

    #endregion
  }
}
