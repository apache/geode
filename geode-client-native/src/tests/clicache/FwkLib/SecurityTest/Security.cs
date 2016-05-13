//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.Cache.Tests;
  using GemStone.GemFire.DUnitFramework;

  public class Security : PerfTests
  {

    #region Protected constants

    protected const string EntryCount = "entryCount";
    protected const string WorkTime = "workTime";
    protected const string ObjectType = "objectType";
    protected const string EntryOps = "entryOps";
    protected const string LargeSetQuery = "largeSetQuery";
    protected const string UnsupportedPRQuery = "unsupportedPRQuery";

    #endregion

    #region Utility methods

    private CacheableKey GetKey(int max)
    {
      ResetKey(ObjectType);
      string objectType = GetStringValue(ObjectType);
      QueryHelper qh = QueryHelper.GetHelper();
      int numSet = 0;
      int setSize = 0;
      if (objectType != null && objectType == "Portfolio")
      {
        setSize = qh.PortfolioSetSize;
        numSet = max / setSize;
        return new CacheableString(String.Format("port{0}-{1}",
          Util.Rand(numSet), Util.Rand(setSize)));
      }
      else if (objectType != null && objectType == "Position")
      {
        setSize = qh.PositionSetSize;
        numSet = max / setSize;
        return new CacheableString(String.Format("pos{0}-{1}",
          Util.Rand(numSet), Util.Rand(setSize)));
      }
      else
      {
        return m_keysA[Util.Rand(m_maxKeys)];
      }
    }

    private IGFSerializable GetUserObject(string objType)
    {
      IGFSerializable usrObj = null;
      ResetKey(EntryCount);
      int numOfKeys = GetUIntValue(EntryCount);
      ResetKey(ValueSizes);
      int objSize = GetUIntValue(ValueSizes);
      QueryHelper qh = QueryHelper.GetHelper();
      int numSet = 0;
      int setSize = 0;
      if (objType != null && objType == "Portfolio")
      {
        setSize = qh.PortfolioSetSize;
        numSet = numOfKeys / setSize;
        usrObj = new Portfolio(Util.Rand(setSize), objSize);
      }
      else if (objType != null && objType == "Position")
      {
        setSize = qh.PositionSetSize;
        numSet = numOfKeys / setSize;
        int numSecIds = Portfolio.SecIds.Length;
        usrObj = new Position(Portfolio.SecIds[setSize % numSecIds], setSize * 100);
      }
      return usrObj;
    }

    private bool AllowQuery(QueryCategory category, bool haveLargeResultset,
      bool islargeSetQuery, bool isUnsupportedPRQuery)
    {
      if (category == QueryCategory.Unsupported)
      {
        return false;
      }
      else if (haveLargeResultset != islargeSetQuery)
      {
        return false;
      }
      else if (isUnsupportedPRQuery &&
               ((category == QueryCategory.MultiRegion) ||
                (category == QueryCategory.NestedQueries)))
      {
        return false;
      }
      else
      {
        return true;
      }
    }

    private void RunQuery(ref int queryCnt)
    {
      FwkInfo("In Security.RunQuery");

      try
      {
        ResetKey(EntryCount);
        int numOfKeys = GetUIntValue(EntryCount);
        QueryHelper qh = QueryHelper.GetHelper();
        int setSize = qh.PortfolioSetSize;
        if (numOfKeys < setSize)
        {
          setSize = numOfKeys;
        }
        int index = Util.Rand(QueryStrings.RSsize);
        DateTime startTime;
        DateTime endTime;
        TimeSpan elapsedTime;
        QueryService qs = CacheHelper.DCache.GetQueryService();
        ResetKey(LargeSetQuery);
        ResetKey(UnsupportedPRQuery);
        bool isLargeSetQuery = GetBoolValue(LargeSetQuery);
        bool isUnsupportedPRQuery = GetBoolValue(UnsupportedPRQuery);
        QueryStrings currentQuery = QueryStatics.ResultSetQueries[index];
        if (AllowQuery(currentQuery.Category, currentQuery.IsLargeResultset,
            isLargeSetQuery, isUnsupportedPRQuery))
        {
          string query = currentQuery.Query;
          FwkInfo("Security.RunQuery: ResultSet Query Category [{0}], " +
            "String [{1}].", currentQuery.Category, query);
          Query qry = qs.NewQuery(query);
          startTime = DateTime.Now;
          ISelectResults results = qry.Execute(600);
          endTime = DateTime.Now;
          elapsedTime = endTime - startTime;
          FwkInfo("Security.RunQuery: Time Taken to execute" +
            " the query [{0}]: {1}ms", query, elapsedTime.TotalMilliseconds);
          ++queryCnt;
        }
        index = Util.Rand(QueryStrings.SSsize);
        currentQuery = QueryStatics.StructSetQueries[index];
        if (AllowQuery(currentQuery.Category, currentQuery.IsLargeResultset,
            isLargeSetQuery, isUnsupportedPRQuery))
        {
          string query = currentQuery.Query;
          FwkInfo("Security.RunQuery: StructSet Query Category [{0}], " +
            "String [{1}].", currentQuery.Category, query);
          Query qry = qs.NewQuery(query);
          startTime = DateTime.Now;
          ISelectResults results = qry.Execute(600);
          endTime = DateTime.Now;
          elapsedTime = endTime - startTime;
          FwkInfo("Security.RunQuery: Time Taken to execute" +
            " the query [{0}]: {1}ms", query, elapsedTime.TotalMilliseconds);
          ++queryCnt;
        }
      }
      catch (Exception ex)
      {
        FwkException("Security.RunQuery: Caught Exception: {0}", ex);
      }
      FwkInfo("Security.RunQuery complete.");
    }

    #endregion

    #region Public methods

    public new void DoRegisterInterestList()
    {
      FwkInfo("In DoRegisterInterestList()");
      try
      {
        Region region = GetRegion();
        int numKeys = GetUIntValue(DistinctKeys);  // check distince keys first
        if (numKeys <= 0)
        {
          FwkSevere("DoRegisterInterestList() Failed to initialize keys " +
            "with numKeys: {0}", numKeys);
          return;
        }
        int low = GetUIntValue(KeyIndexBegin);
        low = (low > 0) ? low : 0;
        int numOfRegisterKeys = GetUIntValue(RegisterKeys);
        int high = numOfRegisterKeys + low;
        ClearKeys();
        m_maxKeys = numOfRegisterKeys;
        m_keyIndexBegin = low;
        int keySize = GetUIntValue(KeySize);
        keySize = (keySize > 0) ? keySize : 10;
        string keyBase = new string('A', keySize);
        InitStrKeys(low, high, keyBase);

        FwkInfo("DoRegisterInterestList() registering interest for {0} to {1}",
          low, high);
        CacheableKey[] registerKeyList = new CacheableKey[high - low];
        for (int j = low; j < high; j++)
        {
          if (m_keysA[j - low] != null)
          {
            registerKeyList[j - low] = m_keysA[j - low];
          }
          else
          {
            FwkInfo("DoRegisterInterestList() key[{0}] is null.", (j - low));
          }
        }
        FwkInfo("DoRegisterInterestList() region name is {0}", region.Name);
        region.RegisterKeys(registerKeyList);
      }
      catch (Exception ex)
      {
        FwkException("DoRegisterInterestList() Caught Exception: {0}", ex);
      }
      FwkInfo("DoRegisterInterestList() complete.");
    }

    public void DoEntryOperations()
    {
      FwkInfo("DoEntryOperations called.");

      int opsSec = GetUIntValue(OpsSecond);
      opsSec = (opsSec < 1) ? 0 : opsSec;

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;

      int valSize = GetUIntValue(ValueSizes);
      valSize = ((valSize < 0) ? 32 : valSize);

      DateTime now = DateTime.Now;
      DateTime end = now + TimeSpan.FromSeconds(secondsToRun);

      byte[] valBuf = Encoding.ASCII.GetBytes(new string('A', valSize));

      string opcode = null;

      int creates = 0, puts = 0, gets = 0, dests = 0, invals = 0, queries = 0;
      Region region = GetRegion();
      if (region == null)
      {
        FwkSevere("Security.DoEntryOperations: No region to perform operations on.");
        now = end; // Do not do the loop
      }

      FwkInfo("DoEntryOperations will work for {0} secs using {1} byte values.", secondsToRun, valSize);

      CacheableKey key;
      IGFSerializable value;
      IGFSerializable tmpValue;
      PaceMeter meter = new PaceMeter(opsSec);
      string objectType = GetStringValue(ObjectType);
      while (now < end)
      {
        try
        {
          opcode = GetStringValue(EntryOps);
          if (opcode == null || opcode.Length == 0) opcode = "no-op";

          if (opcode == "add")
          {
            key = GetKey(entryCount);
            if (objectType != null && objectType.Length > 0)
            {
              tmpValue = GetUserObject(objectType);
            }
            else
            {
              tmpValue = CacheableBytes.Create(valBuf);
            }
            region.Create(key, tmpValue);
            creates++;
          }
          else
          {
            key = GetKey(entryCount);
            if (opcode == "update")
            {
              if (objectType != null && objectType.Length > 0)
              {
                tmpValue = GetUserObject(objectType);
              }
              else
              {
                int keyVal = int.Parse(key.ToString());
                int val = BitConverter.ToInt32(valBuf, 0);
                val = (val == keyVal) ? keyVal + 1 : keyVal;  // alternate the value so that it can be validated later.
                BitConverter.GetBytes(val).CopyTo(valBuf, 0);
                BitConverter.GetBytes(DateTime.Now.Ticks).CopyTo(valBuf, 4);
                tmpValue = CacheableBytes.Create(valBuf);
              }
              region.Put(key, tmpValue);
              puts++;
            }
            else if (opcode == "invalidate")
            {
              region.Invalidate(key);
              invals++;
            }
            else if (opcode == "destroy")
            {
              region.Destroy(key);
              dests++;
            }
            else if (opcode == "read")
            {
              value = region.Get(key);
              gets++;
            }
            else if (opcode == "read+localdestroy")
            {
              value = region.Get(key);
              gets++;
              region.LocalDestroy(key);
              dests++;
            }
            else if (opcode == "query")
            {
              RunQuery(ref queries);
            }
            else
            {
              FwkSevere("Invalid operation specified: {0}", opcode);
            }
          }
        }
        catch (TimeoutException ex)
        {
          FwkSevere("Security: Caught unexpected timeout exception during entry " +
            "{0} operation; continuing with the test: {1}", opcode, ex);
        }
        catch (EntryExistsException)
        {
        }
        catch (EntryNotFoundException)
        {
        }
        catch (EntryDestroyedException)
        {
        }
        catch (Exception ex)
        {
          end = DateTime.Now;
          FwkException("Security: Caught unexpected exception during entry " +
            "{0} operation; exiting task: {1}", opcode, ex);
        }
        meter.CheckPace();
        now = DateTime.Now;
      }
      FwkInfo("DoEntryOperations did {0} creates, {1} puts, {2} gets, " +
        "{3} invalidates, {4} destroys, {5} queries.", creates, puts, gets,
        invals, dests, queries);
    }

    public void DoEntryOperationsMU()
    {
      FwkInfo("DoEntryOperations called.");

      int opsSec = GetUIntValue(OpsSecond);
      opsSec = (opsSec < 1) ? 0 : opsSec;

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;

      int valSize = GetUIntValue(ValueSizes);
      valSize = ((valSize < 0) ? 32 : valSize);

      DateTime now = DateTime.Now;
      DateTime end = now + TimeSpan.FromSeconds(secondsToRun);

      byte[] valBuf = Encoding.ASCII.GetBytes(new string('A', valSize));

      string opcode = null;

      int creates = 0, puts = 0, gets = 0, dests = 0, invals = 0, queries = 0;
      Region region = GetRegion();
      if (region == null)
      {
        FwkSevere("Security.DoEntryOperations: No region to perform operations on.");
        now = end; // Do not do the loop
      }

      FwkInfo("DoEntryOperations will work for {0} secs using {1} byte values.", secondsToRun, valSize);

      CacheableKey key;
      IGFSerializable value;
      IGFSerializable tmpValue;
      PaceMeter meter = new PaceMeter(opsSec);
      string objectType = GetStringValue(ObjectType);
      while (now < end)
      {
        try
        {
          opcode = GetStringValue(EntryOps);
          if (opcode == null || opcode.Length == 0) opcode = "no-op";

          if (opcode == "add")
          {
            key = GetKey(entryCount);
            if (objectType != null && objectType.Length > 0)
            {
              tmpValue = GetUserObject(objectType);
            }
            else
            {
              tmpValue = CacheableBytes.Create(valBuf);
            }
            region.Create(key, tmpValue);
            creates++;
          }
          else
          {
            key = GetKey(entryCount);
            if (opcode == "update")
            {
              if (objectType != null && objectType.Length > 0)
              {
                tmpValue = GetUserObject(objectType);
              }
              else
              {
                int keyVal = int.Parse(key.ToString());
                int val = BitConverter.ToInt32(valBuf, 0);
                val = (val == keyVal) ? keyVal + 1 : keyVal;  // alternate the value so that it can be validated later.
                BitConverter.GetBytes(val).CopyTo(valBuf, 0);
                BitConverter.GetBytes(DateTime.Now.Ticks).CopyTo(valBuf, 4);
                tmpValue = CacheableBytes.Create(valBuf);
              }
              region.Put(key, tmpValue);
              puts++;
            }
            else if (opcode == "invalidate")
            {
              region.Invalidate(key);
              invals++;
            }
            else if (opcode == "destroy")
            {
              region.Destroy(key);
              dests++;
            }
            else if (opcode == "read")
            {
              value = region.Get(key);
              gets++;
            }
            else if (opcode == "read+localdestroy")
            {
              value = region.Get(key);
              gets++;
              region.LocalDestroy(key);
              dests++;
            }
            else if (opcode == "query")
            {
              RunQuery(ref queries);
            }
            else
            {
              FwkSevere("Invalid operation specified: {0}", opcode);
            }
          }
        }
        catch (TimeoutException ex)
        {
          FwkSevere("Security: Caught unexpected timeout exception during entry " +
            "{0} operation; continuing with the test: {1}", opcode, ex);
        }
        catch (EntryExistsException)
        {
        }
        catch (EntryNotFoundException)
        {
        }
        catch (EntryDestroyedException)
        {
        }
        catch (Exception ex)
        {
          end = DateTime.Now;
          FwkException("Security: Caught unexpected exception during entry " +
            "{0} operation; exiting task: {1}", opcode, ex);
        }
        meter.CheckPace();
        now = DateTime.Now;
      }
      FwkInfo("DoEntryOperations did {0} creates, {1} puts, {2} gets, " +
        "{3} invalidates, {4} destroys, {5} queries.", creates, puts, gets,
        invals, dests, queries);
    }
    #endregion
  }
}
