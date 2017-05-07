//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests;

  public class MyCqListener : ICqListener
  {
    private UInt32 m_updateCnt;
    private UInt32 m_createCnt;
    private UInt32 m_destroyCnt;
    private UInt32 m_eventCnt;

    public MyCqListener()
    {
      m_updateCnt = 0;
      m_createCnt = 0;
      m_destroyCnt = 0;
      m_eventCnt = 0;
    }

    public UInt32 NumInserts()
    {
      return m_createCnt;
    }

    public UInt32 NumUpdates()
    {
      return m_updateCnt;
    }

    public UInt32 NumDestroys()
    {
      return m_destroyCnt;
    }

    public UInt32 NumEvents()
    {
      return m_eventCnt;
    }

    public virtual void UpdateCount(CqEvent ev)
    {
      m_eventCnt++;
      CqOperationType opType = ev.getQueryOperation();
      if (opType == CqOperationType.OP_TYPE_CREATE)
      {
        m_createCnt++;
       // Util.Log("m_create is {0}",m_createCnt);
      }
      else if (opType == CqOperationType.OP_TYPE_UPDATE)
      {
        m_updateCnt++;
       // Util.Log("m_create is {0}", m_updateCnt);
      }
      else if (opType == CqOperationType.OP_TYPE_DESTROY)
      {
        m_destroyCnt++;
      //  Util.Log("m_create is {0}", m_destroyCnt);
      }

    }
    public virtual void OnError(CqEvent ev)
    {
      UpdateCount(ev);
    }

    public virtual void OnEvent(CqEvent ev)
    {
      UpdateCount(ev);
    }

    public virtual void Close()
    {
    }
  }

  public class CacheServer : FwkTest
  {
    #region Private constants and statics

    private const string RegionName = "regionName";
    private const string ValueSizes = "valueSizes";
    private const string OpsSecond = "opsSecond";
    private const string EntryCount = "entryCount";
    private const string WorkTime = "workTime";
    private const string EntryOps = "entryOps";
    private const string LargeSetQuery = "largeSetQuery";
    private const string UnsupportedPRQuery = "unsupportedPRQuery";
    private const string ObjectType = "objectType";
   // private const bool MultiRegion = "multiRegion";

    private static Dictionary<string, int> OperationsMap =
      new Dictionary<string, int>();
    private static Dictionary<string, int> ExceptionsMap =
      new Dictionary<string, int>();

    #endregion

    #region Private utility methods
    
    private Region GetRegion()
    {
      return GetRegion(null);
    }

    private Region GetRegion(string regionName)
    {
      Region region;
      if (regionName == null)
      {
        regionName = GetStringValue("regionName");
      }
      if (regionName == null)
      {
        region = GetRootRegion();
        if (region == null)
        {
          Region[] rootRegions = CacheHelper.DCache.RootRegions();
          if (rootRegions != null && rootRegions.Length > 0)
          {
            region = rootRegions[Util.Rand(rootRegions.Length)];
          }
        }
      }
      else
      {
        region = CacheHelper.GetRegion(regionName);
      }
      return region;
    }

    private void AddValue(Region region, int count, byte[] valBuf)
    {
      if (region == null)
      {
        FwkSevere("CacheServer::AddValue(): No region to perform add on.");
        return;
      }
      CacheableKey key = new CacheableString(count.ToString());
      CacheableBytes value = CacheableBytes.Create(valBuf);
      BitConverter.GetBytes(count).CopyTo(valBuf, 0);
      BitConverter.GetBytes(DateTime.Now.Ticks).CopyTo(valBuf, 4);
      try
      {
        region.Create(key, value);
        //FwkInfo("key: {0}  value: {1}", key, Encoding.ASCII.GetString(value.Value));
      }
      catch (Exception ex)
      {
        FwkException("CacheServer.AddValue() caught Exception: {0}", ex);
      }
    }

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
        return new CacheableString(String.Format("port{0}-{1}", Util.Rand(numSet), Util.Rand(setSize)));
      }
      else if (objectType != null && objectType == "Position")
      {
        setSize = qh.PositionSetSize;
        numSet = max / setSize;
        return new CacheableString(String.Format("pos{0}-{1}", Util.Rand(numSet), Util.Rand(setSize)));
      }
      return new CacheableString(Util.Rand(max).ToString());
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
    private void remoteQuery(QueryStrings currentQuery, bool isLargeSetQuery, 
      bool isUnsupportedPRQuery, int queryIndex,bool isparam,bool isStructSet)
    {
      DateTime startTime;
      DateTime endTime;
      TimeSpan elapsedTime;
      QueryService qs = CheckQueryService();
      if (AllowQuery(currentQuery.Category, currentQuery.IsLargeResultset,
            isLargeSetQuery, isUnsupportedPRQuery))
        {
          string query = currentQuery.Query;
          FwkInfo("CacheServer.RunQuery: ResultSet Query Category [{0}], " +
            "String [{1}].", currentQuery.Category, query);
          Query qry = qs.NewQuery(query);
          IGFSerializable[] paramList = null;
          if (isparam)
          {
            Int32 numVal = 0;
            if (isStructSet)
            {
              paramList = new IGFSerializable[QueryStatics.NoOfQueryParamSS[queryIndex]];

              for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[queryIndex]; ind++)
              {
                try
                {
                  numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[queryIndex][ind]);
                  paramList[ind] = new CacheableInt32(numVal);
                }
                catch (FormatException)
                {
                  paramList[ind] = new CacheableString((System.String)QueryStatics.QueryParamSetSS[queryIndex][ind]);
                }
              }
            }
            else
            {
              paramList = new IGFSerializable[QueryStatics.NoOfQueryParam[queryIndex]];
              for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParam[queryIndex]; ind++)
              {
                try
                {
                  numVal = Convert.ToInt32(QueryStatics.QueryParamSet[queryIndex][ind]);
                  paramList[ind] = new CacheableInt32(numVal);
                }
                catch (FormatException)
                {
                  paramList[ind] = new CacheableString((System.String)QueryStatics.QueryParamSet[queryIndex][ind]);
                }
              }
            }
          }
          ISelectResults results = null;
          startTime = DateTime.Now;
          if (isparam)
          {
            results = qry.Execute(paramList,600);
          }
          else
          {
            results = qry.Execute(600);
          }
          endTime = DateTime.Now;
          elapsedTime = endTime - startTime;
          FwkInfo("CacheServer.RunQuery: Time Taken to execute" +
            " the query [{0}]: {1}ms", query, elapsedTime.TotalMilliseconds);
        }
    }

    private void RunQuery()
    {
      FwkInfo("In CacheServer.RunQuery");

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
        int i = Util.Rand(QueryStrings.RSsize);
        ResetKey(LargeSetQuery);
        ResetKey(UnsupportedPRQuery);
        bool isLargeSetQuery = GetBoolValue(LargeSetQuery);
        bool isUnsupportedPRQuery = GetBoolValue(UnsupportedPRQuery);
        QueryStrings currentQuery = QueryStatics.ResultSetQueries[i];
        remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i,false,false);
        i = Util.Rand(QueryStrings.SSsize);
        currentQuery = QueryStatics.StructSetQueries[i];
        remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i,false,false);
        i = Util.Rand(QueryStrings.RSPsize);
        currentQuery = QueryStatics.ResultSetParamQueries[i];
        remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i,true,false);
        i = Util.Rand(QueryStrings.SSPsize);
        currentQuery = QueryStatics.StructSetParamQueries[i];
        remoteQuery(currentQuery, isLargeSetQuery, isUnsupportedPRQuery, i, true,true);
      }
      catch (Exception ex)
      {
        FwkException("CacheServer.RunQuery: Caught Exception: {0}", ex);
      }
      FwkInfo("CacheServer.RunQuery complete.");
    }

    private void UpdateOperationsMap(string opCode, int numOps)
    {
      UpdateOpsMap(OperationsMap, opCode, numOps);
    }

    private void UpdateExceptionsMap(string opCode, int numOps)
    {
      UpdateOpsMap(ExceptionsMap, opCode, numOps);
    }

    private void UpdateOpsMap(Dictionary<string, int> map, string opCode,
      int numOps)
    {
      lock (((ICollection)map).SyncRoot)
      {
        int currentOps;
        if (!map.TryGetValue(opCode, out currentOps))
        {
          currentOps = 0;
        }
        map[opCode] = currentOps + numOps;
      }
    }

    private int GetOpsFromMap(Dictionary<string, int> map, string opCode)
    {
      int numOps;
      lock (((ICollection)map).SyncRoot)
      {
        if (!map.TryGetValue(opCode, out numOps))
        {
          numOps = 0;
        }
      }
      return numOps;
    }
    private void PutAllOps()
    {
      Region region = GetRegion();
      CacheableHashMap map = new CacheableHashMap();
      int valSize = GetUIntValue(ValueSizes);
      int minValSize = (int)(sizeof(int) + sizeof(long) + 4);
      valSize = ((valSize < minValSize) ? minValSize : valSize);
      string valBuf = null;
      ResetKey(ObjectType);
      string objectType = GetStringValue(ObjectType);
      if (objectType != null)
      {
        Int32 numSet = 0;
        Int32 setSize = 0;
        QueryHelper qh = QueryHelper.GetHelper();
        IGFSerializable port;
        setSize = qh.PortfolioSetSize;
        numSet = 200 / setSize;
        for (int set = 1; set <= numSet; set++)
        {
          for (int current = 1; current <= setSize; current++)
          {
            port = new Portfolio(current, valSize);
            string Id = String.Format("port{0}-{1}", set,current); 
            CacheableKey key = new CacheableString(Id.ToString());
            map.Add(key, port);
          }
        }
      }
      else
      {
        valBuf = new string('A', valSize);
        for (int count = 0; count < 200; count++)
        {
          CacheableKey key = new CacheableString(count.ToString());
          CacheableBytes value = CacheableBytes.Create(
            Encoding.ASCII.GetBytes(valBuf));
          map.Add(key, value);
        }
      }
      region.PutAll(map,60);
    }

    private void GetAllOps()
    {
      Region region = GetRegion();
      List<ICacheableKey> keys  = new List<ICacheableKey>();
      keys.Clear();
      for (int count = 0; count < 200; count++)
      {
        CacheableKey key = new CacheableString(count.ToString());
        keys.Add(key);
      }    
      Dictionary<ICacheableKey, IGFSerializable> values = new Dictionary<ICacheableKey, IGFSerializable>();
      values.Clear();
      region.GetAll(keys.ToArray(), values, null, false);
    }
    #endregion

    #region Public methods
    public virtual void DoCreateRegion()
    {
      FwkInfo("In DoCreateRegion()");
      try {
        Region region = CreateRootRegion();
        if (region == null) {
          FwkException("DoCreateRegion()  could not create region.");
        }
        FwkInfo("DoCreateRegion()  Created region '{0}'", region.Name);
      } catch (Exception ex) {
        FwkException("DoCreateRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoCreateRegion() complete.");
    }

    public void DoCloseCache()
    {
      FwkInfo("DoCloseCache()  Closing cache and disconnecting from" +
        " distributed system.");
      CacheHelper.Close();
    }
    public void DoRegisterAllKeys()
    {
      FwkInfo("In DoRegisterAllKeys()");
      try {
        Region region = GetRegion();
        FwkInfo("DoRegisterAllKeys() region name is {0}", region.Name);
        bool isDurable = GetBoolValue("isDurableReg");
        ResetKey("getInitialValues");
        bool isGetInitialValues = GetBoolValue("getInitialValues");
        bool checkReceiveVal = GetBoolValue("checkReceiveVal");
        bool isReceiveValues = true;
        if (checkReceiveVal) {
          ResetKey("receiveValue");
          isReceiveValues = GetBoolValue("receiveValue");
        }
        region.RegisterAllKeys(isDurable, null, isGetInitialValues, isReceiveValues);
      } catch (Exception ex) {
        FwkException("DoRegisterAllKeys() Caught Exception: {0}", ex);
      }
      FwkInfo("DoRegisterAllKeys() complete.");
    }
    public void DoFeed()
    {
      FwkInfo("CacheServer.DoFeed() called.");

      int opsSec = GetUIntValue(OpsSecond);
      opsSec = (opsSec < 1) ? 0 : opsSec;

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int cnt = 0;
      int valSize = GetUIntValue(ValueSizes);
      int minValSize = (int)(sizeof(int) + sizeof(long) + 4);
      valSize = ((valSize < minValSize) ? minValSize : valSize);
      string valBuf = new string('A', valSize);

      Region region = GetRegion();
      PaceMeter pm = new PaceMeter(opsSec);
      while (cnt++ < entryCount)
      {
        AddValue(region, cnt, Encoding.ASCII.GetBytes(valBuf));
        pm.CheckPace();
      }
    }

    public void DoEntryOperations()
    {
      FwkInfo("CacheServer.DoEntryOperations() called.");

      int opsSec = GetUIntValue(OpsSecond);
      opsSec = (opsSec < 1) ? 0 : opsSec;

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;

      int valSize = GetUIntValue(ValueSizes);
      int minValSize = (int)(sizeof(int) + sizeof(long) + 4);
      valSize = ((valSize < minValSize) ? minValSize : valSize);

      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);

      CacheableKey key;
      IGFSerializable value;
      IGFSerializable tmpValue;
      byte[] valBuf = Encoding.ASCII.GetBytes(new string('A', valSize));

      string opcode = null;

      int creates = 0, puts = 0, gets = 0, dests = 0, invals = 0, query = 0, putAll = 0, getAll = 0;
      Region region = GetRegion();
      if (region == null)
      {
        FwkSevere("CacheServer.DoEntryOperations(): No region to perform operations on.");
        now = end; // Do not do the loop
      }

      FwkInfo("CacheServer.DoEntryOperations() will work for {0}secs " +
        "using {1} byte values.", secondsToRun, valSize);

      PaceMeter pm = new PaceMeter(opsSec);
      string objectType = GetStringValue(ObjectType);
      bool multiRegion = GetBoolValue("multiRegion");
      while (now < end)
      {
        try
        {
          opcode = GetStringValue(EntryOps);
          if (opcode == null || opcode.Length == 0) 
          {
            opcode = "no-op";
          }
          if (multiRegion)
          {
            region = GetRegion();
            if (region == null)
            {
              FwkException("CacheServerTest::doEntryOperations(): No region to perform operation {0}" , opcode);
            }
          }

          key = GetKey(entryCount);
          if (opcode == "add")
          {
            if (objectType != null && objectType.Length > 0)
            {
              tmpValue = GetUserObject(objectType);
            }
            else
            {
              tmpValue = CacheableBytes.Create(valBuf);
            }
            try
            {
              region.Create(key, tmpValue);
              creates++;
            }
            catch (EntryExistsException ex)
            {
              FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                   "ex-ception in add: {0}", ex.Message);
            }
          }
          else
          {
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
              try
              {
                region.Invalidate(key);
                invals++;
              }
              catch (EntryNotFoundException ex)
              {
                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                  "ex-ception in invalidate: {0}", ex.Message);
              }
            }
            else if (opcode == "destroy")
            {
              try
              {
                region.Destroy(key);
                dests++;
              }
              catch (EntryNotFoundException ex)
              {
                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                  "ex-ception in destroy: {0}", ex.Message);
              }
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
              try
              {
                region.LocalDestroy(key);
                dests++;
              }
              catch (EntryNotFoundException ex)
              {
                FwkInfo("CacheServer.DoEntryOperations() Caught non-fatal " +
                  "ex-ception in localDestroy: {0}", ex.Message);
              }
            }
            else if (opcode == "query")
            {
              RunQuery();
              query += 4;
            }
            else if (opcode == "putAll")
            {
              PutAllOps();
              putAll++;
            }
            else if (opcode == "getAll")
            {
              GetAllOps();
              getAll++;
            }
            else
            {
              FwkException("CacheServer.DoEntryOperations() Invalid operation " +
                "specified: {0}", opcode);
            }
          }
        }
        catch (Exception ex)
        {
          FwkException("CacheServer.DoEntryOperations() Caught unexpected " +
            "exception during entry '{0}' operation: {1}.", opcode, ex);
        }
        pm.CheckPace();
        now = DateTime.Now;
      }
      key = null;
      value = null;

      FwkInfo("CacheServer.DoEntryOperations() did {0} creates, {1} puts, " +
        "{2} gets, {3} invalidates, {4} destroys, {5} querys, {6} putAll, {7} getAll.",
        creates, puts, gets, invals, dests, query, putAll, getAll);
    }

    public void DoEntryOperationsForSecurity()
    {
      FwkInfo("CacheServer.DoEntryOperationsForSecurity() called.");
      Util.RegisterTestCompleteDelegate(TestComplete);

      int opsSec = GetUIntValue(OpsSecond);
      opsSec = (opsSec < 1) ? 0 : opsSec;

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;

      int valSize = GetUIntValue(ValueSizes);
      int minValSize = (int)(sizeof(int) + sizeof(long) + 4);
      valSize = ((valSize < minValSize) ? minValSize : valSize);

      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);

      CacheableKey key;
      IGFSerializable value;
      IGFSerializable tmpValue;
      byte[] valBuf = Encoding.ASCII.GetBytes(new string('A', valSize));

      string opCode = null;

      Region region = GetRegion();
      if (region == null)
      {
        FwkException("CacheServer.DoEntryOperationsForSecurity(): " +
          "No region to perform operations on.");
      }

      FwkInfo("CacheServer.DoEntryOperationsForSecurity() will work for {0}secs " +
        "using {1} byte values.", secondsToRun, valSize);

      int cnt = 0;
      PaceMeter pm = new PaceMeter(opsSec);
      string objectType = GetStringValue(ObjectType);
      while (now < end)
      {
        int addOps = 1;
        opCode = GetStringValue(EntryOps);
        try
        {
          UpdateOperationsMap(opCode, 1);
          if (opCode == null || opCode.Length == 0)
          {
            opCode = "no-op";
          }

          key = GetKey(entryCount);
          if (opCode == "create")
          {
            if (objectType != null && objectType.Length > 0)
            {
              tmpValue = GetUserObject(objectType);
            }
            else
            {
              tmpValue = CacheableBytes.Create(valBuf);
            }
            region.Create(key, tmpValue);
          }
          else
          {
            if (opCode == "update")
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
            }
            else if (opCode == "invalidate")
            {
              region.Invalidate(key);
            }
            else if (opCode == "destroy")
            {
              region.Destroy(key);
            }
            else if (opCode == "get")
            {
              value = region.Get(key);
            }


            else if (opCode == "getServerKeys")
            {
              ICacheableKey[] serverKeys = region.GetServerKeys();
            }

            else if (opCode == "read+localdestroy")
            {
              value = region.Get(key);
              region.LocalDestroy(key);
            }
            else if (opCode == "regNUnregInterest")
            {
              CacheableKey[] keys = new CacheableKey[] { key };
              region.RegisterKeys(keys);
              region.UnregisterKeys(keys);
            }
            else if (opCode == "query")
            {
              QueryService qs = CheckQueryService();
              Query qry = qs.NewQuery("select distinct * from /Portfolios where FALSE");
              ISelectResults result = qry.Execute(600);
            }
            else if (opCode == "cq")
            {
              string cqName = String.Format("cq-{0}-{1}", Util.ClientId, cnt++);
              QueryService qs = CheckQueryService();
              CqAttributesFactory cqFac = new CqAttributesFactory();
              ICqListener cqLstner = new MyCqListener();
              cqFac.AddCqListener(cqLstner);
              CqAttributes cqAttr = cqFac.Create();
              CqQuery cq = qs.NewCq(cqName, "select * from /Portfolios where FALSE", cqAttr, false);
              cq.Execute();
              cq.Stop();
              cq.Execute();
              cq.Close();
            }
            else
            {
              FwkException("CacheServer.DoEntryOperationsForSecurity() " +
                "Invalid operation specified: {0}", opCode);
            }
          }
        }
        catch (NotAuthorizedException)
        {
          //FwkInfo("Got expected NotAuthorizedException for operation {0}: {1}",
          //  opCode, ex.Message);
          UpdateExceptionsMap(opCode, 1);
        }
        catch (EntryExistsException)
        {
          addOps = -1;
          UpdateOperationsMap(opCode, addOps);
        }
        catch (EntryNotFoundException)
        {
          addOps = -1;
          UpdateOperationsMap(opCode, addOps);
        }
        catch (EntryDestroyedException)
        {
          addOps = -1;
          UpdateOperationsMap(opCode, addOps);
        }
        catch (TimeoutException ex)
        {
          FwkSevere("Caught unexpected timeout exception during entry {0} " +
            " operation: {1}; continuing with test.", opCode, ex.Message);
        }
        catch (Exception ex)
        {
          FwkException("CacheServer.DoEntryOperationsForSecurity() Caught " +
            "unexpected exception during entry '{0}' operation: {1}.",
            opCode, ex);
        }
        pm.CheckPace();
        now = DateTime.Now;
      }
      key = null;
      value = null;
    }

    public void DoValidateEntryOperationsForSecurity()
    {
      bool isExpectedPass = GetBoolValue("isExpectedPass");
      string opCode;
      while ((opCode = GetStringValue(EntryOps)) != null)
      {
        int numOps = GetOpsFromMap(OperationsMap, opCode);
        int notAuthzCount = GetOpsFromMap(ExceptionsMap, opCode);
        if (isExpectedPass)
        {
          if (numOps != 0 && notAuthzCount == 0)
          {
            FwkInfo("Task passed sucessfully for operation {0} with total " +
              "operations = {1}", opCode, numOps);
          }
          else
          {
            FwkException("{0} NotAuthorizedExceptions found for operation {1} " +
              "while expected 0", notAuthzCount, opCode);
          }
        }
        else
        {
          if (numOps == notAuthzCount)
          {
            FwkInfo("Operation {0} passed sucessfully and got the expected " +
              "number of incorrect authorizations: {1}", opCode, numOps);
          }
          else
          {
            FwkException("For operation {0} expected number of " +
              "NotAuthorizedExceptions is {1} but found {2}", opCode,
              numOps, notAuthzCount);
          }
        }
      }
    }

    public static void TestComplete()
    {
      OperationsMap.Clear();
      ExceptionsMap.Clear();
    }

    #endregion
  }

  public class Multiusersecurity : FwkTest
  {
    #region Private constants and statics

    private static Cache m_cache = null;
    private const string RegionName = "regionName";
    private const string ValueSizes = "valueSizes";
    private const string MultipleUser = "MultiUsers";
    private const string OpsSecond = "opsSecond";
    private const string EntryCount = "entryCount";
    private const string WorkTime = "workTime";
    private const string EntryOps = "entryOps";
    private const string isDurable = "isDurable";
    //private const string MultiUserMode = "multiUserMode";
    private const string KeyStoreFileProp = "security-keystorepath";
    private const string KeyStoreAliasProp = "security-alias";
    private const string KeyStorePasswordProp = "security-keystorepass";
    private const CredentialGenerator.ClassCode DefaultSecurityCode =
      CredentialGenerator.ClassCode.LDAP;
    private static Dictionary<string, Region> proxyRegionMap=new Dictionary<string,Region>();
    private static Dictionary<string, IRegionService> authCacheMap = new Dictionary<string, IRegionService>();
    private static Dictionary<string, Dictionary<string, int>> operationMap=new Dictionary<string,Dictionary<string,int>>();
    private static Dictionary<string, Dictionary<string, int>> exceptionMap=new Dictionary<string,Dictionary<string,int>>();

    private static ArrayList userList, readerList, queryList, writerList, adminList;  
    private static Dictionary<string, ArrayList> userToRolesMap=new Dictionary<string,ArrayList>();
  
     #endregion

    #region Private utility methods

    public virtual Region DoCreateDCRegion()
    {
      FwkInfo("In DoCreateDCRegion() Durable");

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
            bool isDC = GetBoolValue("isDurable");
            string m_isPool = null;
            // Check if this is a thin-client region; if so set the endpoints
            int redundancyLevel = 0;
            if (endpoints != null && endpoints.Length > 0)
            {
              redundancyLevel = GetUIntValue(RedundancyLevelKey);
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
              string mode = Util.GetEnvironmentVariable("POOLOPT");
              if (mode == "poolwithendpoints" || mode == "poolwithlocator")
              {
                CacheHelper.InitConfigForPoolDurable(durableClientId, durableTimeout, conflateEvents, false);
              }
              else
              {
                FwkInfo("Setting the cache-level endpoints to {0}", endpoints);
                CacheHelper.InitConfigForDurable(endpoints, redundancyLevel, durableClientId, durableTimeout, conflateEvents, false);
              }

            }
            RegionFactory rootAttrs = CacheHelper.DCache.CreateRegionFactory(RegionShortcut.PROXY);
            SetRegionAttributes(rootAttrs, rootRegionData, ref m_isPool);
            rootAttrs = CreatePool(rootAttrs, redundancyLevel);
            FwkInfo("Entering CacheHelper.CreateRegion()");
            region = CacheHelper.CreateRegion(rootRegionName, rootAttrs);
            RegionAttributes regAttr = region.Attributes;
            FwkInfo("Region attributes for {0}: {1}", rootRegionName,
              CacheHelper.RegionAttributesToString(regAttr));

            if (isDC)
            {
              CacheHelper.DCache.ReadyForEvents();
            }

          }
          return region;
        }
        
      }
      else
      {
        FwkSevere("DoCreateDCRegion() failed to create region");
      }

      FwkInfo("DoCreateDCRegion() complete.");
      return null;
    }

    public virtual void DoCreateRegion()
    {
      FwkInfo("In DoCreateRegion()");
      try
      {
        Region region;
        if (operationMap.Count > 0 || exceptionMap.Count > 0)
        {
          operationMap.Clear();
          exceptionMap.Clear();
        }
        bool isDC = GetBoolValue(isDurable);
        if (isDC)
        {
          region = DoCreateDCRegion();
        }
        else
        {
          region = CreateRootRegion();
        }
        FwkInfo("The region name is {0}",region.Name);

        if (region != null)
        {
          string poolName = region.Attributes.PoolName;

          if (poolName != null)
          {
            Pool pool = PoolManager.Find(poolName);
            if (pool.MultiuserAuthentication)
            {
              FwkInfo("pool is in multiuser mode and entering CreateMultiUserCacheAndRegion");
              CreateMultiUserCacheAndRegion(pool, region);
            }
            else
              FwkInfo(" pool is not in multiuser mode ");
          }
          else
            FwkInfo("poolName is null ");
        }
        else 
          FwkInfo(" returing null region");
        //return null;
      }
      catch (Exception ex)
      {
        FwkException("DoCreateRegion() Caught Exception: {0}", ex);
      }
      FwkInfo("DoCreateRegion() complete.");
    }

    void CreateMultiUserCacheAndRegion(Pool pool,Region region) 
    {
      FwkInfo("In CreateMultiUserCacheAndRegion()");
      string userName;
      CredentialGenerator gen = GetCredentialGenerator();
      CredentialGenerator.ClassCode SecurityCode = gen.GetClassCode();
      gen = CredentialGenerator.Create(SecurityCode, true);
      if (gen == null)
      {
        FwkInfo("Skipping security scheme {0} with no generator. Using  default security scheme.{1}", SecurityCode, DefaultSecurityCode);
        SecurityCode = DefaultSecurityCode;
      }
      string scheme = SecurityCode.ToString();
      string user = GetStringValue(SecurityCode.ToString());
      string regionName=region.Name;
      ResetKey(MultipleUser);
      Int32 numOfMU = GetUIntValue(MultipleUser);
      userList = new ArrayList();
     //populating the list with no. of multiusers.
      for (Int32 i = 1; i <= numOfMU; i++)
      {
        string userStr = string.Format("{0}{1}",user,i);
        userList.Add(userStr);
      }

      Int32 userSize=userList.Count;
      if (SecurityCode == CredentialGenerator.ClassCode.LDAP)
      {
        for (Int32 i = 0; i < userSize; i++)
        {
          Properties userProp = new Properties();
          userName = (String)userList[i];

          userProp.Insert("security-username", userName);
          userProp.Insert("security-password", userName);
          IRegionService mu_cache = CacheHelper.DCache.CreateAuthenticatedView(userProp, pool.Name);
          authCacheMap.Add(userName, mu_cache);
          //mu_cache = pool.CreateSecureUserCache(userProp);
          Region m_region = mu_cache.GetRegion(regionName);
          proxyRegionMap.Add(userName, m_region);
          Dictionary<string, int> opMAP=new Dictionary<string,int>();
          Dictionary<string, int> expMAP=new Dictionary<string,int>();
          operationMap.Add(userName,opMAP);
          exceptionMap.Add(userName,expMAP);
          Utility.GetClientProperties(gen.AuthInit, null, ref userProp);
          FwkInfo("Security properties entries: {0}", userProp);
          switch (i)
          {
            case 0:
            case 1:
              setAdminRole(userName);
             break;
            case 2:
            case 3:
            case 4:
              setReaderRole(userName);
              break;
            case 5:
            case 6:
            case 7:
              setWriterRole(userName);
              break;
            case 8:
            case 9:
              setQueryRole(userName);
              break;
          };
        }
      }
     else
      {
        FwkInfo("Security Scheme is {0}", SecurityCode);
        for (Int32 i = 0; i < userSize; i++)
        {          
          Properties userProp = new Properties();
          PkcsAuthInit pkcs = new PkcsAuthInit();
          if (pkcs == null) {
            FwkException("NULL PKCS Credential Generator");
          }
          userName = (String)userList[i];
          string dataDir = Util.GetFwkLogDir(Util.SystemType) + "/data";
          userProp.Insert(KeyStoreFileProp, GetKeyStoreDir(dataDir) +
            userName + ".keystore");
          userProp.Insert(KeyStoreAliasProp, userName);
          userProp.Insert(KeyStorePasswordProp, "gemfire");
          //mu_cache = pool.CreateSecureUserCache(userProp);
          //IRegionService mu_cache = CacheHelper.DCache.CreateAuthenticatedView(userProp, pool.Name);
          IRegionService mu_cache = CacheHelper.DCache.CreateAuthenticatedView(pkcs.GetCredentials(userProp, "0:0"), pool.Name);
          authCacheMap.Add(userName, mu_cache);
          Region m_region = mu_cache.GetRegion(regionName);
          proxyRegionMap.Add(userName, m_region);
          Dictionary<string, int> opMAP = new Dictionary<string, int>();
          Dictionary<string, int> expMAP = new Dictionary<string, int>();
          operationMap.Add(userName, opMAP);
          exceptionMap.Add(userName, expMAP);
          Utility.GetClientProperties(gen.AuthInit, null, ref userProp);
          FwkInfo("Security properties entries: {0}", userProp);
         switch (i)
          {
            case 0:
            case 1:
              setAdminRole(userName);
              break;
            case 2:
            case 3:
            case 4:
              setReaderRole(userName);
              break;
            case 5:
            case 6:
            case 7:
              setWriterRole(userName);
              break;
            case 8:
            case 9:
              setQueryRole(userName);
              break;
          };
        }
      }
    }

    public string GetKeyStoreDir(string dataDir)
    {
      string keystoreDir = dataDir;
      if (keystoreDir != null && keystoreDir.Length > 0)
      {
        keystoreDir += "/keystore/";
      }
      return keystoreDir;
    }


    public void setAdminRole(string userName)
    {
      adminList = new ArrayList();
      adminList.Add("create");
      adminList.Add("update");
      adminList.Add("get");
      adminList.Add("getServerKeys");
      adminList.Add("destroy");
      adminList.Add("query");
      adminList.Add("cq");
      adminList.Add("putAll");
      adminList.Add("executefunction");
      userToRolesMap.Add(userName,adminList);
    }

    public void setReaderRole(string userName)
    {
      readerList = new ArrayList();
      readerList.Add("get");
      readerList.Add("getServerKeys");
      readerList.Add("cq");
      userToRolesMap.Add(userName,readerList);
    }

    public void setWriterRole(string userName)
    {
      writerList = new ArrayList();
      writerList.Add("create");
      writerList.Add("update");
      writerList.Add("destroy");
      writerList.Add("putAll");
      writerList.Add("executefunction");
      userToRolesMap.Add(userName, writerList);
    }

    public void setQueryRole(string userName)
    {
      queryList = new ArrayList();
      queryList.Add("query");
      queryList.Add("cq");
      userToRolesMap.Add(userName, queryList);
    }
   
       #region Public methods

    public void DoFeedTask()
    {
      FwkInfo("MultiUserSecurity.DoFeed() called.");

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int cnt = 0;
      string userName=(String)userList[0];
      Region region = proxyRegionMap[userName];
      while (cnt++ < entryCount)
      {
        string keyStr = cnt.ToString();
        CacheableKey key = new CacheableString(keyStr);
        CacheableString value = CacheableString.Create("Value");
        region.Put(key,value);
      }
      FwkInfo("MultiUserSecurity.DoFeed() completed");
    }

    public void DoCqForMU()
    {
      FwkInfo("MultiUserSecurity.DoCqForMU() called ");
      CqQuery cq;
      string uName = (String)userList[1];
      Region region = proxyRegionMap[uName];
      IRegionService authCache = authCacheMap[uName];
      for (Int32 i = 0; i < userList.Count; i++)
      {
        string userName = (String)userList[i];
        string cqName = String.Format("cq-{0}", userName);
        QueryService qs = authCache.GetQueryService();
        CqAttributesFactory cqFac = new CqAttributesFactory();
        ICqListener cqLstner = new MyCqListener();
        cqFac.AddCqListener(cqLstner);
        CqAttributes cqAttr = cqFac.Create();
        cq = qs.NewCq(cqName, "select * from /Portfolios where TRUE", cqAttr, true);
        cq.Execute();
      }
      FwkInfo("MultiUserSecurity.DoCqForMU() completed");
    }

    public void DoCloseCacheAndReInitialize() {
      FwkInfo("MultiUserSecurity.DoCloseCacheAndReInitialize() called");
      try
      {
        //if (mu_cache != null)
        {
          Region[] vregion = CacheHelper.DCache.RootRegions();
          try
          {
            for (Int32 i = 0; i < vregion.Length; i++)
            {
              Region region = (Region)vregion.GetValue(i);
              region.LocalDestroyRegion();
            }
          }
          catch (RegionDestroyedException ignore)
          {
            string message = ignore.Message;
            Util.Log(message);
          }
          catch (Exception ex)
          {
            FwkException("Caught unexpected exception during region local destroy {0}", ex);
          }
          bool keepalive = GetBoolValue("keepAlive");
          bool isDurable = GetBoolValue("isDurable");
          if (isDurable)
          {
            FwkInfo("KeepAlive is {0}", keepalive);
            CacheHelper.DCache.Close(keepalive);
            //m_cache.Close(keepalive);            
          }
          else
            m_cache.Close();
          m_cache = null;
          FwkInfo("Cache Close");
        }
        proxyRegionMap.Clear();
        authCacheMap.Clear();
        operationMap.Clear();
        exceptionMap.Clear();
        userToRolesMap.Clear();
        userList.Clear();
        CacheHelper.Close();
          DoCreateRegion();
        bool isCq = GetBoolValue("cq");
        if (isCq)
          DoCqForMU();
      }
      catch (CacheClosedException ignore)
      {
        string message = ignore.Message;
        Util.Log(message);
      }
      catch (Exception ex)
      {
        FwkException("Caught unexpected exception during CacheClose {0}", ex);
      }
    }

    public void DoValidateCqOperationsForPerUser() {
      FwkInfo("MultiUserSecurity.DoValidateCqOperationsForPerUser() called.");
      try
      {
        string uName = (String)userList[0];
        Region region = proxyRegionMap[uName];
        IRegionService authCache = authCacheMap[uName];
        QueryService qs = authCache.GetQueryService();
        ICqListener cqLstner = new MyCqListener();
        for (Int32 i = 0; i < userList.Count; i++)
        {
          string userName = (String)userList[i];
          string cqName = String.Format("cq-{0}", userName);
          CqQuery cq = qs.GetCq(cqName);
          CqStatistics cqStats = cq.GetStatistics();
          CqAttributes cqAttr = cq.GetCqAttributes();
          ICqListener[] vl = cqAttr.getCqListeners();
          cqLstner = vl[0];
          MyCqListener myLisner = (MyCqListener)cqLstner;
          Util.Log("My count for cq {0} Listener : NumInserts:{1}, NumDestroys:{2}, NumUpdates:{3}, NumEvents:{4}", cq.Name, myLisner.NumInserts(), myLisner.NumDestroys(), myLisner.NumUpdates(), myLisner.NumEvents());
          Util.Log("Cq {0} from CqStatistics : NumInserts:{1}, NumDestroys:{2}, NumUpdates:{3}, NumEvents:{4} ", cq.Name, cqStats.numInserts(), cqStats.numDeletes(), cqStats.numUpdates(), cqStats.numEvents());
          if (myLisner.NumInserts() == cqStats.numInserts() && myLisner.NumUpdates() == cqStats.numUpdates() && myLisner.NumDestroys() == cqStats.numDeletes() && myLisner.NumEvents() == cqStats.numEvents())
            Util.Log("Accumulative event count is correct");
          else
            Util.Log("Accumulative event count is incorrect");
        }
      }
      catch (Exception ex)
      {
        FwkException("Query.DoVerifyCQListenerInvoked() Caught Exception : {0}", ex);
      }
    }

    public void DoEntryOperationsForMU()
    {
      FwkInfo("MultiUserSecurity.DoEntryOperationsForMU() called.");
      Util.RegisterTestCompleteDelegate(TestComplete);
      // string bb = "GFE_BB" ;
     // string ky="scheme" ;
    //  string securitySch=(String)Util.BBGet(bb,ky);
      
      int opsSec = GetUIntValue(OpsSecond);
      opsSec = (opsSec < 1) ? 0 : opsSec;

      int entryCount = GetUIntValue(EntryCount);
      entryCount = (entryCount < 1) ? 10000 : entryCount;

      int secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;

      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);

      CacheableKey key;
      CacheableString value;
      IGFSerializable val;

      CqQuery cq = null;
      string opCode = null;

      PaceMeter pm = new PaceMeter(opsSec);
      while (now < end)
      {
        int addOps = 1;
        Random rdm = new Random();
        int userSize = userList.Count;
        string userName = (String)userList[(rdm.Next(userSize))];
        FwkInfo("The userName is {0}",userName);
        Region region = proxyRegionMap[userName];
        IRegionService authCache = authCacheMap[userName];
        opCode = GetStringValue(EntryOps);
        try
        {
          UpdateOperationMap(opCode,userName,1);
          if (opCode == null || opCode.Length == 0)
          {
            opCode = "no-op";
          }

          if (opCode == "create")
          {
            key = new CacheableString(rdm.Next(entryCount).ToString());
            value = CacheableString.Create("Value");
            region.Create(key, value);
          }
          else
          {
            key = new CacheableString(rdm.Next(entryCount).ToString());
            if (opCode == "update")
            {
              value = CacheableString.Create("Value_");
              region.Put(key, value);
            }
            else if (opCode == "destroy")
            {
              region.Destroy(key);
            }
            else if (opCode == "get")
            {
              val = region.Get(key);
            }

            else if (opCode == "getServerKeys")
            {
              ICacheableKey[] serverKeys = region.GetServerKeys();
            }

            else if (opCode == "query")
            {
              QueryService qs = authCache.GetQueryService();
              Query qry = qs.NewQuery("select distinct * from /Portfolios where FALSE");
              ISelectResults result = qry.Execute(600);
            }
            else if (opCode == "cq")
            {
              string cqName = String.Format("cq-{0}",userName);
              QueryService qs = authCache.GetQueryService();
              CqAttributesFactory cqFac = new CqAttributesFactory();
              ICqListener cqLstner = new MyCqListener();
              cqFac.AddCqListener(cqLstner);
              CqAttributes cqAttr = cqFac.Create();
              cq = qs.NewCq(cqName, "select * from /Portfolios where FALSE", cqAttr, false);
              cq.Execute();
              cq.Stop();
              cq.Execute();
              cq.Close();
            }
            else if (opCode == "executefunction")
            {
              bool getResult = true;
              string funcName = null;
              Random rdn = new Random();
              int num = rdn.Next(3);
              Execution exc = null;
              IGFSerializable[] executeFunctionResult = null;
              CacheableVector args = new CacheableVector();
              IGFSerializable[] filterObj = new IGFSerializable[1];
              filterObj[0] = new CacheableString(rdm.Next(entryCount).ToString());
              //args.Add(filterObj[0]);
              Util.Log("Inside FE num = {0}",num);
              if (num == 0)
              {
                args.Add(filterObj[0]);
                args.Add(new CacheableString("addKey")); 
                funcName = "RegionOperationsFunction";
                exc = FunctionService.OnRegion(region);
                executeFunctionResult = exc.WithArgs(args).WithFilter(filterObj).Execute(funcName, getResult, 15, true, true).GetResult();
              }
              else if (num == 1)
              {
                args.Add(filterObj[0]);
                funcName = "ServerOperationsFunction";
                //exc = region.Cache.GetFunctionService().OnServer();
                exc = FunctionService.OnServer(authCache);
                executeFunctionResult = exc.WithArgs(args).Execute(funcName, getResult, 15, true, true).GetResult();
              }
              else
              {
                try
                {
                  args.Add(filterObj[0]);
                  funcName = "ServerOperationsFunction";
                  //exc = region.Cache.GetFunctionService().OnServers();
                  exc = FunctionService.OnServers(authCache);
                  executeFunctionResult = exc.WithArgs(args).Execute(funcName, getResult, 15, true, true).GetResult();
                }
                catch (FunctionExecutionException)
                {
                  //expected exception
                  Util.Log("Inside FunctionExecutionException");
                }
              }
            }
            else if (opCode == "putAll")
            {
              CacheableHashMap map = new CacheableHashMap();
              for (int count = 0; count < 200; count++)
              {
                key = new CacheableString(count.ToString());
                value = CacheableString.Create("Value_");
                map.Add(key, value);
              }
              region.PutAll(map);
            }
            else
            {
              FwkException("CacheServer.DoEntryOperationsForSecurity() " +
                "Invalid operation specified: {0}", opCode);
            }
          }
        }
        catch (NotAuthorizedException ex)
        {
         
          FwkInfo("Got expected NotAuthorizedException for operation {0}: {1}",
            opCode, ex.Message);
          if (opCode == "cq")
          {
            if (cq.IsStopped())
              cq.Close();
          }
          UpdateExceptionMap(opCode,userName,1);
        }
        catch (EntryExistsException)
        {
          addOps = -1;
          
       UpdateOperationMap(opCode,userName,addOps);
        }
        catch (EntryNotFoundException)
        {
          addOps = -1;
        UpdateExceptionMap(opCode,userName,addOps);
        }
        catch (EntryDestroyedException)
        {
          addOps = -1;
          UpdateExceptionMap(opCode, userName, addOps);
        }
        catch (CqExistsException)
        {
          Util.Log("Inside CqExistsException ");
        }
        catch (TimeoutException ex)
        {
          FwkSevere("Caught unexpected timeout exception during entry {0} " +
            " operation: {1}; continuing with test.", opCode, ex.Message);
        }
        catch (Exception ex)
        {
          FwkException("MultiUserSecurity.DoEntryOperationsForSecurity() Caught " +
            "unexpected exception during entry '{0}' operation: {1}.",
            opCode, ex);
        }
        pm.CheckPace();
        now = DateTime.Now;
      }
      key = null;
      value = null;
      FwkInfo("MultiUserSecurity.DoEntryOperationsForSecurity() complete");
    }

    public void UpdateOperationMap(string opcode, string userName,int numOps)
    {
      FwkInfo("Inside updateOperationMap");
      lock (((IDictionary)operationMap).SyncRoot)
      {
        Dictionary<string, int> opMap = operationMap[userName];
        int currentOps;
        if (!opMap.TryGetValue(opcode, out currentOps))
        {
          currentOps = 0;
        }
        opMap[opcode] = currentOps + numOps;
        operationMap[userName] = opMap;
      }
    }

    public void UpdateExceptionMap(string opcode, string userName, int numOps)
    {
      lock (((IDictionary)operationMap).SyncRoot)

      {
        Dictionary<string, int> expMap = exceptionMap[userName];
        int currentOps;
        if (!expMap.TryGetValue(opcode, out currentOps))
        {
          currentOps = 0;
        }
        expMap[opcode] = currentOps + numOps;
        exceptionMap[userName]=expMap;
      }
    }
    public int GetOpsFromMap(Dictionary<string, int> map, string opCode)
    {
      int numOps;
      lock (((ICollection)map).SyncRoot)
      {
        if (!map.TryGetValue(opCode, out numOps))
        {
          numOps = 0;
        }
      }
      return numOps;
    }

    public void DoValidateEntryOperationsForPerUser()
    {
      FwkInfo("Multiusersecurity.DoValidateEntryOperationsForPerUser() started");
      bool opFound = false;
      for (int i = 0; i < userList.Count; i++)
      {
        Dictionary<string, int> validateOpMap;
        Dictionary<string, int> validateExpMap;
        string userName = (String)userList[i];
        validateOpMap = operationMap[userName];
        validateExpMap = exceptionMap[userName];
        foreach (string opcode in validateOpMap.Keys) 
        {
          FwkInfo("opcode is {0} for user {1}", opcode,userName);
          int totalOpCnt = GetOpsFromMap(validateOpMap,opcode);
          int totalNotAuthCnt = GetOpsFromMap(validateExpMap, opcode);
          ArrayList roleList = new ArrayList();
          roleList = userToRolesMap[userName];
          for (int j = 0; j < roleList.Count; j++)
          {
            if ((String)roleList[j] == opcode)
            {
              opFound = true;
              break;
            }
            else
              opFound = false;
          }
          if (opFound)
          {
            if ((totalOpCnt != 0) && (totalNotAuthCnt == 0))
            {
              FwkInfo("Task passed sucessfully with total operation = {0} for user {1}", totalOpCnt, userName);
            }
            else
            {
              FwkException("Task failed for user {0} NotAuthorizedException found for operation = {1} while expected was 0", userName, totalNotAuthCnt);
            }
          }
          else
          {
            if (totalOpCnt == totalNotAuthCnt)
            {
              FwkInfo("Task passed sucessfully and got the expected number of notAuth exception = {0} with total number of operation = {1}", totalOpCnt, totalNotAuthCnt);
            }
            else
            {
              FwkException("Task failed ,Expected NotAuthorizedException cnt to be = {0} but found = {1}",totalOpCnt,totalNotAuthCnt);
            }
          }
        }
      }
    }

    public void DoCloseCache()
    {
      FwkInfo("DoCloseCache()  Closing cache and disconnecting from" +
        " distributed system.");
      userToRolesMap.Clear();
      proxyRegionMap.Clear();
      authCacheMap.Clear();
      CacheHelper.Close();
    }
    public static void TestComplete()
    {
      operationMap.Clear();
      exceptionMap.Clear();
    }

    #endregion
 
  }
   #endregion
}
