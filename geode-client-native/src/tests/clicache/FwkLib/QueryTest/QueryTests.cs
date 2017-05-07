//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Text;

using GemStone.GemFire.Cache.Tests; // for Portfolio and Position classes

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using System.Threading;
  using System.Xml.Serialization;
  using System.IO;
  using System.Reflection;

  public class MyCq1Listener : ICqListener
  {
    private UInt32 m_updateCnt;
    private UInt32 m_createCnt;
    private UInt32 m_destroyCnt;
    private UInt32 m_eventCnt;
    
    public MyCq1Listener()
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
      }
      else if (opType == CqOperationType.OP_TYPE_UPDATE)
      {
        m_updateCnt++;
      }
      else if (opType == CqOperationType.OP_TYPE_DESTROY)
      {
        m_destroyCnt++;
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

  public class QueryTests : FwkTest
   {
    public QueryTests()
      : base()
    {
    }

    #region Private constants

    private const UInt32 QueryResponseTimeout = 600;
    
    private const string QueryBB = "QueryBB";
    private const string DistinctKeys = "distinctKeys";
    private const string DestroyKeys = "destroyKeys";
    private const string InvalidateKeys = "invalidateKeys";
    private const string ObjectType = "objectType";
    private const string NumThreads = "numThreads";
    private const string ValueSizes = "valueSizes";
    private const string QueryString = "query";
    private const string QueryResultType = "queryResultType";
    private const string CategoryType = "categoryType";
    private const string ResultSize = "resultSize";
    private const string RegionPaths = "regionPaths";
    private const string RegionName = "regionName";
    private const string LargeSetQuery = "largeSetQuery";
    private const string IsDurable = "isDurable";
    private const string WorkTime = "workTime";
    private const string RegisterAndExecuteCq = "registerAndExecuteCQs";
    private const string CqState = "cqState";
    private static Cache m_cache = null;
    private static bool isObjectRegistered = false;
   
   
    #endregion

    #region Private utility methods

    private string GetNextRegionName(ref Region region)
    {
      string regionName = string.Empty;
      int count = 0;
      string path = string.Empty;

      do
      {
        path = GetStringValue(RegionPaths);

        if (path == null || path.Length <= 0)
        {
          return path;
        }

        FwkInfo("QueryTestGetNextRegionName() RegionPath is " + path);
        do
        {
          int length = path.Length;

          try
          {
            region = CacheHelper.DCache.GetRegion(path);
          }
          catch (Exception)
          {
          }

          if (region == null)
          {
            int pos = path.LastIndexOf('/');
            regionName = path.Substring(pos+1);
            path = path.Substring(0, pos);
          }
        }
        while (region == null && path.Length > 0);
      }
      while ((++count < 5) && regionName.Length <= 0);

      return regionName;
    }

    protected Region GetRegion()
    {
      return GetRegion(null);
    }

    protected Region GetRegion(string regionName)
    {
      Region region;
      if (regionName == null)
      {
        region = GetRootRegion();
      }
      else
      {
        region = CacheHelper.GetRegion(regionName);
      }
      return region;
    }

    private bool VerifyResultSet()
    {
      return VerifyResultSet(0);
    }

    private bool VerifyResultSet(int distinctKeys)
    {
      bool result = false;
      int numOfKeys;
      if (distinctKeys > 0)
      {
        numOfKeys = distinctKeys;
      }
      else
      {
        ResetKey(DistinctKeys);
        numOfKeys = GetUIntValue(DistinctKeys);
      }
      QueryHelper qh = QueryHelper.GetHelper();
      int setSize = qh.PortfolioSetSize;
      FwkInfo("QueryTests.VerifyResultSet: numOfKeys [{0}], setSize [{1}].",
        numOfKeys, setSize);
      if (numOfKeys < setSize)
      {
        setSize = numOfKeys;
      }
      int numSet = numOfKeys / setSize;

      QueryService qs = CheckQueryService();

      ResetKey(CategoryType);
      int category;

      ResetKey(LargeSetQuery);
      bool isLargeSetQuery = bool.Parse(GetStringValue(LargeSetQuery));
      ResetKey("cq");
      bool isCq = GetBoolValue("cq");
      ResetKey("paramquery");
      bool isParamquery = GetBoolValue("paramquery");
      int RSsize = 0;
      if (isCq)
        RSsize = QueryStrings.CQRSsize;
      else if(isParamquery)
        RSsize = QueryStrings.RSPsize;
      else
        RSsize = QueryStrings.RSsize;
      while ((category = GetUIntValue(CategoryType)) > 0)
      {
        for (int index = 0; index < RSsize; index++)
        {
          try
          {
            if (isCq)
            {
              if ((int)QueryStatics.CqResultSetQueries[index].Category == category - 1 &&
                QueryStatics.CqResultSetQueries[index].Category != QueryCategory.Unsupported &&
                QueryStatics.CqResultSetQueries[index].IsLargeResultset == isLargeSetQuery)
              {
                string CqQuery = QueryStatics.CqResultSetQueries[index].Query;
                FwkInfo("QueryTests.VerifyResultSet: Query Category [{0}]," +
              " CqQueryString [{1}], NumSet [{2}].", category - 1, CqQuery, numSet);

                ISelectResults results = ContinuousQuery(qs, QueryStatics.CqResultSetQueries[index].Query, index);

                if ((category - 1 != (int)QueryCategory.Unsupported) 
                  && (!qh.VerifyRS(results, qh.IsExpectedRowsConstantCQRS(index) ?
                   QueryStatics.CqResultSetRowCounts[index] :
                   QueryStatics.CqResultSetRowCounts[index] * numSet)))
                {
                  FwkSevere("QueryTests.VerifyStructSet: Query verify failed" +
                    " for query index {0}", index);
                  return false;
                }
                else
                {
                  result = true;
                }
              }
            }
            else if (isParamquery)
            {
              if ((int)QueryStatics.ResultSetParamQueries[index].Category == category - 1 &&
                QueryStatics.ResultSetParamQueries[index].Category != QueryCategory.Unsupported &&
                QueryStatics.ResultSetParamQueries[index].IsLargeResultset == isLargeSetQuery)
              {
                string query = QueryStatics.ResultSetParamQueries[index].Query;
                FwkInfo("QueryTests.VerifyResultSet: Query Category [{0}]," +
                " QueryString [{1}], NumSet [{2}].", category - 1, query, numSet);
                IGFSerializable[] paramList = new IGFSerializable[QueryStatics.NoOfQueryParam[index]];
                Int32 numVal = 0;
                for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParam[index]; ind++)
                {
                  try
                  {
                    numVal = Convert.ToInt32(QueryStatics.QueryParamSet[index][ind]);
                    paramList[ind] = new CacheableInt32(numVal);
                  }
                  catch (FormatException)
                  {
                    paramList[ind] = new CacheableString((System.String)QueryStatics.QueryParamSet[index][ind]);
                  }
                }
                Query qry = qs.NewQuery(query);
                DateTime startTime;
                DateTime endTime;
                TimeSpan elapsedTime;
                startTime = DateTime.Now;
                ISelectResults results = qry.Execute(paramList, QueryResponseTimeout);
                endTime = DateTime.Now;
                elapsedTime = endTime - startTime;
                FwkInfo("QueryTests.VerifyResultSet: Time Taken to execute" +
                  " the query [{0}]: {1}ms", query,
                  elapsedTime.TotalMilliseconds);

                FwkInfo("QueryTests.VerifyResultSet: ResultSet size [{0}]," +
                  " numSet [{1}], setRowCount [{2}]", results.Size, numSet,
                  QueryStatics.ResultSetRowCounts[index]);
                if ((category - 1 != (int)QueryCategory.Unsupported) &&
                  (!qh.VerifyRS(results, qh.IsExpectedRowsConstantPQRS(index) ?
                  QueryStatics.ResultSetPQRowCounts[index] :
                  QueryStatics.ResultSetPQRowCounts[index] * numSet)))
                {
                  FwkSevere("QueryTests.VerifyStructSet: Param Query verify failed" +
                    " for query index {0}", index);
                  return false;
                }
                else
                {
                  result = true;
                }
              }
            }
            else
            {
              if ((int)QueryStatics.ResultSetQueries[index].Category == category - 1 &&
                QueryStatics.ResultSetQueries[index].Category != QueryCategory.Unsupported &&
                QueryStatics.ResultSetQueries[index].IsLargeResultset == isLargeSetQuery)
              {
                string query = QueryStatics.ResultSetQueries[index].Query;
                FwkInfo("QueryTests.VerifyResultSet: Query Category [{0}]," +
                " QueryString [{1}], NumSet [{2}].", category - 1, query, numSet);

                ISelectResults results = RemoteQuery(qs, QueryStatics.ResultSetQueries[index].Query);

                FwkInfo("QueryTests.VerifyResultSet: ResultSet size [{0}]," +
                  " numSet [{1}], setRowCount [{2}]", results.Size, numSet,
                  QueryStatics.ResultSetRowCounts[index]);
                if ((category - 1 != (int)QueryCategory.Unsupported) &&
                  (!qh.VerifyRS(results, qh.IsExpectedRowsConstantRS(index) ?
                  QueryStatics.ResultSetRowCounts[index] :
                  QueryStatics.ResultSetRowCounts[index] * numSet)))
                {
                  FwkSevere("QueryTests.VerifyStructSet: Query verify failed" +
                    " for query index {0}", index);
                  return false;
                }
                else
                {
                  result = true;
                }
              }
            }
          }
          catch (Exception)
          {
            if (category - 1 != (int)QueryCategory.Unsupported)
            {
              throw;
            }
          }
        }
        category = GetUIntValue("categoryType");
        GC.Collect();
        GC.WaitForPendingFinalizers();
      }
      return result;
    }
   
    private bool VerifyStructSet()
    {
      return VerifyStructSet(0);
    }

    private bool VerifyStructSet(int distinctKeys)
    {
      bool result = false;
      int numOfKeys;
      if (distinctKeys > 0)
      {
        numOfKeys = distinctKeys;
      }
      else
      {
        ResetKey(DistinctKeys);
        numOfKeys = GetUIntValue(DistinctKeys);
      }
      QueryHelper qh = QueryHelper.GetHelper();
      int setSize = qh.PortfolioSetSize;
      FwkInfo("QueryTests.VerifyStructSet: numOfKeys [{0}], setSize [{1}].",
        numOfKeys, setSize);
      if (numOfKeys < setSize)
      {
        setSize = numOfKeys;
      }
      int numSet = numOfKeys / setSize;

      QueryService qs = CheckQueryService();

      DateTime startTime;
      DateTime endTime;
      TimeSpan elapsedTime;
      ResetKey(CategoryType);
      int category;

      ResetKey(LargeSetQuery);
      bool isLargeSetQuery = bool.Parse(GetStringValue(LargeSetQuery));
      ResetKey( "paramquery" );
      bool isParamquery = GetBoolValue( "paramquery" );
      int SSsize = 0;
      if(isParamquery)
        SSsize = QueryStrings.SSPsize;
      else
        SSsize = QueryStrings.SSsize;
      while ((category = GetUIntValue(CategoryType)) > 0)
      {
        for (int index = 0; index < SSsize; index++)
        {
          if(isParamquery) {
          if ((int)QueryStatics.StructSetParamQueries[index].Category == category - 1 &&
            QueryStatics.StructSetParamQueries[index].Category != QueryCategory.Unsupported &&
            QueryStatics.StructSetParamQueries[index].IsLargeResultset == isLargeSetQuery)
          {
            string query = QueryStatics.StructSetParamQueries[index].Query;
            FwkInfo("QueryTests.VerifyStructSet: Query Category [{0}]," +
              " String [{1}], NumSet [{2}].", category - 1, query, numSet);
            try
            {
              Query qry = qs.NewQuery(query);
              IGFSerializable[] paramList = new IGFSerializable[QueryStatics.NoOfQueryParamSS[index]];

              Int32 numVal = 0;
              for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[index]; ind++)
              {
                try
                {
                  numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[index][ind]);
                  paramList[ind] = new CacheableInt32(numVal);
               }
                catch (FormatException )
                {
                  paramList[ind] = new CacheableString((System.String)QueryStatics.QueryParamSetSS[index][ind]);
                }
              }
              startTime = DateTime.Now;
              ISelectResults results = qry.Execute(paramList,QueryResponseTimeout);
              endTime = DateTime.Now;
              elapsedTime = endTime - startTime;
              FwkInfo("QueryTests.VerifyStructSet: Time Taken to execute" +
                " the query [{0}]: {1}ms", query,
                elapsedTime.TotalMilliseconds);
              FwkInfo("QueryTests.VerifyStructSet: StructSet size [{0}]," +
                " numSet [{1}], setRowCount [{2}]", results.Size, numSet,
                QueryStatics.StructSetPQRowCounts[index]);
              if ((category - 1 != (int)QueryCategory.Unsupported) &&
                (!qh.VerifySS(results, qh.IsExpectedRowsConstantPQSS(index) ?
                QueryStatics.StructSetPQRowCounts[index] :
                QueryStatics.StructSetPQRowCounts[index] * numSet,
                QueryStatics.StructSetPQFieldCounts[index])))
              {
                FwkSevere("QueryTests.VerifyStructSet: Param Query verify failed" +
                  " for query index {0}", index);
                return false;
              }
              else
              {
                result = true;
              }
            }
            catch
            {
              if (category - 1 != (int)QueryCategory.Unsupported)
              {
                throw;
              }
            }
          }
        }
          else {
          if ((int)QueryStatics.StructSetQueries[index].Category == category-1 &&
            QueryStatics.StructSetQueries[index].Category != QueryCategory.Unsupported &&
            QueryStatics.StructSetQueries[index].IsLargeResultset == isLargeSetQuery)
          {
            string query = QueryStatics.StructSetQueries[index].Query;
            FwkInfo("QueryTests.VerifyStructSet: Query Category [{0}]," +
              " String [{1}], NumSet [{2}].", category-1, query, numSet);
            try
            {
              Query qry = qs.NewQuery(query);
              startTime = DateTime.Now;
              ISelectResults results = qry.Execute(QueryResponseTimeout);
              endTime = DateTime.Now;
              elapsedTime = endTime - startTime;
              FwkInfo("QueryTests.VerifyStructSet: Time Taken to execute" +
                " the query [{0}]: {1}ms", query,
                elapsedTime.TotalMilliseconds);
              FwkInfo("QueryTests.VerifyStructSet: StructSet size [{0}]," +
                " numSet [{1}], setRowCount [{2}]", results.Size, numSet,
                QueryStatics.StructSetRowCounts[index]);
              if ((category-1 != (int)QueryCategory.Unsupported) &&
                (!qh.VerifySS(results, qh.IsExpectedRowsConstantSS(index) ?
                QueryStatics.StructSetRowCounts[index] :
                QueryStatics.StructSetRowCounts[index] * numSet,
                QueryStatics.StructSetFieldCounts[index])))
              {
                FwkSevere("QueryTests.VerifyStructSet: Query verify failed" +
                  " for query index {0}", index);
                return false;
              }
              else
              {
                result = true;
              }
            }
            catch
            {
              if (category - 1 != (int)QueryCategory.Unsupported)
              {
                throw;
              }
            }
          }
        }
          GC.Collect();
          GC.WaitForPendingFinalizers();
        }
      }
      return result;
    }

    private bool ReadQueryString(ref String query)
    {
      bool isCq = GetBoolValue("cq");
      do
      {
        int resultSize = GetUIntValue(ResultSize);  // set the query result size in xml
        FwkInfo("ReadQueryString: Query String is: {0}, resultSize: {1}",
          query, resultSize);
        if (resultSize < 0)
        {
          FwkSevere("ReadQueryString: ResultSize is not defined in xml.");
          return false;
        }
        QueryService qs = CheckQueryService();
        DateTime startTime;
        TimeSpan elapsedTime;
        ISelectResults results;
        if (isCq)
        {
          string CqName = String.Format("_default{0}",query);
          CqQuery cq = qs.GetCq(CqName);
          startTime = DateTime.Now;
          results = cq.ExecuteWithInitialResults(QueryResponseTimeout);
          elapsedTime = DateTime.Now - startTime;
          FwkInfo("ReadQueryString: Time Taken to execute the CqQuery [{0}]:" +
          "{1}ms ResultSize Size = {2}", query, elapsedTime.TotalMilliseconds ,results.Size);
        }
        else
        {
          startTime = DateTime.Now;
          Query qry = qs.NewQuery(query);
          results = qry.Execute(QueryResponseTimeout);
          elapsedTime = DateTime.Now - startTime;
          FwkInfo("ReadQueryString: Time Taken to execute the query [{0}]:" +
            "{1}ms", query, elapsedTime.TotalMilliseconds);
        }
        if (resultSize != results.Size)
        {
          FwkSevere("ReadQueryString: Result size found {0}, expected {1}.",
            results.Size, resultSize);
          return false;
        }
        FwkInfo("ReadQueryString: Got expected result size {0}.",
          results.Size);
      } while ((query = GetStringValue(QueryString)) != null &&
        query.Length > 0);
      return true;
    }

    #endregion

    #region Public methods
    public void DoCreateDCRegion()
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
              ResetKey("sslEnable");
              bool isSslEnable = GetBoolValue("sslEnable");
              string mode = Util.GetEnvironmentVariable("POOLOPT");
              if (mode == "poolwithendpoints" || mode == "poolwithlocator")
              {
                CacheHelper.InitConfigForPoolDurable(durableClientId, durableTimeout, conflateEvents, isSslEnable);
              }
              else
              {
                FwkInfo("Setting the cache-level endpoints to {0}", endpoints);
                CacheHelper.InitConfigForDurable(endpoints, redundancyLevel, durableClientId, durableTimeout, conflateEvents, isSslEnable);
              }

            }
            RegionFactory rootAttrs = CacheHelper.DCache.CreateRegionFactory(RegionShortcut.PROXY);
            SetRegionAttributes(rootAttrs, rootRegionData, ref m_isPool);
            rootAttrs = CreatePool(rootAttrs, redundancyLevel);
            region = CacheHelper.CreateRegion(rootRegionName, rootAttrs);
            RegionAttributes regAttr = region.Attributes;
            FwkInfo("Region attributes for {0}: {1}", rootRegionName,
              CacheHelper.RegionAttributesToString(regAttr));

            
          }
        }
      }
      else
      {
        FwkSevere("DoCreateDCRegion() failed to create region");
      }

      FwkInfo("DoCreateDCRegion() complete.");
    }
    public void DoCreateUserDefineRegion()
    {
      FwkInfo("In QueryTest.DoCreateUserDefineRegion()");
      try
      {
        if (!isObjectRegistered)
        {
          Serializable.RegisterType(Portfolio.CreateDeserializable);
          Serializable.RegisterType(Position.CreateDeserializable);
          isObjectRegistered = true;
        }
        string regionName = GetStringValue(RegionName);
        bool isDurable = GetBoolValue("isDurable");
        if (isDurable)
        {
          DoCreateDCRegion();
        }
        else
        {
          Region region = CreateRootRegion(regionName);

          string key = region.Name;
          Util.BBIncrement(QueryBB, key);

          FwkInfo("QueryTest.DoCreateUserDefineRegion() Created Region " +
            region.Name);
        }
        
      }
      catch (Exception ex)
      {
        FwkException("QueryTest.DoCreateUserDefineRegion() Caught Exception: {0}", ex);
      }
    }
    
    public void DoRegisterCq()
    {
      FwkInfo("In Query.DoRegisterCq()");
      try
      {
        int cqNum=0;
        string qryStr = GetStringValue(QueryString);
        string clientId = Util.ClientId;
        string key = "CQLISTENER_" + clientId;
        while (qryStr != null)
        {
          QueryService qs = CheckQueryService();
          CqAttributesFactory cqFac = new CqAttributesFactory();
          ICqListener cqLstner = new MyCq1Listener();
          cqFac.AddCqListener(cqLstner);
          CqAttributes cqAttr = cqFac.Create();
          string CqName = String.Format("cq-{0}", cqNum++);
          FwkInfo("Reading Query from xml {0} for cq {1}", qryStr, CqName);
          CqQuery qry = qs.NewCq(CqName, qryStr, cqAttr, false);
          ISelectResults results = qry.ExecuteWithInitialResults(QueryResponseTimeout);
          Util.BBIncrement("CqListenerBB", key);
        }
      }
      catch(Exception ex)
      {
        FwkException("Query.DoRegisterCq() Caught Exception: {0}", ex);
      }
    }

    public void DoRegisterCqForConc()
    {
      FwkInfo("In Query.DoRegisterCqForConc()");
      try
      {
        string qryStr = GetStringValue(QueryString);
        string clientId = Util.ClientId;
        string key = "CQLISTENER_" + clientId;
        while (qryStr != null)
        {
          QueryService qs = CheckQueryService();
          CqAttributesFactory cqFac = new CqAttributesFactory();
          ICqListener cqLstner = new MyCq1Listener();
          cqFac.AddCqListener(cqLstner);
          CqAttributes cqAttr = cqFac.Create();
          CqQuery qry = qs.NewCq(qryStr, cqAttr, false);
          FwkInfo("Registered Cq {0} with query {1}",qry.Name,qryStr);
          ResetKey(RegisterAndExecuteCq);
          bool regAndExcCq = GetBoolValue(RegisterAndExecuteCq);
          if (regAndExcCq)
          {
            FwkInfo("Inside Execute:");
            qry.Execute();
            FwkInfo("Inside Execute done:");
          }
          qryStr = GetStringValue(QueryString);
          Util.BBIncrement("CqListenerBB", key);
        }
      }
      catch (Exception ex)
      {
        FwkException("Query.DoRegisterCqForConc() Caught Exception: {0}", ex);
      }
    }

    public void DoVerifyCQListenerInvoked()
    {
      FwkInfo("In QueryTest.DoVerifyCQListenerInvoked()");
      try
      {
        QueryService qs = CheckQueryService();
        ICqListener cqLstner = new MyCq1Listener();
        CqQuery[] vcq=qs.GetCqs();
        for (int i = 0; i < vcq.Length; i++)
        {
          CqQuery cq = (CqQuery)vcq.GetValue(i);
          CqStatistics cqStats = cq.GetStatistics();
          CqAttributes cqAttr = cq.GetCqAttributes();
          ICqListener[] vl = cqAttr.getCqListeners();
          cqLstner = vl[0];
          MyCq1Listener myLisner = (MyCq1Listener)cqLstner;
          Util.Log("My count for cq {0} Listener : NumInserts:{1}, NumDestroys:{2}, NumUpdates:{3}, NumEvents:{4}",cq.Name,myLisner.NumInserts(),myLisner.NumDestroys(),myLisner.NumUpdates(),myLisner.NumEvents());
          Util.Log("Cq {0} from CqStatistics : NumInserts:{1}, NumDestroys:{2}, NumUpdates:{3}, NumEvents:{4} ",cq.Name,cqStats.numInserts(),cqStats.numDeletes(),cqStats.numUpdates(),cqStats.numEvents());
          if(myLisner.NumInserts()==cqStats.numInserts() && myLisner.NumUpdates()==cqStats.numUpdates() && myLisner.NumDestroys()==cqStats.numDeletes() && myLisner.NumEvents()==cqStats.numEvents())
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

    public void DoVerifyCqDestroyed()
    {
      FwkInfo("In QueryTest.DoVerifyCQDestroy()");
      try
      {
        QueryService qs = CheckQueryService();
        CqQuery[] vcq =qs.GetCqs();
        if (vcq.Length != 0)
        {
          FwkException("cqs should have been removed after close");
        }
      }
      catch (Exception ex)
      {
        FwkException("Query.DoVerifyCQDestroy() Caught exception {0}",ex);
      }
    }

    public void DoCQState()
    {
      FwkInfo("In QueryTest.DoCQState()");
      string opcode=null;
      try
      {
        opcode = GetStringValue(CqState);
        QueryService qs = CheckQueryService();
        CqQuery[] vcq = qs.GetCqs(); ;
        FwkInfo("QueryTest.DoCQState - number of cqs is {0} ", vcq.Length); 
        for (int i = 0; i < vcq.Length; i++)
        {
          CqQuery cq = (CqQuery)vcq.GetValue(i);
          if (opcode == "stopped")
            cq.Stop();
          else if (opcode == "closed")
            cq.Close();
          else if (opcode == "execute")
            cq.Execute();
          else
            FwkException("Invalid operation specified:");
        }
      }
      catch (Exception ex)
      {
        FwkException("Caught unexpected exception during cq {0} operation {1} exiting task ",opcode,ex);
      }
    }

    public void DoCQOperation()
    {
      FwkInfo("In QueryTest.DoCQOperation()");
      Region region = GetRegion();
      Int32 secondsToRun = GetTimeValue(WorkTime);
      secondsToRun = (secondsToRun < 1) ? 30 : secondsToRun;
      Int32 opsSec = GetUIntValue("opsSecond");
      opsSec = (opsSec < 1) ? 0 : opsSec;
      DateTime now = DateTime.Now;
      DateTime end = now.AddSeconds(secondsToRun);
      string opCode = null;
      Int32 getCQAttributes = 0, getCQName = 0, getQuery = 0, getQueryString = 0, getStatistics = 0, stopCq = 0, closeCq = 0, executeCq = 0, executeCqWithIR = 0;
      FwkInfo("Operation will work for : {0}" ,secondsToRun);
      PaceMeter pm = new PaceMeter(opsSec);
      QueryService qs = CheckQueryService();
      while (now < end)
      {
        try 
        {
          opCode=GetStringValue("cqOps");
          CqQuery[] vcq =qs.GetCqs();
          CqQuery cqs = (CqQuery)vcq.GetValue(Util.Rand(vcq.Length));
          FwkInfo("Performing {0} on cq named {1}",opCode,cqs.Name);
          if (opCode == null || opCode.Length == 0)
          {
            opCode = "no-op";
          }
          if(opCode=="getCQAttributes")
          {
            cqs.GetCqAttributes();
            getCQAttributes++;
          }
          else if (opCode == "getCQName")
          {
            string name = cqs.Name;
            getCQName++;
          }
          else if (opCode == "getQuery")
          {
            cqs.GetQuery();
            getQuery++;
          }
          else if (opCode == "getQueryString")
          {
            string queryString = cqs.QueryString;
            getQueryString++;
          }
          else if (opCode == "getStatistics")
          {
            cqs.GetStatistics();
            getStatistics++;
          }
          else if (opCode == "stopCq")
          {
            StopCQ(cqs);
            stopCq++;
          }
          else if (opCode == "closeCQ")
          {
            CloseCQ(cqs);
            closeCq++;
          }
          else if (opCode == "executeCQ")
          {
            ExecuteCQ(cqs);
            executeCq++;
          }
          else if (opCode == "executeCQWithIR")
          {
            ExecuteCQWithIR(cqs);
            executeCqWithIR++;
          }
          else
          {
            FwkException("Invalid operation specified {0}", opCode);
          }
        }
        catch(Exception ex)
        {
          FwkException("QueryTest.CqOperations() Caught " +
          "unexpected exception during entry '{0}' operation: {1}.",
          opCode, ex);
        }
        pm.CheckPace();
        now = DateTime.Now;
      }
      Util.Log("cqOperations did {0}getCQAttributes,{1} getCQName,{2}getQuery,{3}getQueryString,{4}getStatistics,{5}stopCQ,{6}closeCQ,{7}executeCQ,{8}executeCQWithIR ", getCQAttributes, getCQName, getQuery, getQueryString, getStatistics, stopCq, closeCq, executeCq, executeCqWithIR);
    }

    public void StopCQ(CqQuery cq)
    {
      try 
      {
        if (cq.IsRunning())
          cq.Stop();
        else if (cq.IsStopped())
        {
          try
          {
            cq.Stop();
            FwkException("QueryTest.StopCq : should have thrown IllegalStateException. executed stop() successfully on STOPPED CQ");
          }
          catch(IllegalStateException)
          { }
        }
        else if (cq.IsClosed())
        {
          try
          {
            cq.Stop();
            FwkException("QueryTest::stopCQ : should have thrown CQClosedException. executed stop() successfully on CLOSED CQ");
          }
          catch(Exception)
          {
            ReRegisterCQ(cq);
          }
        }
      }
      catch(Exception ex)
      {
        FwkException("QueryTest::stopCQ : Caught unexpected exception during cq stop operation :{0}",ex);
      }
    }

    public void CloseCQ(CqQuery cq)
    {
      try
      {
        if (cq.IsRunning() || cq.IsStopped())
          cq.Close();
        if (cq.IsClosed())
        {
          try
          {
            cq.Close();
            FwkInfo("Cq {0} is closed ,hence reregistering it before execution");
            ReRegisterCQ(cq);
          }
          catch( CacheClosedException )//should be CqClosedException
          {
            FwkException("QueryTest::CloseCQ : Should not have thrown CQClosedException. close() on CLOSED query is not successful");
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("QueryTest::closeCQ : Caught unexpected exception during cq close operation: {0}", ex);
      }
    }

    public void ExecuteCQ(CqQuery cq)
    {
      try
      {
        if (cq.IsStopped())
          cq.Execute();
        else if (cq.IsRunning())
        {
          try
          {
            cq.Execute();
            FwkException("QueryTest.ExecuteCq : should have thrown IllegalStateException. Execute on RUNNING query is successful");
          }
          catch (IllegalStateException)
          { }
        }
        if (cq.IsClosed())
        {
          try
          {
            cq.Execute();
            FwkException( "QueryTest::ExecuteCQ : Should have thrown CQClosedException. execute() on CLOSED query is successful");
          }
          catch(CacheClosedException) //CqCloseException
          {
            FwkInfo("Cq {0} is destroyed ,hence registering it before execution",cq.Name);
            ReRegisterCQ(cq);
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("QueryTest::ExecuteCQ : Caught unexpected exception during cq execute operation:{0} ",ex);
      }
    }

    public void ExecuteCQWithIR(CqQuery cq)
    {
      try
      {
        if (cq.IsStopped())
        {
          ISelectResults results = cq.ExecuteWithInitialResults();
          FwkInfo("Executed query {0} with initial results",cq.Name);
        }
        else if (cq.IsRunning())
        {
          try
          {
            cq.Execute();
            FwkException("QueryTest::ExecuteCQWithIR : Should have thrown IllegalStateException. executeWithInitialResults on RUNNING query is successful");
          }
          catch (IllegalStateException)
          {// expected
          }
        }
        if (cq.IsClosed())
        {
          try
          {
            cq.ExecuteWithInitialResults();
            FwkException( "QueryTest::ExecuteCQWithIR: Should have thrown CQClosedException. executeWithInitialResults() on CLOSED query is succussful");
          }
          catch (CacheClosedException) //CqClosedException
          {
            FwkInfo( "CQ:- {0} is closed hence registering it before executeWithInitialResults",cq.Name);
            ReRegisterCQ(cq);
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("QueryTest::executeCQWithIR : Caught unexpected exception during cq executeWithIR operation: {0}",ex);
      }
     }
    
    public void ReRegisterCQ(CqQuery cq)
    {
      try
      {
        FwkInfo("re-registering Cq: {0}",cq.Name);
        string query = cq.QueryString;
        CqAttributesFactory cqFac = new CqAttributesFactory();
        ICqListener cqLstner = new MyCq1Listener();
        cqFac.AddCqListener(cqLstner);
        CqAttributes cqAttr=cqFac.Create();
        QueryService qs = CheckQueryService();
        qs.NewCq(query, cqAttr,false);
      }
      catch (Exception ex)
      {
        FwkException("Caught unexpected exception during ReRegisterCq() {0}",ex);
      }
    }

    public void DoCloseCacheAndReInitialize()
    {
      FwkInfo("In QueryTest.DoCloseCacheAndReInitialize()");
      try
      {
        if (m_cache != null)
        {
          Region[] vregion = CacheHelper.DCache.RootRegions();
          try
          {
            for (Int32 i = 0; i < vregion.Length; i++)
            {
              Region region =(Region)vregion.GetValue(i);
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
            m_cache.Close(keepalive);
          }
          else
            m_cache.Close();
          m_cache = null;
          FwkInfo("Cache Close");
        }
        CacheHelper.Close();
          DoRestartClientAndRegInt();
        bool isCq = GetBoolValue("cq");
        if (isCq)
          DoRegisterCqForConc();
      }
      catch (CacheClosedException ignore)
      {
        string message = ignore.Message;
        Util.Log(message);
      }
      catch (Exception ex)
      {
        FwkException("Caught unexpected exception during CacheClose {0}",ex);
      }
    }

    public void DoRestartClientAndRegInt()
    {
      int sleepSec = GetUIntValue("restartTime");
      if (sleepSec > 0 )
        Thread.Sleep(sleepSec * 1000);
      DoCreateUserDefineRegion();
      string name = GetStringValue("regionName");
      Region region = GetRegion(name);
      bool isDurable = GetBoolValue("isDurableReg");
      if (region == null)
        FwkInfo("DoRestartClientAndRegInt() region is null");
      else
        FwkInfo("DoRestartClientAndRegInt()region not null");
      region.RegisterAllKeys(isDurable);
      int waitSec = GetUIntValue("updateReceiveTime");
      if (waitSec > 0)
        Thread.Sleep(waitSec * 1000);
      FwkInfo("DoRestartClientAndRegInt() complete.");
    }

    public void DoRegisterAllKeys()
    {
      FwkInfo("In QueryTest.DoRegisterAllKeys");
      try
      {
        Region region = GetRegion();
        FwkInfo("QueryTest::registerAllKeys region name is {0}",region.Name);
        bool isDurable = GetBoolValue("isDurableReg");
        region.RegisterAllKeys(isDurable);
        
      }
      catch (Exception ex)
      {
        FwkException("QueryTest.DoRegisterAllKeys() caught exception {0}",ex);
      }
    }

  public ISelectResults RemoteQuery(QueryService qs, string queryStr)
    {
      DateTime startTime;
      DateTime endTime;
      TimeSpan elapsedTime;
      Query qry = qs.NewQuery(queryStr);
      startTime = DateTime.Now;
      ISelectResults results = qry.Execute(QueryResponseTimeout);
      endTime = DateTime.Now;
      elapsedTime = endTime - startTime;
      FwkInfo("QueryTest.RemoteQuery: Time Taken to execute" +
            " the query [{0}]: {1}ms", queryStr, elapsedTime.TotalMilliseconds);
      return results;
    }

   public ISelectResults ContinuousQuery(QueryService qs, string queryStr, int cqNum)
    {
      DateTime startTime ,endTime;
      TimeSpan elapsedTime;
      CqAttributesFactory cqFac = new CqAttributesFactory();
      ICqListener cqLstner = new MyCq1Listener();
      cqFac.AddCqListener(cqLstner);
      CqAttributes cqAttr = cqFac.Create();
      startTime = DateTime.Now;
      string CqName = String.Format("cq-{0}", cqNum);
      CqQuery cq = qs.NewCq(CqName,queryStr,cqAttr,false);
      ISelectResults results = cq.ExecuteWithInitialResults(QueryResponseTimeout);
      endTime = DateTime.Now;
      elapsedTime = endTime - startTime;
      return results;
    }
    
    public void DoAddRootAndSubRegion()
    {
      FwkInfo("In QueryTest.DoAddRootAndSubRegion()");
      try
      {
        Serializable.RegisterType(Portfolio.CreateDeserializable);
        Serializable.RegisterType(Position.CreateDeserializable);
        Region parentRegion = null;
        ResetKey(RegionPaths);
        string sRegionName;
        string rootRegionData = GetStringValue("regionSpec");
        string tagName = GetStringValue("TAG");
        string endpoints = Util.BBGet(JavaServerBB, EndPointTag + tagName)
          as string;
        while ((sRegionName = GetNextRegionName(ref parentRegion)) != null &&
          sRegionName.Length > 0)
        {
          Region region;
          if (parentRegion == null)
          {
            region = CreateRootRegion(sRegionName, rootRegionData, endpoints);
          }
          else
          {
            string fullName = parentRegion.FullPath;
            RegionAttributes regattrs = parentRegion.Attributes;
            parentRegion.CreateSubRegion(sRegionName, regattrs);

            Util.BBSet(QueryBB, sRegionName, fullName);
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("QueryTest.DoAddRootAndSubRegion() Caught exception: {0}", ex);
      }
    }

    public void DoDestroyUserObject()
    {
      FwkInfo("In QueryTests.DoDestroyUserObject()");
      try
      {
        string name = GetStringValue("regionName");
        Region region = GetRegion(name);
        string label = CacheHelper.RegionTag(region.Attributes);
        ResetKey(DistinctKeys);
        int numOfKeys = GetUIntValue(DistinctKeys);
        QueryHelper qh = QueryHelper.GetHelper();
        string objectType = GetStringValue(ObjectType);
        int setSize = qh.PortfolioSetSize;
        int destroyKeys = GetUIntValue(DestroyKeys);
        if (destroyKeys <= 0 && numOfKeys < setSize)
        {
          FwkException("QueryTests.DoDestroyUserObject() Number of keys should be multiple of 20");
        }
        else
        {
          numOfKeys = destroyKeys;
        }

        int numSet = numOfKeys / setSize;

        FwkInfo("QueryTests.DoDestroyUserObject() Destroying " + numOfKeys + " keys, destroyKeys=" + destroyKeys +
          ", setSize=" + setSize + ", numSet=" + numSet);

        qh.DestroyPortfolioOrPositionData(region, setSize, numSet, objectType);

        FwkInfo("QueryTests.DoDestroyUserObject() Destroyed user objects.");
      }
      catch (Exception ex)
      {
        FwkException("QueryTests.DoDestroyUserObject() Caught Exception: {0}", ex);
      }
    }

    public void DoInvalidateUserObject()
    {
      FwkInfo("In QueryTests.DoInvalidateUserObject().");
      try
      {
        string name = GetStringValue("regionName");
        Region region = GetRegion(name);
        string label = CacheHelper.RegionTag(region.Attributes);
        ResetKey(DistinctKeys);
        int numOfKeys = GetUIntValue(DistinctKeys);
        QueryHelper qh = QueryHelper.GetHelper();
        string objectType = GetStringValue(ObjectType);
        int setSize = qh.PortfolioSetSize;
        int invalidateKeys = GetUIntValue(InvalidateKeys);
        if (invalidateKeys <= 0 && numOfKeys < setSize)
        {
          FwkException("QueryTests.DoInvalidateUserObject() Number of keys should be multiple of 20");
        }
        else
        {
          numOfKeys = invalidateKeys;
        }

        int numSet = numOfKeys / setSize;

        FwkInfo("QueryTests.DoInvalidateUserObject() Invalidating " + numOfKeys +
          " keys, invalidateKeys=" + invalidateKeys + ", setSize=" + setSize + ", numSet=" + numSet);

        qh.InvalidatePortfolioOrPositionData(region, setSize, numSet, objectType);

        FwkInfo("QueryTests.DoInvalidateUserObject() Destroyed user objects.");
      }
      catch (Exception ex)
      {
        FwkException("QueryTests.DoInvalidateUserObject() caught Exception: {0}", ex);
      }
    }

    public void DoPopulateUserObject()
    {
      FwkInfo("In QueryTest.PopulateUserObject().");
      try
      {
        string name = GetStringValue("regionName");
        Region region = GetRegion(name);
        string label = CacheHelper.RegionTag(region.Attributes);
        ResetKey(DistinctKeys);
        int numOfKeys = GetUIntValue(DistinctKeys); // number of keys should be multiple of 20
        QueryHelper qh = QueryHelper.GetHelper();
        int numSet = 0;
        int setSize = 0;
        // Loop over value sizes
        ResetKey(ValueSizes);
        int objSize;
        string objectType = GetStringValue(ObjectType);
        while ((objSize = GetUIntValue(ValueSizes)) > 0)
        { // value sizes loop
          if (objectType == "Portfolio")
          {
            setSize = qh.PortfolioSetSize;
            if (numOfKeys < setSize)
            {
              FwkException("QueryTests.PopulateUserObject: Number of keys" +
                " should be multiple of 20");
            }
            numSet = numOfKeys / setSize;
            qh.PopulatePortfolioData(region, setSize, numSet, objSize);
          }
          else if (objectType == "Position")
          {
            setSize = qh.PositionSetSize;
            if (numOfKeys < setSize)
            {
              FwkException("QueryTests.PopulateUserObject: Number of keys" +
                " should be multiple of 20");
            }
            numSet = numOfKeys / setSize;
            qh.PopulatePositionData(region, setSize, numSet);
          }
          FwkInfo("QueryTests.PopulateUserObject: Done populating {0} objects",
            objectType);

          Thread.Sleep(1000); // Put a marker of inactivity in the stats
        } // valueSizes loop
      }
      catch (Exception ex)
      {
        FwkException("QueryTests.PopulateUserObject: Caught Exception: {0}", ex);
      }
      Thread.Sleep(3000); // Put a marker of inactivity in the stats
      FwkInfo("QueryTests.PopulateUserObject complete.");
    }

    public void DoGetObject()
    {
      FwkInfo("In QueryTests.GetObject.");
      try
      {
        Region region = GetRegion();
        ResetKey(DistinctKeys);
        int numOfKeys = GetUIntValue(DistinctKeys);

        string objectType = GetStringValue(ObjectType);
        QueryHelper qh = QueryHelper.GetHelper();
        int setSize = qh.PortfolioSetSize;
        if (numOfKeys < setSize)
        {
          setSize = numOfKeys;
        }
        int numSets = numOfKeys / setSize;

        IGFSerializable valuepos;
        CacheableKey keypos;
        for (int set = 1; set <= numSets; set++)
        {
          for (int current = 1; current <= setSize; current++)
          {
            string posname = null;
            if (objectType == "Portfolio")
            {
              posname = string.Format("port{0}-{1}", set, current);
            }
            else if (objectType == "Position")
            {
              posname = string.Format("pos{0}-{1}", set, current);
            }
            keypos = new CacheableString(posname);
            valuepos = region.Get(keypos);
            if (valuepos == null)
            {
              FwkException("QueryTests.GetObject: Could not find key [{0}]" +
                " in region.", keypos);
            }
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("QueryTests.GetObject: Caught Exception: {0}", ex);
      }
      FwkInfo("QueryTests.GetObject complete.");
    }

    public void DoRunQuery()
    {
      FwkInfo("In QueryTests.RunQuery");
      try
      {
        string queryType = GetStringValue(QueryResultType);
        string query = GetStringValue(QueryString); // set the query string in xml
        if (queryType == "resultSet")
        {
          FwkInfo("QueryTests.RunQuery: Calling VerifyResultSet");
          if (!VerifyResultSet())
          {
            FwkException("QueryTests.RunQuery: failed in VerifyResultSet");
          }
        }
        else if (queryType == "structSet")
        {
          FwkInfo("QueryTests.RunQuery: Calling VerifyStructSet");
          if (!VerifyStructSet())
          {
            FwkException("QueryTests.RunQuery: failed in VerifyStructSet");
          }
        }
        else if (query != null && query.Length > 0)
        {
          FwkInfo("QueryTests.RunQuery: Reading query from xml: {0}",
            query);
          if (!ReadQueryString(ref query))
          {
            FwkException("QueryTests.RunQuery: Failed in ReadQueryString");
          }
        }
        else
        {
          FwkException("QueryTests.RunQuery: Query type: {0} is not supported",
            queryType);
        }
      }
      catch (Exception ex)
      {
        FwkException("QueryTests.RunQuery: Caught Exception: {0}", ex);
      }
      FwkInfo("QueryTests.RunQuery complete.");
    }

    public void DoRunQueryWithPayloadAndEntries()
    {
      try
      {
        QueryHelper qh = QueryHelper.GetHelper();
        int numSet = 0;
        int setSize = 0;
        //populating data
        ResetKey(DistinctKeys);
        int numOfKeys; // number of key should be multiple of 20
        while ((numOfKeys = GetUIntValue(DistinctKeys)) > 0)
        { // distinctKeys loop
          ResetKey(ValueSizes);
          int objSize;
          while ((objSize = GetUIntValue(ValueSizes)) > 0)
          { // valueSizes loop
            FwkInfo("DoRunQueryWithPayloadAndEntries() Populating {0} " +
              "entries with {1} payload size", numOfKeys, objSize);
            ResetKey(RegionName);
            Region region;
            while ((region = GetRegion()) != null)
            {
              string regionName = region.Name;
              if ((regionName == "Portfolios") ||
                (regionName == "Portfolios2") ||
                (regionName == "Portfolios3"))
              {
                FwkInfo("DoRunQueryWithPayloadAndEntries() " +
                  "Populating Portfolio object to region " + region.FullPath);
                setSize = qh.PortfolioSetSize;
                if (numOfKeys < setSize)
                {
                  FwkException("doRunQueryWithPayloadAndEntries : " +
                    "Number of keys should be multiple of 20");
                }
                numSet = numOfKeys / setSize;
                qh.PopulatePortfolioData(region, setSize, numSet, objSize);
              }
              else if ((regionName == "Positions") ||
                (regionName == "/Portfolios/Positions"))
              {
                FwkInfo("DoRunQueryWithPayloadAndEntries() " +
                  "Populating Position object to region " + region.FullPath);
                setSize = qh.PositionSetSize;
                if (numOfKeys < setSize)
                {
                  FwkException("DoRunQueryWithPayloadAndEntries : " +
                    "Number of keys should be multiple of 20");
                }
                numSet = numOfKeys / setSize;
                qh.PopulatePositionData(region, setSize, numSet);
              }
            }
            FwkInfo("Populated User objects");
            Thread.Sleep(10000);
            // running queries
            FwkInfo("DoRunQueryWithPayloadAndEntries: " +
              "Calling VerifyResultSet ");
            if (!VerifyResultSet(numOfKeys))
            {
              FwkException("DoRunQueryWithPayloadAndEntries: " +
                "Failed in VerifyResultSet");
            }
            FwkInfo("DoRunQueryWithPayloadAndEntries: " +
              "Calling VerifyStructSet");
            if (!VerifyStructSet(numOfKeys))
            {
              FwkException("DoRunQueryWithPayloadAndEntries: " +
                "Failed in VerifyStructSet");
            }
            Thread.Sleep(3000); // Put a marker of inactivity in the stats
          } // valueSizes loop
          Thread.Sleep(2000);
        }// distinctKeys loop
      }
      catch (Exception ex)
      {
        FwkException("DoRunQueryWithPayloadAndEntries() Caught Exception: {0}", ex);
      }
      Thread.Sleep(4000); // Put a marker of inactivity in the stats
      FwkInfo("DoRunQueryWithPayloadAndEntries() complete.");
    }

    public void DoCloseCache()
    {
      FwkInfo("In QueryTests.CloseCache");

      CacheHelper.Close();

      FwkInfo("QueryTests.CloseCache complete.");
    }

    public void DoPopulateRangePositions()
    {
      FwkInfo("In QueryTests.DoPopulateRangePositions");

      try
      {
        Region region = GetRegion();

        int rangeStart = GetUIntValue("range-start");
        int rangeEnd = GetUIntValue("range-end");

        QueryHelper qh = QueryHelper.GetHelper();

        qh.PopulateRangePositionData(region, rangeStart, rangeEnd);
      }
      catch (Exception ex)
      {
        FwkException("DoPopulateRangePositions() Caught Exception: {0}", ex);
      }

      FwkInfo("DoPopulateRangePositions() complete.");
    }

    public void DoGetAndComparePositionObjects()
    {
      FwkInfo("In QueryTests.DoGetAndComparePositionObjects");

      try
      {
        Region region = GetRegion();

        int rangeStart = GetUIntValue("range-start");
        int rangeEnd = GetUIntValue("range-end");

        QueryHelper qh = QueryHelper.GetHelper();

        for (int i = rangeStart; i <= rangeEnd; i++)
        {
          IGFSerializable cachedPos = qh.GetCachedPositionObject(region, i);
          IGFSerializable generatedPos = qh.GetExactPositionObject(i);

          if (!qh.CompareTwoPositionObjects(cachedPos, generatedPos))
          {
            FwkSevere("QueryTest:DoGetAndComparePositionObjects: objects not same for index " + i);
          }
        }

        qh.PopulateRangePositionData(region, rangeStart, rangeEnd);
      }
      catch (Exception ex)
      {
        FwkException("DoGetAndComparePositionObjects() Caught Exception: {0}", ex);
      }

      FwkInfo("DoGetAndComparePositionObjects() complete.");
    }

    public void DoUpdateRangePositions()
    {
      FwkInfo("In QueryTests.DoUpdateRangePositions");

      try
      {
        int maxRange = GetUIntValue("range-max");
        int secondsToRun = GetTimeValue("workTime");

        DateTime nowTime = DateTime.Now;
        DateTime endTime = DateTime.Now;
        endTime = endTime.AddSeconds(secondsToRun);

        Region region = GetRegion();
        QueryHelper qh = QueryHelper.GetHelper();

        while (nowTime < endTime)
        {
          qh.PutExactPositionObject(region, Util.Rand(maxRange));
          nowTime = DateTime.Now;
        }        
      }
      catch (Exception ex)
      {
        FwkException("DoUpdateRangePositions() Caught Exception: {0}", ex);
      }
      FwkInfo("DoUpdateRangePositions() complete.");
    }

    public void DoVerifyAllPositionObjects()
    {
      FwkInfo("In QueryTests.DoVerifyAllPositionObjects");

      try
      {
        int maxRange = GetUIntValue("range-max");
        Region region = GetRegion();
        QueryHelper qh = QueryHelper.GetHelper();

        for (int i = 1; i <= maxRange; i++)
        {
          IGFSerializable pos1 = qh.GetCachedPositionObject(region, i);
          IGFSerializable pos2 = qh.GetExactPositionObject(i);
          if (!qh.CompareTwoPositionObjects(pos1, pos2))
          {
            FwkSevere("QueryTests.VerifyAllPositionObjects: objects not same for index " + i);
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("DoVerifyAllPositionObjects() Caught Exception: {0}", ex);
      }
      FwkInfo("DoVerifyAllPositionObjects() complete.");
    }

    #endregion

  }
}
