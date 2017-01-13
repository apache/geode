//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Text;

using GemStone.GemFire.Cache.Tests.NewAPI; // for Portfolio and Position classes

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Tests.NewAPI;
  using GemStone.GemFire.Cache.Generic;
  using QueryCategory = GemStone.GemFire.Cache.Tests.QueryCategory;
  using QueryStrings = GemStone.GemFire.Cache.Tests.QueryStrings;
  using QueryStatics = GemStone.GemFire.Cache.Tests.QueryStatics;
  using System.Threading;
  using System.Xml.Serialization;
  using System.IO;
  using System.Reflection;

  public class MyCq1Listener<TKey, TResult> : ICqListener<TKey, TResult>
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

    public virtual void UpdateCount(CqEvent<TKey, TResult> ev)
    {
      m_eventCnt++;
      CqOperationType opType = ev.getQueryOperation();
      if (opType == CqOperationType.OP_TYPE_CREATE)
      {
          Util.Log("ML:INSERT invoked");
        m_createCnt++;
      }
      else if (opType == CqOperationType.OP_TYPE_UPDATE)
      {
          Util.Log("ML:UPDATE invoked");
        m_updateCnt++;
      }
      else if (opType == CqOperationType.OP_TYPE_DESTROY)
      {
          Util.Log("ML:DESTROY invoked");
        m_destroyCnt++;
      }
 
    }
    public virtual void OnError(CqEvent<TKey, TResult> ev)
    {
      UpdateCount(ev);
    }

    public virtual void OnEvent(CqEvent<TKey, TResult> ev)
    {
      UpdateCount(ev);
    }

    public virtual void Close()
    { 
    }

  }

  public class QueryTests<TKey, TVal> : FwkTest<TKey, TVal>
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
    private static bool m_istransaction = false;
    private CacheTransactionManager txManager = null;
   
    #endregion

    #region Private utility methods

    private string GetNextRegionName(ref IRegion<TKey, TVal> region)
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
            region = (IRegion<TKey, TVal>)CacheHelper<TKey, TVal>.DCache.GetRegion<TKey, TVal>(path);
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

    protected IRegion<TKey, TVal> GetRegion()
    {
      return GetRegion(null);
    }

    protected IRegion<TKey, TVal> GetRegion(string regionName)
    {
      IRegion<TKey, TVal> region;
      if (regionName == null)
      {
        region = GetRootRegion();
      }
      else
      {
        region = CacheHelper<TKey, TVal>.GetRegion(regionName);
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
      QueryHelper<TKey,TVal> qh = QueryHelper<TKey,TVal>.GetHelper();
      int setSize = qh.PortfolioSetSize;
      FwkInfo("QueryTests.VerifyResultSet: numOfKeys [{0}], setSize [{1}].",
        numOfKeys, setSize);
      if (numOfKeys < setSize)
      {
        setSize = numOfKeys;
      }
      int numSet = numOfKeys / setSize;

      QueryService<TKey, object> qs = CheckQueryService(); 

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

                ISelectResults<object> results = ContinuousQuery(qs, QueryStatics.CqResultSetQueries[index].Query, index);

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
                //IGFSerializable[] paramList = new IGFSerializable[QueryStatics.NoOfQueryParam[index]];
                object[] paramList = new object[QueryStatics.NoOfQueryParam[index]];
                Int32 numVal = 0;
                for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParam[index]; ind++)
                {
                  try
                  {
                    numVal = Convert.ToInt32(QueryStatics.QueryParamSet[index][ind]);
                    //paramList[ind] = new CacheableInt32(numVal);
                    paramList[ind] = numVal;
                  }
                  catch (FormatException)
                  {
                    paramList[ind] = (System.String)QueryStatics.QueryParamSet[index][ind];
                  }
                }
                Query<object> qry = qs.NewQuery(query);
                DateTime startTime;
                DateTime endTime;
                TimeSpan elapsedTime;
                startTime = DateTime.Now;
                ISelectResults<object> results = qry.Execute(paramList, QueryResponseTimeout);
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

                ISelectResults<object> results = RemoteQuery(qs, QueryStatics.ResultSetQueries[index].Query);

                FwkInfo("QueryTests.VerifyResultSet: ResultSet size [{0}]," +
                  " numSet [{1}], setRowCount [{2}]", results.Size, numSet,
                  QueryStatics.ResultSetRowCounts[index]);
                if ((category - 1 != (int)QueryCategory.Unsupported) &&
                  (!qh.VerifyRS(results, qh.IsExpectedRowsConstantRS(index) ?
                  QueryStatics.ResultSetRowCounts[index] :
                  QueryStatics.ResultSetRowCounts[index] * numSet)))
                {
                  FwkSevere("QueryTests.VerifyResultSet: Query verify failed" +
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
      QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
      int setSize = qh.PortfolioSetSize;
      FwkInfo("QueryTests.VerifyStructSet: numOfKeys [{0}], setSize [{1}].",
        numOfKeys, setSize);
      if (numOfKeys < setSize)
      {
        setSize = numOfKeys;
      }
      int numSet = numOfKeys / setSize;

      QueryService<TKey, object> qs = CheckQueryService();

      DateTime startTime;
      DateTime endTime;
      TimeSpan elapsedTime;
      ResetKey(CategoryType);
      int category;
      string objectType = GetStringValue(ObjectType);
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
              Query<object> qry = qs.NewQuery(query);
              object[] paramList = new object[QueryStatics.NoOfQueryParamSS[index]];

              Int32 numVal = 0;
              for (Int32 ind = 0; ind < QueryStatics.NoOfQueryParamSS[index]; ind++)
              {
                try
                {
                  numVal = Convert.ToInt32(QueryStatics.QueryParamSetSS[index][ind]);
                  paramList[ind] = numVal;
               }
                catch (FormatException )
                {
                  paramList[ind] = (System.String)QueryStatics.QueryParamSetSS[index][ind];
                }
              }
              startTime = DateTime.Now;
              ISelectResults<object> results = qry.Execute(paramList, QueryResponseTimeout);
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
          }//}
        }
          else {
          if ((int)QueryStatics.StructSetQueries[index].Category == category-1 &&
            QueryStatics.StructSetQueries[index].Category != QueryCategory.Unsupported &&
            QueryStatics.StructSetQueries[index].IsLargeResultset == isLargeSetQuery)
          {
            int [] a = new int[] {4,6,7,9,12,14,15,16};
            if ((typeof(TVal).Equals(typeof(GemStone.GemFire.Cache.Tests.NewAPI.PortfolioPdx))||
              (typeof(TVal).Equals(typeof(GemStone.GemFire.Cache.Tests.NewAPI.PositionPdx)))) && ((IList<int>)a).Contains(index))
            {
              FwkInfo("Skiping Query for pdx object [{0}]", QueryStatics.StructSetQueries[index].Query);
            }
            else {
            string query = QueryStatics.StructSetQueries[index].Query;
            FwkInfo("QueryTests.VerifyStructSet: Query Category [{0}]," +
              " String [{1}], NumSet [{2}].", category-1, query, numSet);
            try
            {
              Query<object> qry = qs.NewQuery(query);
              startTime = DateTime.Now;
              ISelectResults<object> results = qry.Execute(QueryResponseTimeout);
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
        QueryService<TKey, object> qs = CheckQueryService();
        DateTime startTime;
        TimeSpan elapsedTime;
        ISelectResults<object> results;
        if (isCq)
        {
          string CqName = String.Format("_default{0}",query);
          CqQuery<TKey, object> cq = qs.GetCq(CqName);
          startTime = DateTime.Now;
          results = cq.ExecuteWithInitialResults(QueryResponseTimeout);
          elapsedTime = DateTime.Now - startTime;
          FwkInfo("ReadQueryString: Time Taken to execute the CqQuery [{0}]:" +
          "{1}ms ResultSize Size = {2}", query, elapsedTime.TotalMilliseconds ,results.Size);
        }
        else
        {
          startTime = DateTime.Now;
          Query<object> qry = qs.NewQuery(query);
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
          IRegion<TKey, TVal> region;
          if ((region = CacheHelper<TKey, TVal>.GetRegion(rootRegionName)) == null)
          {
            bool isDC = GetBoolValue("isDurable");
            RegionFactory rootAttrs = null;
            string m_isPool = null;
            //RegionAttributes rootAttrs = GetRegionAttributes(rootRegionData);
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
              CacheHelper<TKey, TVal>.InitConfigForPoolDurable(durableClientId, durableTimeout, conflateEvents, isSslEnable);
              rootAttrs = CacheHelper<TKey, TVal>.DCache.CreateRegionFactory(RegionShortcut.PROXY);
              SetRegionAttributes(rootAttrs, rootRegionData, ref m_isPool);
            }
            rootAttrs = CreatePool(rootAttrs, redundancyLevel);
            region = CacheHelper<TKey, TVal>.CreateRegion(rootRegionName, rootAttrs);
            GemStone.GemFire.Cache.Generic.RegionAttributes<TKey, TVal> regAttr = region.Attributes;
            FwkInfo("Region attributes for {0}: {1}", rootRegionName,
              CacheHelper<TKey, TVal>.RegionAttributesToString(regAttr));
            if (isDC)
            {
                CacheHelper<TKey, TVal>.DCache.ReadyForEvents();
            }
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
          FwkInfo("isObjectRegistered value is {0}",isObjectRegistered);
        if (!isObjectRegistered)
        {
            {
                Serializable.RegisterTypeGeneric(Portfolio.CreateDeserializable);
                FwkInfo("Completed Portfolio registeration");
            }
            {
                Serializable.RegisterTypeGeneric(Position.CreateDeserializable);
                Serializable.RegisterPdxType(PortfolioPdx.CreateDeserializable);
                Serializable.RegisterPdxType(PositionPdx.CreateDeserializable);
                FwkInfo("Completed other object registeration");
            }

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
          IRegion<TKey, TVal> region = CreateRootRegion(regionName);

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
          QueryService<TKey, object> qs = CheckQueryService();
          CqAttributesFactory<TKey, object> cqFac = new CqAttributesFactory<TKey, object>();
          ICqListener<TKey, object> cqLstner = new MyCq1Listener<TKey, object>();
          cqFac.AddCqListener(cqLstner);
          CqAttributes<TKey, object> cqAttr = cqFac.Create();
          string CqName = String.Format("cq-{0}", cqNum++);
          FwkInfo("Reading Query from xml {0} for cq {1}", qryStr, CqName);
          CqQuery<TKey, object> qry = qs.NewCq(CqName, qryStr, cqAttr, false);
          ISelectResults<object> results = qry.ExecuteWithInitialResults(QueryResponseTimeout);
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
        ResetKey("isDurableC");
        bool isdurable = GetBoolValue("isDurableC");
        while (qryStr != null)
        {
          QueryService<TKey, object> qs = CheckQueryService();
          CqAttributesFactory<TKey, object> cqFac = new CqAttributesFactory<TKey, object>();
          ICqListener<TKey, object> cqLstner = new MyCq1Listener<TKey, object>();
          cqFac.AddCqListener(cqLstner);
          CqAttributes<TKey, object> cqAttr = cqFac.Create();
          CqQuery<TKey, object> qry=null ;
          qry = qs.NewCq(qryStr, cqAttr, isdurable);
          FwkInfo("Registered Cq {0} with query {1} and isDurable {2} ", qry.Name, qryStr, isdurable);
          ResetKey(RegisterAndExecuteCq);
          ResetKey("executeWithIR");
          bool regAndExcCq = GetBoolValue(RegisterAndExecuteCq);
          bool isExecuteWithIR = GetBoolValue("executeWithIR");
          FwkInfo("Registered Cq with regAndExcCq {0} and with isExecuteWithIR {1} and with QueryState = {2} ", regAndExcCq, isExecuteWithIR, qry.GetState());
          if (regAndExcCq)
             qry.Execute();
          if (isExecuteWithIR)
              qry.ExecuteWithInitialResults(QueryResponseTimeout);
          qryStr = GetStringValue(QueryString);
          Util.BBIncrement("CqListenerBB", key);
        }
      }
      catch (Exception ex)
      {
        FwkException("Query.DoRegisterCqForConc() Caught Exception: {0}", ex);
      }
    }

    public void DoValidateCq()
    {
        FwkInfo("In QueryTest.DoValidateCq()");
        try
        {
          QueryService<TKey, object> qs = CheckQueryService();
          ResetKey("isDurableC");
          bool isdurable = GetBoolValue("isDurableC");
          ResetKey("NoCqs");
          int numCqs = GetUIntValue("NoCqs");
          
          CqServiceStatistics cqSvcStats = qs.GetCqStatistics();
          FwkInfo("Number of Cqs created is {0} number of Cqs active is {1} number of Cqs Closed is {2} number of Cqs onClient is {3} number of Cqs stopped is {4} ", cqSvcStats.numCqsCreated(), cqSvcStats.numCqsActive(), cqSvcStats.numCqsClosed(), cqSvcStats.numCqsOnClient(), cqSvcStats.numCqsStopped());
          System.Collections.Generic.List<string> durableCqList = qs.GetAllDurableCqsFromServer();

          int actualCqs = durableCqList.Count;
          UInt32 expectedCqs = cqSvcStats.numCqsCreated() - cqSvcStats.numCqsClosed();

          if (isdurable)
          {
              if (expectedCqs == actualCqs)
               {
                   FwkInfo("No of durableCqs on DC client is {0}  which is equal to the number of expected Cqs {1}", actualCqs, expectedCqs);
               }
             else
                  FwkException("No of durableCqs on DC client is {0} is not equal to the number of expected Cqs {1} ", actualCqs, expectedCqs);
          }
          else
          {
              if (durableCqList.Count == 0)
             {
                FwkInfo("No. of durable Cqs for NDC is {0}", durableCqList.Count);
             }
             else
               FwkException("Client is not durable, hence durableCqs for this client should have, had been 0,but it is{0}", durableCqList.Count);
          }
          FwkInfo("Cq durable list is {0}",durableCqList.Count);
        }
        catch (Exception ex)
        {
            FwkException("Query.DoValidateCq() Caught Exception : {0}", ex);
        }
    }

    public void DoVerifyCQListenerInvoked()
    {
      FwkInfo("In QueryTest.DoVerifyCQListenerInvoked()");
      try
      {
        QueryService<TKey, object> qs = CheckQueryService();
        ICqListener<TKey, object> cqLstner = new MyCq1Listener<TKey, object>();
        CqQuery<TKey, object>[] vcq = qs.GetCqs();
        System.Collections.Generic.List<string> durableCqList = qs.GetAllDurableCqsFromServer();
        FwkInfo("Durable Cq count is {0} and all cq count is {1}", durableCqList.Count, vcq.Length);
        //Validate Cq by name
                 
        for (int i = 0; i < vcq.Length; i++)
        {
          CqQuery<TKey, object> cq = (CqQuery<TKey, object>)vcq.GetValue(i);
          
          CqServiceStatistics cqSvcStats = qs.GetCqStatistics();
          FwkInfo("Number of Cqs created is {0} number of Cqs active is {1} number of Cqs Closed is {2} number of Cqs onClient is {3} number of Cqs stopped is {4} ", cqSvcStats.numCqsCreated(),cqSvcStats.numCqsActive(),cqSvcStats.numCqsClosed(),cqSvcStats.numCqsOnClient(),cqSvcStats.numCqsStopped());
         
          CqStatistics cqStats = cq.GetStatistics();
          CqAttributes<TKey, object> cqAttr = cq.GetCqAttributes();
          ICqListener<TKey, object>[] vl = cqAttr.getCqListeners();
          cqLstner = vl[0];
          MyCq1Listener<TKey, object> myLisner = (MyCq1Listener<TKey, object>)cqLstner;
          Util.BBSet("CQLISTNERBB", "CQ",myLisner.NumEvents());
          ResetKey("checkEvents");
          bool isCheckEvents = GetBoolValue("checkEvents");
          Util.Log("My count for cq {0} Listener : NumInserts:{1}, NumDestroys:{2}, NumUpdates:{3}, NumEvents:{4}", cq.Name, myLisner.NumInserts(), myLisner.NumDestroys(), myLisner.NumUpdates(), myLisner.NumEvents());
          if (isCheckEvents)
          {
              Int32 createCnt = (Int32)Util.BBGet("OpsBB", "CREATES");
              Int32 updateCnt = (Int32)Util.BBGet("OpsBB", "UPDATES");
              Int32 destCnt = (Int32)Util.BBGet("OpsBB", "DESTROYS");
              Int32 invalCnt = (Int32)Util.BBGet("OpsBB", "INVALIDATES");
              Int32 totalEvent = createCnt + updateCnt + destCnt + invalCnt;

              ResetKey("invalidate");
              bool isInvalidate = GetBoolValue("invalidate");
              ResetKey("stopped");
              bool isStoped = GetBoolValue("stopped");
              ResetKey("execute");
              bool isExecute = GetBoolValue("execute");

              FwkInfo("BB cnt are create = {0} update = {1} destroy = {2} invalidate = {3} TotalEvent = {4} ",createCnt , updateCnt ,destCnt, invalCnt ,totalEvent);
              if ((!cq.IsStopped()) && isExecute)
              {
                  if (isInvalidate)
                  {
                      if (myLisner.NumEvents() == totalEvent)
                          FwkInfo("Events count match ");
                      else
                          FwkException(" Total event count incorrect");
                  }
                  else
                  {
                      if (myLisner.NumInserts() == createCnt && myLisner.NumUpdates() == updateCnt && myLisner.NumDestroys() == destCnt && myLisner.NumEvents() == totalEvent)
                          FwkInfo("Events count match ");
                      else
                          FwkException(" accumulative event count incorrect");
                  }
              }
              else if (isStoped)
              {
                  if (cqStats.numEvents() == 0)
                      FwkInfo("");
                  else
                      FwkException("Cq is stopped before entry operation,hence events should not have been received.");
              }

          }
          else
          {
            //if (myLisner.NumEvents() > 0)
            {
              if (myLisner.NumInserts() == cqStats.numInserts() && myLisner.NumUpdates() == cqStats.numUpdates() && myLisner.NumDestroys() == cqStats.numDeletes() && myLisner.NumEvents() == cqStats.numEvents())
                Util.Log("Accumulative event count is correct");
              else
                FwkException("Accumulative event count is incorrect");
            }
            //else
              //FwkException("The listener should have processed some events:");
          }
        }
      }
      catch (Exception ex)
      {
        FwkException("Query.DoVerifyCQListenerInvoked() Caught Exception : {0}", ex);
      }
    }

    public void DoValidateEvents()
    {
        FwkInfo("validateEvents() called");
        try
        {
          DoVerifyCQListenerInvoked();
          ResetKey("distinctKeys");
          ResetKey("NumNewKeys");
          Int32 numKeys = GetUIntValue("distinctKeys");
          Int32 numNewKeys = GetUIntValue("NumNewKeys");
          FwkInfo("NUMKEYS = {0}", numKeys);
          Int32 clntCnt = GetUIntValue("clientCount");
          Int32 totalEvents = 0;
          UInt32 listnEvent = 0;
          Int32 numDestroyed = (Int32)Util.BBGet("ImageBB", "Last_Destroy") - (Int32)Util.BBGet("ImageBB", "First_Destroy") + 1;
          Int32 numInvalide = (Int32)Util.BBGet("ImageBB", "Last_Invalidate") - (Int32)Util.BBGet("ImageBB", "First_Invalidate") + 1;
          Int32 updateCount = (Int32)Util.BBGet("ImageBB", "Last_UpdateExistingKey") - (Int32)Util.BBGet("ImageBB", "First_UpdateExistingKey") + 1;
          
            //As CqListener is not invoked when events like "get,localDEstroy and localInvalidate" happen,hence not adding those  events count for correct validation.
          totalEvents = numNewKeys + numDestroyed +  numInvalide+ updateCount;
          FwkInfo("TOTALEVENTS = {0}" , totalEvents);
          listnEvent = (UInt32)Util.BBGet("CQLISTNERBB", "CQ");
          if (listnEvent == totalEvents)
          {
            FwkInfo("ListenerEvents and RegionEvents are equal i.e ListenerEvents = {0} ,RegionEvents = {1}",listnEvent ,totalEvents);
          }
          else
            FwkException("Events mismatch Listner event = {0} and entry event = {1}", listnEvent, totalEvents);
        }
        catch (Exception e)
        { 
          FwkException("QueryTest::validateEvents() FAILED caught exception  {0}", e.Message);
        }
    }

    public void DoVerifyCqDestroyed()
    {
      FwkInfo("In QueryTest.DoVerifyCQDestroy()");
      try
      {
        QueryService<TKey, object> qs = CheckQueryService();
        CqQuery<TKey, object>[] vcq = qs.GetCqs();
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
        QueryService<TKey, object> qs = CheckQueryService();
        CqQuery<TKey, object>[] vcq = qs.GetCqs();
        FwkInfo("QueryTest.DoCQState - number of cqs is {0} ", vcq.Length); 
        for (int i = 0; i < vcq.Length; i++)
        {
          CqQuery<TKey, object> cq = (CqQuery<TKey, object>)vcq.GetValue(i);
          if (opcode == "stopped")
              cq.Stop();
          else if (opcode == "closed")
              cq.Close();
          else if (opcode == "execute")
              if (cq.IsStopped())
              {
                  cq.Execute();
              }
              else
                  FwkInfo("Cq has not been stopped,it is still running");
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
      IRegion<TKey, TVal> region = GetRegion();
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
      QueryService<TKey, object> qs = CheckQueryService();
      while (now < end)
      {
        try 
        {
          opCode=GetStringValue("cqOps");
          CqQuery<TKey, object>[] vcq = qs.GetCqs();
          CqQuery<TKey, object> cqs = (CqQuery<TKey, object>)vcq.GetValue(Util.Rand(vcq.Length));
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

    public void StopCQ(CqQuery<TKey, object> cq)
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

    public void CloseCQ(CqQuery<TKey, object> cq)
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

    public void ExecuteCQ(CqQuery<TKey, object> cq)
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

    public void ExecuteCQWithIR(CqQuery<TKey, object> cq)
    {
      try
      {
        if (cq.IsStopped())
        {
          ISelectResults<object> results = cq.ExecuteWithInitialResults();
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

    public void ReRegisterCQ(CqQuery<TKey, object> cq)
    {
      try
      {
        FwkInfo("re-registering Cq: {0}",cq.Name);
        string query = cq.QueryString;
        CqAttributesFactory<TKey, object> cqFac = new CqAttributesFactory<TKey, object>();
        ICqListener<TKey, object> cqLstner = new MyCq1Listener<TKey, object>();
        cqFac.AddCqListener(cqLstner);
        CqAttributes<TKey, object> cqAttr = cqFac.Create();
        QueryService<TKey, object> qs = CheckQueryService();
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
          IRegion<TKey, TVal>[] vregion = CacheHelper<TKey, TVal>.DCache.RootRegions<TKey, TVal>();
          try
          {
            for (Int32 i = 0; i < vregion.Length; i++)
            {
              IRegion<TKey, TVal> region = (IRegion<TKey, TVal>)vregion.GetValue(i);
              region.GetLocalView().DestroyRegion();
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
        CacheHelper<TKey, TVal>.Close();
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
      IRegion<TKey, TVal> region = GetRegion(name);
      bool isDurable = GetBoolValue("isDurableReg");
      if (region == null)
        FwkInfo("DoRestartClientAndRegInt() region is null");
      else
        FwkInfo("DoRestartClientAndRegInt()region not null");
      region.GetSubscriptionService().RegisterAllKeys(isDurable);
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
        IRegion<TKey, TVal> region = GetRegion();
        FwkInfo("QueryTest::registerAllKeys region name is {0}",region.Name);
        bool isDurable = GetBoolValue("isDurableReg");
        region.GetSubscriptionService().RegisterAllKeys(isDurable);
        if (isDurable)
          CacheHelper<TKey, TVal>.DCache.ReadyForEvents();
      }
      catch (Exception ex)
      {
        FwkException("QueryTest.DoRegisterAllKeys() caught exception {0}",ex);
      }
    }

    public ISelectResults<object> RemoteQuery(QueryService<TKey, object> qs, string queryStr)
    {
      DateTime startTime;
      DateTime endTime;
      TimeSpan elapsedTime;
      Query<object> qry = qs.NewQuery(queryStr);
      startTime = DateTime.Now;
      ISelectResults<object> results = qry.Execute(QueryResponseTimeout);
      endTime = DateTime.Now;
      elapsedTime = endTime - startTime;
      FwkInfo("QueryTest.RemoteQuery: Time Taken to execute" +
            " the query [{0}]: {1}ms", queryStr, elapsedTime.TotalMilliseconds);
      return results;
    }

    public ISelectResults<object> ContinuousQuery(QueryService<TKey, object> qs, string queryStr, int cqNum)
    {
      DateTime startTime ,endTime;
      TimeSpan elapsedTime;
      CqAttributesFactory<TKey, object> cqFac = new CqAttributesFactory<TKey, object>();
      ICqListener<TKey, object> cqLstner = new MyCq1Listener<TKey, object>();
      cqFac.AddCqListener(cqLstner);
      CqAttributes<TKey, object> cqAttr = cqFac.Create();
      startTime = DateTime.Now;
      string CqName = String.Format("cq-{0}", cqNum);
      CqQuery<TKey, object> cq = qs.NewCq(CqName, queryStr, cqAttr, false);
      ISelectResults<object> results = cq.ExecuteWithInitialResults(QueryResponseTimeout);
      endTime = DateTime.Now;
      elapsedTime = endTime - startTime;
      return results;
    }
    
    public void DoAddRootAndSubRegion()
    {
      FwkInfo("In QueryTest.DoAddRootAndSubRegion()");
      ResetKey("subRegion");
      bool createSubReg = GetBoolValue("subRegion");
      try
      {
        string isTypeRegistered = GetStringValue("TypeId");
        if (isTypeRegistered != "registered")
        {
          FwkInfo("Getting inside for registeration");
          Serializable.RegisterTypeGeneric(Position.CreateDeserializable);
          Serializable.RegisterTypeGeneric(Portfolio.CreateDeserializable);
          Serializable.RegisterPdxType(GemStone.GemFire.Cache.Tests.NewAPI.PortfolioPdx.CreateDeserializable);
          Serializable.RegisterPdxType(GemStone.GemFire.Cache.Tests.NewAPI.PositionPdx.CreateDeserializable);

        }
        IRegion<TKey, TVal> parentRegion = null;
        ResetKey(RegionPaths);
        string sRegionName;
        string rootRegionData = GetStringValue("regionSpec");
        string tagName = GetStringValue("TAG");
        string endpoints = Util.BBGet(JavaServerBB, EndPointTag + tagName)
          as string;
        while ((sRegionName = GetNextRegionName(ref parentRegion)) != null &&
          sRegionName.Length > 0)
        {
          IRegion<TKey, TVal> region;
          if (parentRegion == null)
          {
            region = CreateRootRegion(sRegionName, rootRegionData, endpoints);
          }
          else
          {
            string fullName = parentRegion.FullPath;
            GemStone.GemFire.Cache.Generic.RegionAttributes<TKey, TVal> regattrs = parentRegion.Attributes;
            parentRegion.CreateSubRegion(sRegionName, regattrs);
            Util.BBSet(QueryBB, sRegionName, fullName); parentRegion.SubRegions(false);
          }
          if (createSubReg)
          {
              CreateSubRegion(sRegionName);
          }
      
        }
        ResetKey("useTransactions");
        m_istransaction = GetBoolValue("useTransactions");

      }
      catch (Exception ex)
      {
        FwkException("QueryTest.DoAddRootAndSubRegion() Caught exception: {0}", ex);
      }
      
    }

    public void CreateSubRegion(string regName) 
    {
        IRegion<TKey, TVal> parentRegion = GetRegion();
        IRegion<TKey, TVal> subRegion;
        GemStone.GemFire.Cache.Generic.RegionAttributes<TKey, TVal> regattrs = parentRegion.Attributes;
        subRegion = parentRegion.CreateSubRegion(regName,regattrs);
        ICollection<IRegion<TKey, TVal>> subRegions = parentRegion.SubRegions(true);
        FwkInfo("subregions are {0}",subRegions.Count);
    }

    public void DoDestroyUserObject()
    {
      FwkInfo("In QueryTests.DoDestroyUserObject()");
      try
      {
        string name = GetStringValue("regionName");
        IRegion<TKey,TVal> region = GetRegion(name);
        //string label = CacheHelper<TKey, TVal>.RegionTag(region.Attributes);
        ResetKey(DistinctKeys);
        int numOfKeys = GetUIntValue(DistinctKeys);
        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
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
        if (m_istransaction)
        {
          txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
          txManager.Begin();
        }
        qh.DestroyPortfolioOrPositionData(region, setSize, numSet, objectType);
        if (m_istransaction)
          txManager.Commit();
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
        IRegion<TKey, TVal> region = GetRegion(name);
        //string label = CacheHelper<TKey, TVal>.RegionTag(region.Attributes);
        ResetKey(DistinctKeys);
        int numOfKeys = GetUIntValue(DistinctKeys);
        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
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
        if (m_istransaction)
        {
          txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
          txManager.Begin();
        }
        qh.InvalidatePortfolioOrPositionData(region, setSize, numSet, objectType);
        if (m_istransaction)
           txManager.Commit();
        
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
        IRegion<TKey, TVal> region = GetRegion(name);
        //string label = CacheHelper<TKey, TVal>.RegionTag(region.Attributes);
        ResetKey(DistinctKeys);
        int numOfKeys = GetUIntValue(DistinctKeys); // number of keys should be multiple of 20
        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
        int numSet = 0;
        int setSize = 0;
        // Loop over value sizes
        ResetKey(ValueSizes);
        int objSize;
        string objectType = GetStringValue(ObjectType);
        
        while ((objSize = GetUIntValue(ValueSizes)) > 0)
        { // value sizes loop
          if (m_istransaction)
          {
            txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
            txManager.Begin();
          }
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
          if (objectType == "PortfolioPdx")
          {
            setSize = qh.PortfolioSetSize;
            if (numOfKeys < setSize)
            {
              FwkException("QueryTests.PopulateUserObject: Number of keys" +
                " should be multiple of 20");
            }
            numSet = numOfKeys / setSize;
            qh.PopulatePortfolioPdxData(region, setSize, numSet, objSize);
          }
          else if (objectType == "PositionPdx")
          {
            setSize = qh.PositionSetSize;
            if (numOfKeys < setSize)
            {
              FwkException("QueryTests.PopulateUserObject: Number of keys" +
                " should be multiple of 20");
            }
            numSet = numOfKeys / setSize;
            qh.PopulatePositionPdxData(region, setSize, numSet);
          }
          if(m_istransaction)
            txManager.Commit();
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
        IRegion<TKey, TVal> region = GetRegion();
        ResetKey(DistinctKeys);
        int numOfKeys = GetUIntValue(DistinctKeys);

        string objectType = GetStringValue(ObjectType);
        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
        int setSize = qh.PortfolioSetSize;
        if (numOfKeys < setSize)
        {
          setSize = numOfKeys;
        }
        int numSets = numOfKeys / setSize;

        TVal valuepos;
        TKey keypos;
        for (int set = 1; set <= numSets; set++)
        {
          for (int current = 1; current <= setSize; current++)
          {
            string posname = null;
            if (objectType == "Portfolio" || (objectType == "PortfolioPdx"))
            {
              posname = string.Format("port{0}-{1}", set, current);
            }
            else if (objectType == "Position" || (objectType == "PositionPdx"))
            {
              posname = string.Format("pos{0}-{1}", set, current);
            }
            
            keypos = (TKey)(object)posname;
            if (m_istransaction)
            {
              txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
              txManager.Begin();
            }
            valuepos = region[keypos];
            if (m_istransaction)
              txManager.Commit();
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
        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
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
            IRegion<TKey, TVal> region;
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

      CacheHelper<TKey, TVal>.Close();

      FwkInfo("QueryTests.CloseCache complete.");
    }

    public void DoPopulateRangePositions()
    {
      FwkInfo("In QueryTests.DoPopulateRangePositions");
      try
      {
        IRegion<TKey, TVal> region = GetRegion();

        int rangeStart = GetUIntValue("range-start");
        int rangeEnd = GetUIntValue("range-end");

        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();
        if (m_istransaction)
        {
          txManager = CacheHelper<TKey, TVal>.DCache.CacheTransactionManager;
          txManager.Begin();
        }
        qh.PopulateRangePositionData(region, rangeStart, rangeEnd);
        if(m_istransaction)
          txManager.Commit();
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
        IRegion<TKey, TVal> region = GetRegion();

        int rangeStart = GetUIntValue("range-start");
        int rangeEnd = GetUIntValue("range-end");

        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();

        for (int i = rangeStart; i <= rangeEnd; i++)
        {
          TVal cachedPos = qh.GetCachedPositionObject(region, i);
          TVal generatedPos = qh.GetExactPositionObject(i);

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

        IRegion<TKey, TVal> region = GetRegion();
        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();

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
        IRegion<TKey, TVal> region = GetRegion();
        QueryHelper<TKey, TVal> qh = QueryHelper<TKey, TVal>.GetHelper();

        for (int i = 1; i <= maxRange; i++)
        {
          TVal pos1 = qh.GetCachedPositionObject(region, i);
          TVal pos2 = qh.GetExactPositionObject(i);
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
