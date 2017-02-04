/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

﻿using System;

namespace Apache.Geode.Client.UnitTests
{
  using Apache.Geode.Client;
  using Apache.Geode.Client.Tests;
  using Apache.Geode.DUnitFramework;
  using NUnit.Framework;
  using QueryCategory = Apache.Geode.Client.Tests.QueryCategory;
  using QueryStatics = Apache.Geode.Client.Tests.QueryStatics;
  using QueryStrings = Apache.Geode.Client.Tests.QueryStrings;

  [TestFixture]
  [Category("group1")]
  [Category("unicast_only")]
  [Category("generics")]
  internal class ThinClientAppDomainQueryTests : ThinClientRegionSteps
  {
    #region Private members

    private UnitProcess m_client1;
    private UnitProcess m_client2;

    private static string[] QueryRegionNames = { "Portfolios", "Positions", "Portfolios2",
      "Portfolios3" };

    private static string QERegionName = "Portfolios";
    private static string endpoint1;
    private static string endpoint2;

    #endregion Private members

    protected override ClientBase[] GetClients()
    {
      return new ClientBase[] { };
    }

    [TestFixtureSetUp]
    public override void InitTests()
    {
      Properties<string, string> config = new Properties<string, string>();
      config.Insert("appdomain-enabled", "true");
      CacheHelper.InitConfig(config);
    }


    [TearDown]
    public override void EndTest()
    {
      CacheHelper.StopJavaServers();
      base.EndTest();
    }

    #region Functions invoked by the tests

    public void InitClient()
    {
      Serializable.RegisterTypeGeneric(Portfolio.CreateDeserializable);
      Serializable.RegisterTypeGeneric(Position.CreateDeserializable);
      Serializable.RegisterPdxType(Apache.Geode.Client.Tests.PortfolioPdx.CreateDeserializable);
      Serializable.RegisterPdxType(Apache.Geode.Client.Tests.PositionPdx.CreateDeserializable);
    }

    public void CreateCache(string locators)
    {
      CacheHelper.CreateTCRegion_Pool<object, object>(QueryRegionNames[0], true, true,
      null, locators, "__TESTPOOL1_", true);
      CacheHelper.CreateTCRegion_Pool<object, object>(QueryRegionNames[1], true, true,
        null, locators, "__TESTPOOL1_", true);
      CacheHelper.CreateTCRegion_Pool<object, object>(QueryRegionNames[2], true, true,
        null, locators, "__TESTPOOL1_", true);
      CacheHelper.CreateTCRegion_Pool<object, object>(QueryRegionNames[3], true, true,
        null, locators, "__TESTPOOL1_", true);

      IRegion<object, object> region = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);
      Apache.Geode.Client.RegionAttributes<object, object> regattrs = region.Attributes;
      region.CreateSubRegion(QueryRegionNames[1], regattrs);
    }

    public void PopulateRegions()
    {
      IRegion<object, object> region0 = CacheHelper.GetRegion<object, object>(QueryRegionNames[0]);
      IRegion<object, object> subRegion0 = (IRegion<object, object>)region0.GetSubRegion(QueryRegionNames[1]);
      IRegion<object, object> region1 = CacheHelper.GetRegion<object, object>(QueryRegionNames[1]);
      IRegion<object, object> region2 = CacheHelper.GetRegion<object, object>(QueryRegionNames[2]);
      IRegion<object, object> region3 = CacheHelper.GetRegion<object, object>(QueryRegionNames[3]);

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();
      Util.Log("SetSize {0}, NumSets {1}.", qh.PortfolioSetSize,
        qh.PortfolioNumSets);

      qh.PopulatePortfolioPdxData(region0, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
      qh.PopulatePositionPdxData(subRegion0, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
      qh.PopulatePositionPdxData(region1, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
      qh.PopulatePortfolioPdxData(region2, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
      qh.PopulatePortfolioPdxData(region3, qh.PortfolioSetSize,
        qh.PortfolioNumSets);
    }

    public void VerifyQueries()
    {
      bool ErrorOccurred = false;

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      QueryService<object, object> qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.ResultSetQueries)
      {
        if (qrystr.Category == QueryCategory.Unsupported)
        {
          Util.Log("Skipping query index {0} because it is unsupported.", qryIdx);
          qryIdx++;
          continue;
        }

        if (qryIdx == 2 || qryIdx == 3 || qryIdx == 4)
        {
          Util.Log("Skipping query index {0} for Pdx because it is function type.", qryIdx);
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating query index {0}. Query string {1}", qryIdx, qrystr.Query);

        Query<object> query = qs.NewQuery(qrystr.Query);

        ISelectResults<object> results = query.Execute();

        int expectedRowCount = qh.IsExpectedRowsConstantRS(qryIdx) ?
          QueryStatics.ResultSetRowCounts[qryIdx] : QueryStatics.ResultSetRowCounts[qryIdx] * qh.PortfolioNumSets;

        if (!qh.VerifyRS(results, expectedRowCount))
        {
          ErrorOccurred = true;
          Util.Log("Query verify failed for query index {0}.", qryIdx);
          qryIdx++;
          continue;
        }

        ResultSet<object> rs = results as ResultSet<object>;

        foreach (object item in rs)
        {
          PortfolioPdx port = item as PortfolioPdx;
          if (port == null)
          {
            PositionPdx pos = item as PositionPdx;
            if (pos == null)
            {
              string cs = item.ToString();
              if (cs == null)
              {
                Util.Log("Query got other/unknown object.");
              }
              else
              {
                Util.Log("Query got string : {0}.", cs);
              }
            }
            else
            {
              Util.Log("Query got Position object with secId {0}, shares {1}.", pos.secId, pos.getSharesOutstanding);
            }
          }
          else
          {
            Util.Log("Query got Portfolio object with ID {0}, pkid {1}.", port.ID, port.Pkid);
          }
        }

        qryIdx++;
      }

      Assert.IsFalse(ErrorOccurred, "One or more query validation errors occurred.");
    }

    public void VerifyUnsupporteQueries()
    {
      bool ErrorOccurred = false;

      QueryHelper<object, object> qh = QueryHelper<object, object>.GetHelper();

      QueryService<object, object> qs = PoolManager/*<object, object>*/.Find("__TESTPOOL1_").GetQueryService<object, object>();

      int qryIdx = 0;

      foreach (QueryStrings qrystr in QueryStatics.ResultSetQueries)
      {
        if (qrystr.Category != QueryCategory.Unsupported)
        {
          qryIdx++;
          continue;
        }

        Util.Log("Evaluating unsupported query index {0}.", qryIdx);

        Query<object> query = qs.NewQuery(qrystr.Query);

        try
        {
          ISelectResults<object> results = query.Execute();

          Util.Log("Query exception did not occur for index {0}.", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
        catch (GeodeException)
        {
          // ok, exception expected, do nothing.
          qryIdx++;
        }
        catch (Exception)
        {
          Util.Log("Query unexpected exception occurred for index {0}.", qryIdx);
          ErrorOccurred = true;
          qryIdx++;
        }
      }

      Assert.IsFalse(ErrorOccurred, "Query expected exceptions did not occur.");
    }

    #endregion Functions invoked by the tests

    [Test]
    public void RemoteQueryRS()
    {
      Util.Log("DoRemoteQueryRS: AppDomain: " + AppDomain.CurrentDomain.Id);
      CacheHelper.SetupJavaServers(true, "remotequeryN.xml");
      CacheHelper.StartJavaLocator(1, "GFELOC");
      Util.Log("Locator started");
      CacheHelper.StartJavaServerWithLocators(1, "GFECS1", 1);
      Util.Log("Cacheserver 1 started.");

      CreateCache(CacheHelper.Locators);
      Util.Log("CreateCache complete.");

      PopulateRegions();
      Util.Log("PopulateRegions complete.");

      VerifyQueries();
      Util.Log("VerifyQueries complete.");

      VerifyUnsupporteQueries();
      Util.Log("VerifyUnsupporteQueries complete.");

      Close();

      CacheHelper.StopJavaServer(1);
      Util.Log("Cacheserver 1 stopped.");

      CacheHelper.StopJavaLocator(1);
      Util.Log("Locator stopped");
    }
  }
}