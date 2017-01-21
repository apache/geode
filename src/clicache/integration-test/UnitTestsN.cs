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

using System;

namespace Apache.Geode.Client.UnitTests
{
  using NUnit.Framework;
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client;
  using System.IO;

  //using Region = Apache.Geode.Client.IRegion<Object, Object>;

  /// <summary>
  /// Setup the test parameters including logfile, timebomb and timeout settings.
  /// Also close the cache for each client in teardown.
  /// </summary>
  public abstract class UnitTests : DUnitTestClass
  {
    protected virtual string ExtraPropertiesFile
    {
      get
      {
        return null;
      }
    }

    protected DateTime m_startTime;
    protected DateTime m_endTime;

    protected override void SetLogging(string logFile)
    {
      base.SetLogging(logFile);
      CacheHelper.SetLogging();
    }

    protected override void SetClientLogging(ClientBase[] clients, string logFile)
    {
      base.SetClientLogging(clients, logFile);
      if (clients != null)
      {
        foreach (ClientBase client in clients)
        {
          client.Call(CacheHelper.SetLogging);
        }
      }
    }

    [TestFixtureSetUp]
    public override void InitTests()
    {
      base.InitTests();
      string extraPropsFile = ExtraPropertiesFile;
      if (extraPropsFile != null)
      {
        CacheHelper.SetExtraPropertiesFile(extraPropsFile);
        if (m_clients != null)
        {
          foreach (ClientBase client in m_clients)
          {
            client.Call(CacheHelper.SetExtraPropertiesFile, extraPropsFile);
          }
        }
      }
    }

    [TestFixtureTearDown]
    public override void EndTests()
    {
      string coverageXMLs = string.Empty;
      string startDir = null;
      bool hasCoverage = "true".Equals(Environment.GetEnvironmentVariable(
        "COVERAGE_ENABLED"));
      try
      {
        CacheHelper.SetExtraPropertiesFile(null);
        if (m_clients != null)
        {
          foreach (ClientBase client in m_clients)
          {
            try
            {
              client.Call(CacheHelper.Close);
            }
            catch (System.Runtime.Remoting.RemotingException)
            {
            }
            catch (System.Net.Sockets.SocketException)
            {
            }
            if (hasCoverage)
            {
              coverageXMLs = coverageXMLs + " coverage-" + client.ID + ".xml";
              startDir = client.StartDir;
            }
          }
        }
        CacheHelper.Close();
      }
      finally
      {
        base.EndTests();
      }
      // merge ncover output
      if (coverageXMLs.Length > 0)
      {
        string mergedCoverage = "merged-coverage.xml";
        string mergedCoverageTmp = "merged-coverage-tmp.xml";
        System.Diagnostics.Process mergeProc;
        if (File.Exists(mergedCoverage))
        {
          coverageXMLs = coverageXMLs + " " + mergedCoverage;
        }
        //Console.WriteLine("Current directory: " + Environment.CurrentDirectory + "; merging: " + coverageXMLs);
        if (!Util.StartProcess("ncover.reporting.exe", coverageXMLs + " //s "
          + mergedCoverageTmp, Util.LogFile == null, startDir,
          true, true, true, out mergeProc))
        {
          Assert.Fail("FATAL: Could not start ncover.reporting");
        }
        if (!mergeProc.WaitForExit(UnitProcess.MaxEndWaitMillis)
          && !mergeProc.HasExited)
        {
          mergeProc.Kill();
        }
        File.Delete(mergedCoverage);
        File.Move(mergedCoverageTmp, mergedCoverage);
        if (m_clients != null)
        {
          foreach (ClientBase client in m_clients)
          {
            File.Delete("coverage-" + client.ID + ".xml");
          }
        }
      }
    }

    [TearDown]
    public override void EndTest()
    {
      CacheHelper.EndTest();
      base.EndTest();
    }

    public void StartTimer()
    {
      m_startTime = DateTime.Now;
    }

    public TimeSpan StopTimer()
    {
      m_endTime = DateTime.Now;
      return (m_endTime - m_startTime);
    }

    public void LogTaskTiming(ClientBase client, string taskName, int numOps)
    {
      StopTimer();
      TimeSpan elapsed = m_endTime - m_startTime;
      Util.Log("{0}Time taken for task [{1}]: {2}ms {3}ops/sec{4}",
        Util.MarkerString, taskName, elapsed.TotalMilliseconds,
        (numOps * 1000) / elapsed.TotalMilliseconds, Util.MarkerString);
    }
  }
}
