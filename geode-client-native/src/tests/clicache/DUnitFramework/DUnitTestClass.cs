//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;

namespace GemStone.GemFire.DUnitFramework
{
  using NUnit.Framework;

  /// <summary>
  /// Setup the test parameters including logfile, timebomb and timeout settings
  /// </summary>
  public abstract class DUnitTestClass
  {
    public static string CurrentTestClassName
    {
      get
      {
        return m_currentTestClass.FullName;
      }
    }

    public static string CurrentTestName
    {
      get
      {
        return m_currentTest.Name;
      }
    }

    protected virtual string TimeoutsXML
    {
      get
      {
        return "Timeouts.xml";
      }
    }
    protected virtual string TimeoutAttribName
    {
      get
      {
        return "timeout";
      }
    }

    protected int m_currentTestNumber = 0;
    protected Util.LogLevel m_logLevel = Util.DefaultLogLevel;
    protected ClientBase[] m_clients;

    private static Type m_currentTestClass;
    private static MethodInfo m_currentTest;
    private int m_defaultTimeoutMillis = -1;
    private XmlNodeReaderWriter m_timings;
    private static TimeBomb m_timeBomb = new TimeBomb();
    private MethodInfo m_fixtureSetup;
    private List<MethodInfo> m_tests = new List<MethodInfo>();

    protected virtual void SetLogging(string logFile)
    {
      Util.LogFile = logFile;
      Util.CurrentLogLevel = m_logLevel;
    }

    protected virtual void SetClientLogging(ClientBase[] clients, string logFile)
    {
      if (clients != null)
      {
        if (m_logLevel != Util.DefaultLogLevel)
        {
          foreach (ClientBase client in clients)
          {
            client.SetLogLevel(m_logLevel);
          }
        }
        if (logFile != null)
        {
          foreach (ClientBase client in clients)
          {
            client.SetLogFile(logFile);
          }
        }
      }
    }

    void SetClientLogging()
    {
      SetClientLogging(m_clients, Util.LogFile);
    }

    [TestFixtureSetUp]
    public virtual void InitTests()
    {
      m_currentTestClass = this.GetType();

      if (m_timings == null)
      {
        object[] attrs;
        // Get the list of tests for this class and fixture setup method.
        foreach (MethodInfo method in m_currentTestClass.GetMethods())
        {
          if (m_fixtureSetup == null)
          {
            attrs = method.GetCustomAttributes(
              typeof(TestFixtureSetUpAttribute), true);
            if (attrs != null && attrs.Length > 0)
            {
              m_fixtureSetup = method;
            }
          }
          attrs = method.GetCustomAttributes(
            typeof(TestAttribute), true);
          if (attrs != null && attrs.Length > 0)
          {
            m_tests.Add(method);
          }
        }
        m_timeBomb.SetFixtureSetup(m_fixtureSetup, this);
        // Get the default timeout
        string timingsFile = TimeoutsXML;
        m_timings = XmlNodeReaderWriter.GetInstance(timingsFile);
        try
        {
          string timeoutStr = m_timings.GetAttribute(
            '/' + XmlNodeReaderWriter.RootNodeName, TimeoutAttribName);
          if (timeoutStr != null)
          {
            m_defaultTimeoutMillis = int.Parse(timeoutStr) * 1000;
          }
        }
        catch (Exception ex)
        {
          Util.Log("Got an exception while setting default timeout from {0}: {1}",
            timingsFile, ex);
          m_defaultTimeoutMillis = -1;
        }
        NUnitMethodComparer mComp = new NUnitMethodComparer();
        m_tests.Sort(mComp);
      }
      SetLogging(GetClassLogFile());
      m_clients = GetClients();
      m_timeBomb.AddClients(m_clients);
      SetClientLogging();
    }

    [TestFixtureTearDown]
    public virtual void EndTests()
    {
      if (m_clients != null)
      {
        foreach (ClientBase client in m_clients)
        {
          client.Dispose();
        }
      }
      m_timeBomb.Diffuse();
    }

    [SetUp]
    public virtual void InitTest()
    {
      if (m_currentTestNumber < m_tests.Count)
      {
        m_currentTest = m_tests[m_currentTestNumber];
        m_currentTestNumber++;

        SetLogging(GetLogFile());
        SetClientLogging();
        int timeout = m_defaultTimeoutMillis;
        if (m_defaultTimeoutMillis > 0)
        {
          string timingsFile = TimeoutsXML;
          try
          {
            // Find the timeout set for this test in the Settings file.
            string timeoutStr = m_timings.GetAttribute(
              XmlNodeReaderWriter.GetPathForNode(m_currentTest),
              TimeoutAttribName);
            if (timeoutStr != null)
            {
              timeout = int.Parse(timeoutStr) * 1000;
            }
          }
          catch (Exception ex)
          {
            Util.Log("Got an exception while setting timeout from {0}: {1}",
              timingsFile, ex);
            timeout = m_defaultTimeoutMillis;
          }
        }
        else
        {
          timeout = Timeout.Infinite;
        }
        DateTime now = DateTime.Now;
        Console.WriteLine("[{0}:{1}:{2}.{3}] Set the timeout to {4} secs.{5}",
          now.Hour.ToString("D02"), now.Minute.ToString("D02"),
          now.Second.ToString("D02"), now.Millisecond.ToString("D03"),
          timeout / 1000, Environment.NewLine);
        Util.Log("INIT:: Starting next test in {0} with timeout as {1} secs.",
          this.GetType().FullName, timeout / 1000);
        m_timeBomb.TaskName = GetTaskName();
        m_timeBomb.Start(timeout);
      }
    }

    [TearDown]
    public virtual void EndTest()
    {
      m_timeBomb.WaitTimeout();
      m_timeBomb.Diffuse();
    }

    protected abstract ClientBase[] GetClients();

    protected virtual void AddClients(params ClientBase[] clients)
    {
      if (clients != null)
      {
        SetClientLogging(clients, GetLogFile());
        m_timeBomb.AddClients(clients);
      }
    }

    protected virtual string GetClassLogFile()
    {
      if (Util.DUnitLogDir == null)
      {
        return null;
      }
      return Util.DUnitLogDir + Path.DirectorySeparatorChar +
        m_currentTestClass.Name + ".log";
    }

    protected virtual string GetLogFile()
    {
      if (Util.DUnitLogDir == null)
      {
        return null;
      }
      return Util.DUnitLogDir + Path.DirectorySeparatorChar +
        m_currentTestClass.Name + '.' + m_currentTest.Name + ".log";
    }

    protected virtual string GetTaskName()
    {
      return m_currentTest.Name;
    }

    public override sealed int GetHashCode()
    {
      return base.GetHashCode();
    }
  }

  public class NUnitMethodComparer : IComparer<MethodInfo>
  {
    #region IComparer<MethodInfo> Members

    public int Compare(MethodInfo first, MethodInfo second)
    {
      return first.Name.CompareTo(second.Name);
    }

    #endregion
  }
}
