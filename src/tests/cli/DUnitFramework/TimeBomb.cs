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
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;

namespace Apache.Geode.DUnitFramework
{
  using NUnit.Framework;

  public class TimeBomb : IDisposable
  {
    #region Private members

    private List<ClientBase> m_clients;
    private Timer m_timer;
    private int m_currentTimeout;
    private ManualResetEvent m_timeoutEvent;
    private string m_taskName;
    private MethodInfo m_fixtureSetupMethod;
    private object m_targetObj;
    private object m_syncRoot;
    private const int WaitMillis = 5000;

    #endregion

    #region Public accessors

    public string TaskName
    {
      get
      {
        return m_taskName;
      }
      set
      {
        m_taskName = value;
      }
    }

    public List<ClientBase> Clients
    {
      get
      {
        return m_clients;
      }
    }

    #endregion

    #region Public methods

    public TimeBomb()
    {
      m_timer = new Timer(new TimerCallback(TimeoutHandler), null,
        Timeout.Infinite, Timeout.Infinite);
      m_clients = new List<ClientBase>();
      m_timeoutEvent = new ManualResetEvent(true);
      m_syncRoot = new object();
    }

    public void SetFixtureSetup(UnitFnMethod fixtureSetup)
    {
      m_fixtureSetupMethod = fixtureSetup.Method;
      m_targetObj = fixtureSetup.Target;
    }

    public void SetFixtureSetup(MethodInfo fixtureSetupMethod, object target)
    {
      m_fixtureSetupMethod = fixtureSetupMethod;
      m_targetObj = target;
    }

    public void Start(int millis)
    {
      if (m_timer != null)
      {
        m_currentTimeout = millis;
        m_timer.Change(millis, Timeout.Infinite);
      }
    }

    public void Diffuse()
    {
      if (m_timer != null)
      {
        m_timer.Change(Timeout.Infinite, Timeout.Infinite);
      }
    }

    public void AddClients(ClientBase[] clients)
    {
      if (clients != null)
      {
        lock (m_syncRoot)
        {
          if (m_clients != null)
          {
            foreach (ClientBase client in clients)
            {
              m_clients.Add(client);
            }
          }
        }
      }
    }

    public void AddClients(List<ClientBase> clients)
    {
      if (clients != null)
      {
        lock (m_syncRoot)
        {
          if (m_clients != null)
          {
            m_clients.AddRange(clients);
          }
        }
      }
    }

    public void WaitTimeout()
    {
      m_timeoutEvent.WaitOne();
    }

    #endregion

    private void TimeoutHandler(object state)
    {
      Diffuse();
      m_timeoutEvent.Reset();
      lock (m_syncRoot)
      {
        if (m_clients != null)
        {
          Util.Log(Util.LogLevel.Error,
            "Timeout occurred for task[{0}] after waiting for {1}ms",
            m_taskName, m_currentTimeout);
          foreach (ClientBase client in m_clients)
          {
            try
            {
              Util.Log(Util.LogLevel.Info, "Dumping stack for client [{0}] ", client.ID);
              client.DumpStackTrace();
            }
            catch (Exception ex)
            {
              Util.Log(Util.LogLevel.Error, string.Format("Error in dumping " +
                "stack for client[{0}]: {1}", client.ID, ex));
            }
          }
          Thread.Sleep(WaitMillis);
          foreach (ClientBase client in m_clients)
          {
            try
            {
              client.ForceKill(0);
            }
            catch (Exception ex)
            {
              Util.Log(Util.LogLevel.Error, string.Format("Error in killing " +
                "client[{0}]: {1}", client.ID, ex));
            }
          }
          m_clients.Clear();
        }
      }
      if (m_fixtureSetupMethod != null)
      {
        try
        {
          m_fixtureSetupMethod.Invoke(m_targetObj, null);
        }
        catch (Exception ex)
        {
          Util.Log(Util.LogLevel.Error, "FATAL: Error in invoking " +
            "FixtureSetup method {0}: {1}", m_fixtureSetupMethod.Name, ex);
        }
      }
      m_timeoutEvent.Set();
    }

    protected void Dispose(bool disposing)
    {
      Diffuse();
      if (m_timer != null)
      {
        if (disposing)
        {
          m_timer.Dispose();
          m_timer = null;
        }
      }
      lock (m_syncRoot)
      {
        if (m_clients != null)
        {
          m_clients.Clear();
          m_clients = null;
        }
      }
    }

    #region IDisposable Members

    public void Dispose()
    {
      Dispose(true);
      GC.SuppressFinalize(this);
    }

    #endregion

    ~TimeBomb()
    {
      Dispose(false);
    }
  }
}
