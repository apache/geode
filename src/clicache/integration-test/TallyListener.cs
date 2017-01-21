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
using System.Threading;

namespace Apache.Geode.Client.UnitTests
{
  using Apache.Geode.DUnitFramework;

  class TallyListener : Apache.Geode.Client.CacheListenerAdapter<Object, Object>
  {
    #region Private members

    private int m_creates = 0;
    private int m_updates = 0;
    private int m_invalidates = 0;
    private int m_destroys = 0;
    private int m_clears = 0;
    private Apache.Geode.Client.ICacheableKey m_lastKey = null;
    private Apache.Geode.Client.IGFSerializable m_lastValue = null;
    private Apache.Geode.Client.IGFSerializable m_callbackArg = null;
    private bool m_ignoreTimeout = false;
    private bool m_quiet = false;
    private bool isListenerInvoke = false;
    private bool isCallbackCalled = false;

    #endregion

    #region Public accessors

    public int Creates
    {
      get
      {
        return m_creates;
      }
    }

    public int Clears
    {
      get
      {
        return m_clears;
      }
    }

    public int Updates
    {
      get
      {
        return m_updates;
      }
    }

    public int Invalidates
    {
      get
      {
        return m_invalidates;
      }
    }

    public int Destroys
    {
      get
      {
        return m_destroys;
      }
    }

    public Apache.Geode.Client.IGFSerializable LastKey
    {
      get
      {
        return m_lastKey;
      }
    }

    public bool IsListenerInvoked
    {
      get
      {
        return isListenerInvoke;
      }
    }

    public bool IsCallBackArgCalled
    {
      get
      {
        return isCallbackCalled;
      }
    }

    public Apache.Geode.Client.IGFSerializable LastValue
    {
      get
      {
        return m_lastValue;
      }
    }

    public bool IgnoreTimeout
    {
      set
      {
        m_ignoreTimeout = value;
      }
    }

    public bool Quiet
    {
      set
      {
        m_quiet = value;
      }
    }

     public void SetCallBackArg(Apache.Geode.Client.IGFSerializable callbackArg)
    {
      m_callbackArg = callbackArg;
    }


    #endregion

    public void CheckcallbackArg(Apache.Geode.Client.EntryEvent<Object, Object> ev)
    {
      if (!isListenerInvoke)
        isListenerInvoke = true;
      if (m_callbackArg != null)
      {
        Apache.Geode.Client.IGFSerializable callbkArg = (Apache.Geode.Client.IGFSerializable)ev.CallbackArgument;
        if (m_callbackArg.Equals(callbkArg))
          isCallbackCalled = true;
      }
    }

   
    public void ResetListenerInvokation()
    {
      isListenerInvoke = false;
      isCallbackCalled = false;
    }
    public int ExpectCreates(int expected)
    {
      int tries = 0;
      while ((m_creates < expected) && (tries < 200))
      {
        Thread.Sleep(100);
        tries++;
      }
      return m_creates;
    }

    public int ExpectUpdates(int expected)
    {
      int tries = 0;
      while ((m_updates < expected) && (tries < 200))
      {
        Thread.Sleep(100);
        tries++;
      }
      return m_updates;
    }

    public int ExpectedInvalidates(int expected)
    {
      int tries = 0;
      while ((m_invalidates < expected) && (tries < 200))
      {
        Thread.Sleep(100);
        tries++;
      }
      return m_invalidates;
    }

    public int ExpectedDestroys(int expected)
    {
      int tries = 0;
      while ((m_destroys < expected) && (tries < 200))
      {
        Thread.Sleep(100);
        tries++;
      }
      return m_destroys;
    }
         
    public void ShowTallies()
    {
      Util.Log("TallyListener state: (updates = {0}, creates = {1}, invalidates = {2}, destroys = {3})",
        Updates,Creates, Invalidates, Destroys);
    }

    #region Logging functions that check for m_quiet

    private void WriteLog(string message)
    {
      if (!m_quiet)
      {
        Util.Log(message);
      }
    }

    private void WriteLog(string format, params object[] args)
    {
      if (!m_quiet)
      {
        Util.Log(format, args);
      }
    }

    public static TallyListener Create()
    {
      return new TallyListener();
    }

    #endregion

    #region ICacheListener Members

    public override void AfterCreate(Apache.Geode.Client.EntryEvent<Object, Object> ev)
    {
      m_creates++;
      m_lastKey = (Apache.Geode.Client.ICacheableKey)ev.Key;
      m_lastValue = (Apache.Geode.Client.IGFSerializable)ev.NewValue;
      CheckcallbackArg(ev);

      string keyString = m_lastKey.ToString();
      WriteLog("TallyListener create - key = \"{0}\", value = \"{1}\"",
        keyString, m_lastValue.ToString());

      if ((!m_ignoreTimeout) && (keyString.IndexOf("timeout") >= 0))
      {
        WriteLog("TallyListener: Sleeping 10 seconds to force a timeout.");
        Thread.Sleep(10000); // this should give the client cause to timeout...
        WriteLog("TallyListener: done sleeping..");
      }
    }

    public override void AfterUpdate(Apache.Geode.Client.EntryEvent<Object, Object> ev)
    {
      m_updates++;
      m_lastKey = (Apache.Geode.Client.ICacheableKey)ev.Key;
      m_lastValue = (Apache.Geode.Client.IGFSerializable)ev.NewValue;
      CheckcallbackArg(ev);
     
      string keyString = m_lastKey.ToString();
      WriteLog("TallyListener update - key = \"{0}\", value = \"{1}\"",
        keyString, m_lastValue.ToString());

      if ((!m_ignoreTimeout) && (keyString.IndexOf("timeout") >= 0))
      {
        WriteLog("TallyListener: Sleeping 10 seconds to force a timeout.");
        Thread.Sleep(10000); // this should give the client cause to timeout...
        WriteLog("TallyListener: done sleeping..");
      }
    }
    public override void AfterDestroy(Apache.Geode.Client.EntryEvent<Object, Object> ev)
    {
      m_destroys++;
      CheckcallbackArg(ev);
    }
    public override void AfterInvalidate(Apache.Geode.Client.EntryEvent<Object, Object> ev)
    {
      m_invalidates++;
      CheckcallbackArg(ev);
    }

    public override void AfterRegionDestroy(Apache.Geode.Client.RegionEvent<Object, Object> ev) { }

    public override void AfterRegionClear(Apache.Geode.Client.RegionEvent<Object, Object> ev) 
    { 
        m_clears++;
    }

    public override void AfterRegionInvalidate(Apache.Geode.Client.RegionEvent<Object, Object> ev) { }

    public override void AfterRegionLive(Apache.Geode.Client.RegionEvent<Object, Object> ev) { }

    public override void Close(Apache.Geode.Client.IRegion<Object, Object> region) { }

    #endregion
  }
}
