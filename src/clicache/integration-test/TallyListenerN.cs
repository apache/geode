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

namespace Apache.Geode.Client.UnitTests.NewAPI
{
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client.Generic;
  //using Region = Apache.Geode.Client.Generic.IRegion<Object, Object>;

  class TallyListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>
  {
    #region Private members

    private int m_creates = 0;
    private int m_updates = 0;
    private int m_invalidates = 0;
    private int m_destroys = 0;
    private int m_clears = 0;
    private TKey m_lastKey = default(TKey);
    private TVal m_lastValue = default(TVal);
    private object m_callbackArg = null;
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

    public TKey LastKey
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

    public TVal LastValue
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

     public void SetCallBackArg(object callbackArg)
    {
      m_callbackArg = callbackArg;
    }


    #endregion

     public void CheckcallbackArg<TKey1, TVal1>(EntryEvent<TKey1, TVal1> ev)
    {
      Util.Log("TallyListenerN: Checking callback arg for EntryEvent " +
          "key:{0} oldval: {1} newval:{2} cbArg:{3} for region:{4} and remoteOrigin:{5}",
          ev.Key, ev.NewValue, ev.OldValue, ev.CallbackArgument, ev.Region.Name, ev.RemoteOrigin);

      if (!isListenerInvoke)
        isListenerInvoke = true;
      /*
      if (m_callbackArg != null)
      {
        IGFSerializable callbkArg1 = ev.CallbackArgument as IGFSerializable;
        IGFSerializable callbkArg2 = m_callbackArg as IGFSerializable;
        if (callbkArg2 != null && callbkArg2.Equals(callbkArg1))
        {
          isCallbackCalled = true;
        }
        string callbkArg3 = ev.CallbackArgument as string;
        string callbkArg4 = m_callbackArg as string;
        if (callbkArg3 != null && callbkArg3.Equals(callbkArg4))
        {
          isCallbackCalled = true;
        }
      }
      */
      if (m_callbackArg != null && m_callbackArg.Equals(ev.CallbackArgument))
      {
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

    public static TallyListener<TKey, TVal> Create()
    {
      return new TallyListener<TKey, TVal>();
    }

    #endregion

    #region ICacheListener Members

    public override void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      m_creates++;
      m_lastKey = (TKey)ev.Key;
      m_lastValue = ev.NewValue;
      CheckcallbackArg(ev);

      string keyString = m_lastKey.ToString();
      if (m_lastValue != null)
        WriteLog("TallyListener create - key = \"{0}\", value = \"{1}\"",
          keyString, m_lastValue.ToString());
      else
        WriteLog("TallyListener create - key = \"{0}\", value = \"null\"",
          keyString);

      if ((!m_ignoreTimeout) && (keyString.IndexOf("timeout") >= 0))
      {
        WriteLog("TallyListener: Sleeping 10 seconds to force a timeout.");
        Thread.Sleep(10000); // this should give the client cause to timeout...
        WriteLog("TallyListener: done sleeping..");
      }
    }

    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      m_updates++;
      m_lastKey = (TKey)ev.Key;
      m_lastValue = ev.NewValue;
      CheckcallbackArg(ev);
     
      string keyString = m_lastKey.ToString();
      if (m_lastValue != null)
        WriteLog("TallyListener update - key = \"{0}\", value = \"{1}\"",
          keyString, m_lastValue.ToString());
      else
        WriteLog("TallyListener update - key = \"{0}\", value = \"null\"",
          keyString);

      if ((!m_ignoreTimeout) && (keyString.IndexOf("timeout") >= 0))
      {
        WriteLog("TallyListener: Sleeping 10 seconds to force a timeout.");
        Thread.Sleep(10000); // this should give the client cause to timeout...
        WriteLog("TallyListener: done sleeping..");
      }
    }
    public override void AfterDestroy(EntryEvent<TKey, TVal> ev)
    {
      WriteLog("TallyListener destroy - key = \"{0}\"",
          ((TKey)ev.Key).ToString());
      m_destroys++;
      CheckcallbackArg(ev);
    }
    public override void AfterInvalidate(EntryEvent<TKey, TVal> ev)
    {
      WriteLog("TallyListener invalidate - key = \"{0}\"",
          ((TKey)ev.Key).ToString()); 
      m_invalidates++;
      CheckcallbackArg(ev);
    }

    public override void AfterRegionDestroy(RegionEvent<TKey, TVal> ev) { }

    public override void AfterRegionClear(RegionEvent<TKey, TVal> ev) 
    { 
        m_clears++;
    }

    public override void AfterRegionInvalidate(RegionEvent<TKey, TVal> ev) { }

    public override void AfterRegionLive(RegionEvent<TKey, TVal> ev) { }

    public override void Close(IRegion<TKey, TVal> region) { }

    #endregion
  }
}
