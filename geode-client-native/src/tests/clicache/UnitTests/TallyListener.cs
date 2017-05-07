//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using GemStone.GemFire.DUnitFramework;

  class TallyListener : CacheListenerAdapter
  {
    #region Private members

    private int m_creates = 0;
    private int m_updates = 0;
    private int m_invalidates = 0;
    private int m_destroys = 0;
    private int m_clears = 0;
    private ICacheableKey m_lastKey = null;
    private IGFSerializable m_lastValue = null;
    private IGFSerializable m_callbackArg = null;
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

    public IGFSerializable LastKey
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

    public IGFSerializable LastValue
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

     public void SetCallBackArg(IGFSerializable callbackArg)
    {
      m_callbackArg = callbackArg;
    }


    #endregion

    public void CheckcallbackArg(EntryEvent ev)
    {
      if (!isListenerInvoke)
        isListenerInvoke = true;
      if (m_callbackArg != null)
      {
        IGFSerializable callbkArg = (IGFSerializable)ev.CallbackArgument;
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

    public override void AfterCreate(EntryEvent ev)
    {
      m_creates++;
      m_lastKey = ev.Key;
      m_lastValue = ev.NewValue;
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

    public override void AfterUpdate(EntryEvent ev)
    {
      m_updates++;
      m_lastKey = ev.Key;
      m_lastValue = ev.NewValue;
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
    public override void AfterDestroy(EntryEvent ev)
    {
      m_destroys++;
      CheckcallbackArg(ev);
    }
    public override void AfterInvalidate(EntryEvent ev)
    {
      m_invalidates++;
      CheckcallbackArg(ev);
    }

    public override void AfterRegionDestroy(RegionEvent ev) { }

    public override void AfterRegionClear(RegionEvent ev) 
    { 
        m_clears++;
    }

    public override void AfterRegionInvalidate(RegionEvent ev) { }

    public override void AfterRegionLive(RegionEvent ev) { }

    public override void Close(Region region) { }

    #endregion
  }
}
