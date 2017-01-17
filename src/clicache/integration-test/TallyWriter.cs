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

namespace GemStone.GemFire.Cache.UnitTests
{
  using GemStone.GemFire.DUnitFramework;

  class TallyWriter : GemStone.GemFire.Cache.Generic.CacheWriterAdapter<Object, Object>
  {
    #region Private members

    private int m_creates = 0;
    private int m_updates = 0;
    private int m_invalidates = 0;
    private int m_destroys = 0;
    private GemStone.GemFire.Cache.Generic.IGFSerializable m_callbackArg = null;
    private int m_clears = 0;
    private GemStone.GemFire.Cache.Generic.IGFSerializable m_lastKey = null;
    private GemStone.GemFire.Cache.Generic.IGFSerializable m_lastValue = null;
    private bool isWriterFailed = false;
    private bool isWriterInvoke = false;
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


    public GemStone.GemFire.Cache.Generic.IGFSerializable LastKey
    {
      get
      {
        return m_lastKey;
      }
    }

    public GemStone.GemFire.Cache.Generic.IGFSerializable CallbackArgument
    {
      get
      {
        return m_callbackArg;
      }
    }


    public GemStone.GemFire.Cache.Generic.IGFSerializable LastValue
    {
      get
      {
        return m_lastValue;
      }
    }

   public void SetWriterFailed( )
   {
    isWriterFailed = true;
   }

  public void SetCallBackArg( GemStone.GemFire.Cache.Generic.IGFSerializable callbackArg )
  {
    m_callbackArg = callbackArg;
  }

  public void ResetWriterInvokation()
  {
    isWriterInvoke = false;
    isCallbackCalled = false;
  }

  public  bool IsWriterInvoked
  {
    get
    {
      return isWriterInvoke;
    }
  }
  public bool IsCallBackArgCalled
  {
    get
    {
      return isCallbackCalled;
    }
  }
    #endregion

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
    
    public void ShowTallies()
    {
      Util.Log("TallyWriter state: (updates = {0}, creates = {1}, invalidates = {2}, destroys = {3})",
        Updates, Creates, Invalidates, Destroys);
    }

    public void CheckcallbackArg(GemStone.GemFire.Cache.Generic.EntryEvent<Object, Object> ev)
      {

        if(!isWriterInvoke)
          isWriterInvoke = true;
        if (m_callbackArg != null)
        {
          GemStone.GemFire.Cache.Generic.IGFSerializable callbkArg = (GemStone.GemFire.Cache.Generic.IGFSerializable)ev.CallbackArgument;
          if (m_callbackArg.Equals(callbkArg))
            isCallbackCalled = true;
        }  
      }

    public static TallyWriter Create()
    {
      return new TallyWriter();
    }

    #region ICacheWriter Members

    public override bool BeforeCreate(GemStone.GemFire.Cache.Generic.EntryEvent<Object, Object> ev)
    {
      m_creates++;
      Util.Log("TallyWriter::BeforeCreate");
      CheckcallbackArg(ev);
      return !isWriterFailed;
    }

    public override bool BeforeDestroy(GemStone.GemFire.Cache.Generic.EntryEvent<Object, Object> ev)
    {
      m_destroys++;
      Util.Log("TallyWriter::BeforeDestroy");
      CheckcallbackArg(ev);
      return !isWriterFailed;
    }

    public override bool BeforeRegionClear(GemStone.GemFire.Cache.Generic.RegionEvent<Object, Object> ev)
    {
      m_clears++;
      Util.Log("TallyWriter::BeforeRegionClear");
      return true;
    }

    public override bool BeforeUpdate(GemStone.GemFire.Cache.Generic.EntryEvent<Object, Object> ev)
    {
      m_updates++;
      Util.Log("TallyWriter::BeforeUpdate");
      CheckcallbackArg(ev);
      return !isWriterFailed;
    }
   #endregion
  }
}
