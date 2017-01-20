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
using System.Collections.Generic;

namespace Apache.Geode.Client.FwkLib
{
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client.Tests.NewAPI;
  using Apache.Geode.Client.Generic;
  public class DeltaClientValidationListener<TKey, TVal> : CacheListenerAdapter<TKey, TVal>
  {
    private IDictionary<TKey, Int32> m_latestValues;
    private IDictionary<TKey, Int64> m_ValidateMap;
    private long m_numAfterCreate;
    private long m_numAfterUpdate;
    private long m_numAfterInvalidate;
    private long m_numAfterDestroy;
    
    public DeltaClientValidationListener()
    {
      m_numAfterCreate = 0;
      m_numAfterUpdate = 0;
      m_numAfterInvalidate = 0;
      m_numAfterDestroy = 0;
      m_latestValues = new Dictionary<TKey, Int32>();
      m_ValidateMap = new Dictionary<TKey, Int64>();
    }

    public static ICacheListener<TKey, TVal> Create()
    {
      return new DeltaClientValidationListener<TKey, TVal>();
    }
    ~DeltaClientValidationListener() { }

    #region ICacheListener Members

    public override void AfterCreate(EntryEvent<TKey, TVal> ev)
    {
      m_numAfterCreate++;
      TKey key = (TKey)ev.Key;
      DeltaTestImpl value = ev.NewValue as DeltaTestImpl;
      if (value == null)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkException(" Value in afterCreate cannot be null : key = {0} ", key.ToString());
        return;
      }
      if( value.GetIntVar() != 0 && value.GetFromDeltaCounter() != 0)
      {
         ValidateIncreamentByOne(key,value);
      }
      Int32 mapValue = value.GetIntVar();
      Int64 deltaValue = value.GetFromDeltaCounter();
      m_latestValues[key] = mapValue;
      m_ValidateMap[key] = deltaValue;
    }

    public override void AfterUpdate(EntryEvent<TKey, TVal> ev)
    {
      m_numAfterUpdate++;
      TKey key = ev.Key;
      DeltaTestImpl oldValue = ev.OldValue as DeltaTestImpl;
      DeltaTestImpl newValue = ev.NewValue as DeltaTestImpl;
      if (newValue == null)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkException(" newValue in afterUpdate cannot be null : key = {0} ", key.ToString());
        return;
      }
       if (oldValue == null)
        {
          ValidateIncreamentByOne(key, newValue);
        }
        else
        {
          Int32 mapValue1;
          m_latestValues.TryGetValue(key, out mapValue1);
          Int32 mapValue2 = mapValue1;
          // CacheableInt32 mapValue2 = m_latestValues[key] as CacheableInt32;
          Int32 diff = newValue.GetIntVar() - mapValue2;
          if (diff != 1)
          {
            FwkTest<TKey, TVal>.CurrentTest.FwkException("difference expected in newValue and oldValue is 1 , but it was not" +
              " for key {0} & newVal = {1} oldValue = {2} map count = {3} : {4}", key.ToString(), newValue.GetIntVar(), mapValue2, m_latestValues.Count, m_latestValues.ToString());
            return;
          }
        }
        Int32 mapValue = newValue.GetIntVar();
        Int64 deltaValue = newValue.GetFromDeltaCounter();
        m_latestValues[key] = mapValue;
        m_ValidateMap[key] = deltaValue;
    }

    public override void AfterRegionLive(RegionEvent<TKey, TVal> ev) 
    {
    }

    public override void AfterDestroy(EntryEvent<TKey, TVal> ev)
    {
      m_numAfterDestroy++;
      TKey key = ev.Key;
      DeltaTestImpl oldValue=ev.OldValue as DeltaTestImpl;
      if (oldValue != null)
      {
        Int32 mapValue = oldValue.GetIntVar();
        m_latestValues.Remove(key);
        //m_ValidateMap.Remove(key);
      }
    }

    public override void AfterInvalidate(EntryEvent<TKey, TVal> ev)
    {
      m_numAfterInvalidate++;
      TKey key = ev.Key;
      DeltaTestImpl oldValue = ev.OldValue as DeltaTestImpl;
      if(oldValue==null)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkException("oldValue cannot be null key = {0}", key.ToString());
        return;
      }
      Int32 mapValue = oldValue.GetIntVar();
      m_latestValues[key] = 0;
      
    }

    public override void AfterRegionDestroy(RegionEvent<TKey, TVal> ev)
    {
      dumbToBB(ev.Region);
    }

    public override void AfterRegionInvalidate(RegionEvent<TKey, TVal> ev)
    {
    }

    public void Close(Apache.Geode.Client.Generic.IRegion<TKey, TVal> region)
    {
    }

    public void AfterRegionDisconnected(Apache.Geode.Client.Generic.IRegion<TKey, TVal> region)
    {
    }
    public override void AfterRegionClear(RegionEvent<TKey, TVal> ev)
    {
      // Do nothing.
    }

    public IDictionary<TKey, Int64> getMap() 
    {
      return m_ValidateMap;
    }
    private void dumbToBB(IRegion<TKey, TVal> region)
    {
      Util.BBSet("DeltaBB", "AFTER_CREATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterCreate);
      Util.BBSet("DeltaBB", "AFTER_UPDATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterUpdate);
      Util.BBSet("DeltaBB", "AFTER_INVALIDATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterInvalidate);
      Util.BBSet("DeltaBB", "AFTER_DESTROY_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterDestroy);
    }

    public void ValidateIncreamentByOne(TKey key, DeltaTestImpl newValue)
    {
      Int32 oldValue =  m_latestValues[key];
      if (oldValue == 0)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkException("oldValue in latestValues cannot be null: key = {0} & newVal = {1} ", key, newValue.ToString());
      }
      Int32 diff = newValue.GetIntVar() - oldValue;
      if (diff != 1)
      {
        FwkTest<TKey, TVal>.CurrentTest.FwkException("defference expected in newValue and oldValue is 1 , but it was {0}" +
          " for key {1} & newVal = {2}", diff, key, newValue.ToString());
      }
    } 
    #endregion
  }
}