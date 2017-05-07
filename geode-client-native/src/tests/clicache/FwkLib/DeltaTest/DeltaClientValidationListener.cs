using System;
using System.Threading;
using System.Collections.Generic;
using GemStone.GemFire.Cache.Tests;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  public class DeltaClientValidationListener : ICacheListener
  {
    private CacheableHashMap m_latestValues;
    private CacheableHashMap m_ValidateMap;
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
      m_latestValues = new CacheableHashMap();
      m_ValidateMap = new CacheableHashMap();
    }

    public static ICacheListener Create()
    {
      return new DeltaClientValidationListener();
    }
    ~DeltaClientValidationListener() { }

    #region ICacheListener Members

    public void AfterCreate(EntryEvent ev)
    {
      m_numAfterCreate++;
      CacheableKey key = ev.Key as CacheableKey;
      DeltaTestImpl value = ev.NewValue as DeltaTestImpl;
      if (value == null)
      {
        FwkTest.CurrentTest.FwkException(" Value in afterCreate cannot be null : key = {0} ", key.ToString());
        return;
      }
      if( value.GetIntVar() != 0 && value.GetFromDeltaCounter() != 0)
      {
         ValidateIncreamentByOne(key,value);
      }
      CacheableInt32 mapValue = CacheableInt32.Create(value.GetIntVar());
      CacheableInt64 deltaValue = CacheableInt64.Create(value.GetFromDeltaCounter());
      m_latestValues[key] = mapValue;
      m_ValidateMap[key] = deltaValue;
    }
      
    public void AfterUpdate(EntryEvent ev)
    {
      m_numAfterUpdate++;
      CacheableKey key = ev.Key as CacheableKey;
      DeltaTestImpl oldValue = ev.OldValue as DeltaTestImpl;
      DeltaTestImpl newValue = ev.NewValue as DeltaTestImpl;
      if (newValue == null)
      {
        FwkTest.CurrentTest.FwkException(" newValue in afterUpdate cannot be null : key = {0} ", key.ToString());
        return;
      }
       if (oldValue == null)
        {
          ValidateIncreamentByOne(key, newValue);
        }
        else
        {
          IGFSerializable mapValue1;
          m_latestValues.TryGetValue(key, out mapValue1);
          CacheableInt32 mapValue2 = mapValue1 as CacheableInt32;
          // CacheableInt32 mapValue2 = m_latestValues[key] as CacheableInt32;
          Int32 diff = newValue.GetIntVar() - mapValue2.Value;
          if (diff != 1)
          {
            FwkTest.CurrentTest.FwkException("difference expected in newValue and oldValue is 1 , but it was not" +
              " for key {0} & newVal = {1} oldValue = {2} map count = {3} : {4}", key.ToString(), newValue.GetIntVar(), mapValue2.Value, m_latestValues.Count, m_latestValues.ToString());
            return;
          }
        }
        CacheableInt32 mapValue = CacheableInt32.Create(newValue.GetIntVar());
        CacheableInt64 deltaValue = CacheableInt64.Create(newValue.GetFromDeltaCounter());
        m_latestValues[key] = mapValue;
        m_ValidateMap[key] = deltaValue;
    }

    public void AfterRegionLive(RegionEvent ev) 
    {
    }

    public void AfterDestroy(EntryEvent ev)
    {
      m_numAfterDestroy++;
      CacheableKey key = ev.Key as CacheableKey;
      DeltaTestImpl oldValue=ev.OldValue as DeltaTestImpl;
      if (oldValue != null)
      {
        CacheableInt32 mapValue = CacheableInt32.Create(oldValue.GetIntVar());
        m_latestValues.Remove(key);
        //m_ValidateMap.Remove(key);
      }
    }

    public void AfterInvalidate(EntryEvent ev)
    {
      m_numAfterInvalidate++;
      CacheableKey key = ev.Key as CacheableKey;
      DeltaTestImpl oldValue = ev.OldValue as DeltaTestImpl;
      if(oldValue==null)
      {
        FwkTest.CurrentTest.FwkException("oldValue cannot be null key = {0}",key.ToString());
        return;
      }
      CacheableInt32 mapValue = CacheableInt32.Create(oldValue.GetIntVar());
      m_latestValues[key] = null;
      
    }

    public void AfterRegionDestroy(RegionEvent ev)
    {
      dumbToBB(ev.Region);
    }

    public void AfterRegionInvalidate(RegionEvent ev)
    {
    }

    public void Close(Region region)
    {
    }

    public void AfterRegionDisconnected(Region region)
    {
    }
    public void AfterRegionClear(RegionEvent ev)
    {
      // Do nothing.
    }

    public CacheableHashMap getMap() 
    {
      return m_ValidateMap;
    }
   private void dumbToBB(Region region)
    {
      Util.BBSet("DeltaBB", "AFTER_CREATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterCreate);
      Util.BBSet("DeltaBB", "AFTER_UPDATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterUpdate);
      Util.BBSet("DeltaBB", "AFTER_INVALIDATE_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterInvalidate);
      Util.BBSet("DeltaBB", "AFTER_DESTROY_COUNT_" + Util.ClientId + "_" + region.Name, m_numAfterDestroy);
    }

    public void ValidateIncreamentByOne(CacheableKey key, DeltaTestImpl newValue)
    {
      CacheableInt32 oldValue =  m_latestValues[key] as CacheableInt32;
      if (oldValue.Value == 0)
      {
        FwkTest.CurrentTest.FwkException("oldValue in latestValues cannot be null: key = {0} & newVal = {1} ", key, newValue.ToString());
      }
      Int32 diff = newValue.GetIntVar() - oldValue.Value;
      if (diff != 1)
      {
        FwkTest.CurrentTest.FwkException("defference expected in newValue and oldValue is 1 , but it was {0}" +
          " for key {1} & newVal = {2}", diff, key, newValue.ToString());
      }
    } 
    #endregion
  }
}