//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
namespace GemStone.GemFire.Cache.Tests
{
  public class DeltaFastAssetAccount
    : IGFSerializable, IGFDelta
  {
    private bool encodeTimestamp;
    private Int32 acctId;
    private CacheableString customerName = null;
    private double netWorth;
    private CacheableHashMap assets;
    private long timestamp;
    private bool getBeforeUpdate;
    private bool hasDelta = false;

    public DeltaFastAssetAccount()
    {
      encodeTimestamp = false;
      acctId = 0;
      customerName = null;
      netWorth = 0.0;
      assets = null;
      timestamp = 0;
      getBeforeUpdate = false;
    }
    public DeltaFastAssetAccount(Int32 index, bool encodeTimestp, Int32 maxVal,Int32 asstSize, bool getbfrUpdate)
    {
      customerName = CacheableString.Create("Milton Moneybags");
      netWorth = 0.0;
      assets = CacheableHashMap.Create();
      for (int i = 0; i < asstSize; i++) {
        FastAsset asset = new FastAsset(i, maxVal);
        assets.Add(CacheableInt32.Create(i), asset);
        netWorth += asset.GetValue();     
      }
      if(encodeTimestamp){
        DateTime startTime = DateTime.Now;
        timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
      }
      getBeforeUpdate = getbfrUpdate;
    }
    public UInt32 ObjectSize
    {
      get
      {
        return 0;
      }
    }
    public UInt32 ClassId
    {
      get
      {
        return 41;
      }
    }
    public IGFSerializable FromData(DataInput input)
    {
      acctId = input.ReadInt32();
      customerName = (CacheableString)input.ReadObject();
      netWorth = input.ReadDouble();
      assets = (CacheableHashMap)input.ReadObject();
      timestamp = input.ReadInt64();
      return this;
    }
    public void ToData(DataOutput output)
    {
      output.WriteInt32(acctId);
      output.WriteObject(customerName);
      output.WriteDouble(netWorth);
      output.WriteObject(assets);
      output.WriteInt64(timestamp);
    }
    public void ToDelta(DataOutput output)
    {
      output.WriteDouble(netWorth);
      if (encodeTimestamp)
      {
        output.WriteInt64(timestamp);
      }
    }
    public void FromDelta(DataInput input)
    {
      if (getBeforeUpdate)
        netWorth = input.ReadDouble();
      else
      {
        double netWorthTemp;
        netWorthTemp = input.ReadDouble();
        netWorth += netWorthTemp;
      }
      if (encodeTimestamp)
      {
        timestamp = input.ReadInt64();
      }
    }
    public bool HasDelta()
    {
      return hasDelta;
    }
    public Int32 GetAcctId()
    {
      return acctId;
    }

    public CacheableString GetCustomerName()
    {
      return customerName;
    }

    public double GetNetWorth()
    {
      return netWorth;
    }

    public void IncrementNetWorth()
    {
      ++netWorth;
    }

    public CacheableHashMap GetAssets()
    {
      return assets;
    }
    public Int32 GetIndex()
    {
      return acctId;
    }
    public long GetTimestamp()
    {
      if (encodeTimestamp)
      {
        return timestamp;
      }
      else
      {
        return 0;
      }
    }

    public void ResetTimestamp()
    {
      if (encodeTimestamp)
      {
        DateTime startTime = DateTime.Now;
        timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
      }
      else
      {
        timestamp = 0;
      }
    }
    public void Update()
    {
      IncrementNetWorth();
      if (encodeTimestamp)
      {
        ResetTimestamp();
      }
    }
    public Object Clone()
    {
      DeltaFastAssetAccount clonePtr = new DeltaFastAssetAccount();
      clonePtr.assets = CacheableHashMap.Create();
      Int32 mapCount = clonePtr.assets.Count;
      foreach (KeyValuePair<ICacheableKey, IGFSerializable> item in clonePtr.assets)
      {
        CacheableInt32 key = item.Key as CacheableInt32;
        FastAsset asset = item.Value as FastAsset;
        clonePtr.assets.Add(key, asset.Copy());
      }
      return clonePtr;
    }
    public static IGFSerializable CreateDeserializable()
    {
      return new DeltaFastAssetAccount();
    }
  }
}
