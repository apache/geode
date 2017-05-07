//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.Cache.Generic;
  public class DeltaFastAssetAccount
    : IGFSerializable, IGFDelta
  {
    private bool encodeTimestamp;
    private Int32 acctId;
    private string customerName = null;
    private double netWorth;
    private Dictionary<int,FastAsset> assets;
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
      customerName = "Milton Moneybags";
      netWorth = 0.0;
      assets = new Dictionary<Int32,FastAsset>();
      for (int i = 0; i < asstSize; i++) {
        FastAsset asset = new FastAsset(i, maxVal);
        assets.Add(i, asset);
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
      customerName = input.ReadUTF();
      netWorth = input.ReadDouble();
      assets = new Dictionary<int,FastAsset>();
      input.ReadDictionary((System.Collections.IDictionary)assets);
      timestamp = input.ReadInt64();
      return this;
    }
    public void ToData(DataOutput output)
    {
      output.WriteInt32(acctId);
      output.WriteUTF(customerName);
      output.WriteDouble(netWorth);
      output.WriteDictionary((IDictionary)assets); // rjk currently not work as WriteDictionary API is not generic
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

    public string GetCustomerName()
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

    public IDictionary<int,FastAsset> GetAssets()
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
      clonePtr.assets = new Dictionary<Int32, FastAsset>();
      Int32 mapCount = clonePtr.assets.Count;
      foreach (KeyValuePair<int, FastAsset> item in clonePtr.assets)
      {
        Int32 key = item.Key;
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
