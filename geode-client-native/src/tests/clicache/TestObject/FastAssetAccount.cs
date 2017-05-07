//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.Tests
{
  // need to implement
  public class FastAssetAccount
    : TimeStampdObject
  {
    // need to implement
    protected bool encodeTimestamp;
    protected Int32 acctId;
    protected CacheableString customerName;
    protected double netWorth;
    protected CacheableHashMap assets;
    protected long timestamp;

    public FastAssetAccount()
    {
      encodeTimestamp = false;
      acctId = 0;
      customerName = null;
      netWorth = 0.0;
      assets = null;
      timestamp = 0;
    }
    public FastAssetAccount(Int32 index, bool encodeTimestp, Int32 maxVal, Int32 asstSize)
    {
      customerName = CacheableString.Create("Milton Moneybags");
      netWorth = 0.0;
      assets = CacheableHashMap.Create();
      for (int i = 0; i < asstSize; i++)
      {
        FastAsset asset = new FastAsset(i, maxVal);
        assets.Add(CacheableInt32.Create(i), asset);
        netWorth += asset.GetValue();
      }
      if (encodeTimestamp)
      {
        DateTime startTime = DateTime.Now;
        timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
      }
    }
    public override UInt32 ObjectSize
    {
      get
      {
        return 0;
      }
    }
    public override UInt32 ClassId
    {
      get
      {
        return 23;
      }
    }
    public override IGFSerializable FromData(DataInput input)
    {
      acctId = input.ReadInt32();
      customerName = (CacheableString)input.ReadObject();
      netWorth = input.ReadDouble();
      assets = (CacheableHashMap)input.ReadObject();
      timestamp = input.ReadInt64();
      return this;
    }
    public override void ToData(DataOutput output)
    {
      output.WriteInt32(acctId);
      output.WriteObject(customerName);
      output.WriteDouble(netWorth);
      output.WriteObject(assets);
      output.WriteInt64(timestamp);
    }

    public static IGFSerializable CreateDeserializable()
    {
      return new FastAssetAccount();
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
    public override long GetTimestamp()
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

    public override void ResetTimestamp()
    {
      if (encodeTimestamp) {
        DateTime startTime = DateTime.Now;
        timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
      } else {
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
  }
}
