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
namespace Apache.Geode.Client.Tests.NewAPI
{
  using Apache.Geode.Client;
  // need to implement
  public class FastAssetAccount
    : TimeStampdObject
  {
    // need to implement
    protected bool encodeTimestamp;
    protected Int32 acctId;
    protected string customerName;
    protected double netWorth;
    protected Dictionary<int, FastAsset> assets;
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
      customerName = "Milton Moneybags";
      netWorth = 0.0;
      assets = new Dictionary<Int32, FastAsset>();
      for (int i = 0; i < asstSize; i++)
      {
        FastAsset asset = new FastAsset(i, maxVal);
        assets.Add(i, asset);
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
      customerName = (string)input.ReadObject();
      netWorth = input.ReadDouble();
      assets = new Dictionary<int, FastAsset>();
      input.ReadDictionary((System.Collections.IDictionary)assets);
      timestamp = input.ReadInt64();
      return this;
    }
    public override void ToData(DataOutput output)
    {
      output.WriteInt32(acctId);
      output.WriteObject(customerName);
      output.WriteDouble(netWorth);
      output.WriteDictionary((IDictionary)assets); // rjk currently not work as WriteDictionary API is not generic
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

    public IDictionary<int, FastAsset> GetAssets()
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
