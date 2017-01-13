//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.Cache.Generic;
  public class BatchObject
    : TimeStampdObject
  {
    private Int32 index;
    private long timestamp;
    private Int32 batch;
    private byte[] byteArray;

    public BatchObject()
    {
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
        return 25;
      }
    }
    public override long GetTimestamp()
    {
      return timestamp;
    }
    public int GetIndex()
    {
      return index;
    }
    public int GetBatch()
    {
      return batch;
    }
    public override void ResetTimestamp()
    {
      DateTime startTime = DateTime.Now;
      timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
    }
    public static IGFSerializable CreateDeserializable()
    {
      return new BatchObject();
    }
    public BatchObject(Int32 anIndex, Int32 batchSize, Int32 size)
    {
      index = anIndex;
      DateTime startTime = DateTime.Now;
      timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
      batch = anIndex/batchSize;
      byteArray = new byte[size];
    }
    public override IGFSerializable FromData(DataInput input)
    {
      index = input.ReadInt32();
      timestamp = input.ReadInt64();
      batch = input.ReadInt32();
      byteArray = (byte[])input.ReadObject();
      return this;
    }
    public override void ToData(DataOutput output)
    {
      output.WriteInt32(index);
      output.WriteInt64(timestamp);
      output.WriteInt32(batch);
      output.WriteObject(byteArray);
    }
    public override string ToString()
    {
      string batchStr = string.Format("BatchObject:[index = {0} timestamp = {1} " +
        " batch = {2} byteArray={3}]", index, timestamp, batch, byteArray);

      return batchStr;
    }
  }
}
