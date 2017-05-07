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
  public class PSTObject
    : TimeStampdObject
  {
    protected long timestamp;
    protected Int32 field1;
    protected SByte field2;
    protected byte[] valueData;

    public PSTObject()
    {
      timestamp = 0;
      valueData = null;
    }
    public PSTObject(Int32 size, bool encodeKey, bool encodeTimestamp)
    {
      DateTime startTime = DateTime.Now;
      timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
      field1 = 1234;
      field2 = 123;
      if (size == 0)
      {
        valueData = null;
      }
      else
      {
        encodeKey = true;
        valueData = ArrayOfByte.Init(size, encodeKey, false);
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
        return 4;
      }
    }
    public override IGFSerializable FromData(DataInput input)
    {
      timestamp = input.ReadInt64();
      field1 = input.ReadInt32();
      field2 = input.ReadSByte();
      valueData = input.ReadBytes();
      return this;
    }
    public override void ToData(DataOutput output)
    {
      output.WriteInt64(timestamp);
      output.WriteInt32(field1);
      output.WriteSByte(field2);
      output.WriteBytes(valueData);
    }

    public static IGFSerializable CreateDeserializable()
    {
      return new PSTObject();
    }
    public override long GetTimestamp()
    {
      return timestamp;
    }
    public override void ResetTimestamp()
    {
      DateTime startTime = DateTime.Now;
      timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
    }
     public override string ToString()
    {
      string portStr = string.Format("PSTObject [field1={0} field2={1}", field1, field2);
      return portStr;
    }

  }
}
