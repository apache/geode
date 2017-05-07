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
  public class DeltaPSTObject
    : IGFSerializable, IGFDelta
  {
    private long timestamp;
    private Int32 field1;
    private SByte field2;
    private byte[] valueData;
    private bool hasDelta = false;

    public DeltaPSTObject()
    {
      timestamp = 0;
      valueData = null;
    }

    public DeltaPSTObject(Int32 size, bool encodeKey, bool encodeTimestamp)
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
        return 42;
      }
    }
    public IGFSerializable FromData(DataInput input)
    {
      timestamp = input.ReadInt64();
      field1 = input.ReadInt32();
      field2 =input.ReadSByte();
      valueData = input.ReadBytes();
      return this;
    }
    public void ToData(DataOutput output)
    {
      output.WriteInt64(timestamp);
      output.WriteInt32(field1);
      output.WriteSByte(field2);
      output.WriteBytes(valueData);
    }
    public void ToDelta(DataOutput output)
    {
      output.WriteInt32(field1);
      output.WriteInt64(timestamp);
    }
    public void FromDelta(DataInput input)
    {
      field1 = input.ReadInt32();
      timestamp = input.ReadInt64();
    }
    public bool HasDelta()
    {
      return hasDelta;
    }

    public void Update()
    {
      IncrementField1();
      ResetTimestamp();
    }
    public void IncrementField1()
    {
      ++field1;
    }
    public void ResetTimestamp()
    {
      DateTime startTime = DateTime.Now;
      timestamp = startTime.Ticks * (1000000 / TimeSpan.TicksPerMillisecond);
    }
    public Object Clone()
    {
      return new DeltaPSTObject();
    }
    public static IGFSerializable CreateDeserializable()
    {
      return new DeltaPSTObject();
    }
  }
}
