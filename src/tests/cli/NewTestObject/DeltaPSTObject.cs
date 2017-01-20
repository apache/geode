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

namespace Apache.Geode.Client.Tests.NewAPI
{
  using Apache.Geode.Client.Generic;
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
