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
