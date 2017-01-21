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
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Apache.Geode.Client.Tests.NewAPI;
namespace Apache.Geode.Client.FwkLib
{
  using Apache.Geode.DUnitFramework;
  using Apache.Geode.Client;
  public class ObjectHelper<TKey, TVal>
  {
    public static TVal CreateObject(string objectname, Int32 size, bool encodeKey, bool encodeTimestamp,
        Int32 assetSize,Int32 maxVal ,Int32 idx) {
      
      if (objectname.Equals("ArrayOfByte"))
      {
        return (TVal)(object)ArrayOfByte.Init(size,encodeKey,encodeTimestamp);
      }
      else if(objectname.Equals("BatchObject"))
      {
        return (TVal)(object) new BatchObject(idx, assetSize, size);
      }
      
      else if (objectname.Equals("PSTObject"))
      {
        return (TVal)(object) new PSTObject(size, encodeKey, encodeTimestamp);
      }
      else if(objectname.Equals("FastAssetAccount"))
      {
        return (TVal)(object) new FastAssetAccount(idx, encodeTimestamp, maxVal, assetSize);
      }
      else if (objectname.Equals("DeltaFastAssetAccount"))
      {
        return (TVal)(object) new DeltaFastAssetAccount(idx, encodeTimestamp, maxVal, assetSize, encodeKey);
      }
      else if (objectname == "EqStruct")
      {
        return (TVal)(object) new EqStruct(idx);
      }
      else if (objectname.Equals("DeltaPSTObject"))
      {
        return (TVal)(object)new DeltaPSTObject(size, encodeKey, encodeTimestamp);
      }
      else {
        Int32 bufSize = size;
        byte[] buf = new byte[bufSize];
        for (int i = 0; i < bufSize; i++)
        {
          buf[i] = 123;
        }
        Int32 rsiz = (bufSize <= 10) ? bufSize : 10;
        return (TVal)(object)buf;
      }
    }
  }
}
