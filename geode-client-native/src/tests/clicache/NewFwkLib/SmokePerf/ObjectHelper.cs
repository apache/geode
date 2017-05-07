//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using GemStone.GemFire.Cache.Tests.NewAPI;
namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
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
