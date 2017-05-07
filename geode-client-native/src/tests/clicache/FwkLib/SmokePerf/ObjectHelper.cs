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
using GemStone.GemFire.Cache.Tests;
namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  public class ObjectHelper
  {
    public static IGFSerializable CreateObject(string objectname, Int32 size, bool encodeKey, bool encodeTimestamp,
        Int32 assetSize,Int32 maxVal ,Int32 idx) {
      
      if (objectname.Equals("ArrayOfByte"))
      {
        return  ArrayOfByte.Init(size,encodeKey,encodeTimestamp);
      }
      else if(objectname.Equals("BatchObject"))
      {
        return new BatchObject(idx,assetSize,size);
      }
      
      else if (objectname.Equals("PSTObject"))
      {
        return new PSTObject(size,encodeKey,encodeTimestamp);
      }
      else if(objectname.Equals("FastAssetAccount"))
      {
        return new FastAssetAccount(idx,encodeTimestamp,maxVal,assetSize);
      }
      else if (objectname.Equals("DeltaFastAssetAccount"))
      {
        return new DeltaFastAssetAccount(idx, encodeTimestamp,maxVal,assetSize,encodeKey);
      }
      else if (objectname.Equals("DeltaPSTObject"))
      {
        return new DeltaPSTObject(size,encodeKey,encodeTimestamp);
      }
      /*else if(objectname == "FlatObject")
      {
      }
      else if( objectname == "SizedString")
      {
        return;
      }
      else if( objectname == "BatchString")
      {
        return;
      }
      else if( objectname == "TestInteger")
      {
        return;
      }*/
      else {
        Int32 bufSize = size;
        byte[] buf = new byte[bufSize];
        for (int i = 0; i < bufSize; i++)
        {
          buf[i] = 123;
        }
        Int32 rsiz = (bufSize <= 10) ? bufSize : 10;
        return CacheableBytes.Create(buf, bufSize);
      }
    }
  }
}
