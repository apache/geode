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
  public class TimeStampdObject
    : IGFSerializable
  {

    public virtual UInt32 ObjectSize
    {
      get
      {
        return 0;
      }
    }
    public virtual UInt32 ClassId
    {
      get
      {
        return 0;
      }
    }
    public virtual IGFSerializable FromData(DataInput input)
    {
      return this;
    }
    public virtual void ToData(DataOutput output)
    {
    }
    public virtual long GetTimestamp()
    {
      return 0;
    }
    public virtual void ResetTimestamp() { }
    
     
  }
}
