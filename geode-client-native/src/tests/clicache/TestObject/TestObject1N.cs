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
  public class TestObject1
    : IGFSerializable
  {
    private string name;
    private byte[] arr;
    private int identifire;

    public TestObject1()
    {
    }
    public TestObject1(string objectName, int objectIdentifire)
    {
      name = objectName;
      byte[] arr1 = new byte[1024 * 4];
      arr = arr1;
      //Array.ForEach(arr, 'A');
      identifire = objectIdentifire;
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
        return 0x1F;
      }
    }
    public IGFSerializable FromData(DataInput input)
    {
      arr = input.ReadBytes();
      name = (string)input.ReadObject();
      identifire = input.ReadInt32();
      return this;
    }
    public void ToData(DataOutput output)
    {
      output.WriteBytes(arr);
      output.WriteObject(name);
      output.WriteInt32(identifire);
    }

    public static IGFSerializable CreateDeserializable()
    {
      return new TestObject1();
    }
  }
}
