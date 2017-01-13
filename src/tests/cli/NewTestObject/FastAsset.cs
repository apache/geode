//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Cache.Generic;
 public class FastAsset
    : TimeStampdObject
  {
    private Int32 assetId;
    private double value;

    public FastAsset()
    {
      assetId = 0;
      value = 0;
    }
   public FastAsset(int idx, int maxVal)
    {
      assetId = idx;
      Random temp = new Random(12);
      value = temp.NextDouble() * (maxVal - 1) + 1;
      
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
        return 24;
      }
    }
    public override IGFSerializable FromData(DataInput input)
    {
      assetId = input.ReadInt32();
      value = input.ReadDouble();
      return this;
    }
    public override void ToData(DataOutput output)
    {
      output.WriteInt32(assetId);
      output.WriteDouble(value);
    }

    public static IGFSerializable CreateDeserializable()
    {
      return new FastAsset();
    }
   /**
   * Returns the id of the asset.
   */
  public Int32 GetAssetId() {
    return assetId;
  }

  /**
   * Returns the asset value.
   */
  public double GetValue() {
    return value;
  }

  /**
   * Sets the asset value.
   */
  public void SetValue(double d) {
    value = d;
  }

  public Int32 GetIndex() {
    return assetId;
  }
  /**
   * Makes a copy of this asset.
   */
  public FastAsset Copy() {
    FastAsset asset = new FastAsset();
    asset.SetAssetId(GetAssetId());
    asset.SetValue(GetValue());
    return asset;
  }
  /**
   * Sets the id of the asset.
   */
  public void SetAssetId(int i) {
    assetId = i;
  }

  public string toString() 
  {
    string assetStr = string.Format("FastAsset:[assetId =  = {0} value = {1} ", assetId, value);
    return assetStr;
  }
}
}
