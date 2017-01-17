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
