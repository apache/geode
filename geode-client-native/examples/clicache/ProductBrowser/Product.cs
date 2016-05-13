/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

using System;
using System.Collections.Generic;
using System.Text;
using GemStone.GemFire.Cache;

namespace ProductBrowser
{
  class Product : IGFSerializable
  {
    private int ProductID;
    private string Name;
    private string ProductNumber;
    private string MakeFlag;
    private string FinishedGoodsFlag;
    private string Color;
    private int SafetyStockLevel;
    private int ReorderPoint;
    private double StandardCost;
    private double ListPrice;
    private int DaysToManufacture;
    private string SellStartDate;
    private string DiscontinuedDate;

    private UInt32 GetStringSize(string str)
    {
      return (UInt32)(str == null ? 0 : sizeof(char) * str.Length);
    }

    public Product()
    {
    }

    public Product(int prodId, string prodName, string prodNum, string makeFlag, string finished, string color,
                    int safetyLock, int reorderPt, double stdCost, double listPrice, int mfgDays,
                    string startDate, string discDate)
    {
      ProductID = prodId;
      Name = prodName;
      ProductNumber = prodNum;
      MakeFlag = makeFlag;
      FinishedGoodsFlag = finished;
      Color = color;
      SafetyStockLevel = safetyLock;
      ReorderPoint = reorderPt;
      StandardCost = stdCost;
      ListPrice = listPrice;
      DaysToManufacture = mfgDays;
      SellStartDate = startDate;
      DiscontinuedDate = discDate;
    }

    public static IGFSerializable CreateInstance()
    {
      return new Product(0, null, null, Convert.ToString(false), Convert.ToString(false), null, 1000, 750, 0.00, 0.00, 5,
                          Convert.ToString(DateTime.Now), Convert.ToString(DateTime.Now));
    }

    public int productId
    {
      get
      {
        return ProductID;
      }
      set
      {
        ProductID = value;
      }
    }

    public string name
    {
      get
      {
        return Name;
      }
      set
      {
        Name = value;
      }
    }

    public string productNumber
    {
      get
      {
        return ProductNumber;
      }
      set
      {
        ProductNumber = value;
      }
    }

    public string makeFlag
    {
      get
      {
        return MakeFlag;
      }
      set
      {
        MakeFlag = value;
      }
    }

    public string finishedGoodsFlag
    {
      get
      {
        return FinishedGoodsFlag;
      }
      set
      {
        FinishedGoodsFlag = value;
      }
    }

    public string color
    {
      get
      {
        return Color;
      }
      set
      {
        Color = value;
      }
    }

    public int safetyLockLevel
    {
      get
      {
        return SafetyStockLevel;
      }
      set
      {
        SafetyStockLevel = value;
      }
    }

    public int reorderPoint
    {
      get
      {
        return ReorderPoint;
      }
      set
      {
        ReorderPoint = value;
      }
    }

    public double standardCost
    {
      get
      {
        return StandardCost;
      }
      set
      {
        StandardCost = value;
      }
    }

    public double listPrice
    {
      get
      {
        return ListPrice;
      }
      set
      {
        ListPrice = value;
      }
    }

    public int daysToManufacture
    {
      get
      {
        return DaysToManufacture;
      }
      set
      {
        DaysToManufacture = value;
      }
    }

    public string sellStartDate
    {
      get
      {
        return SellStartDate;
      }
      set
      {
        SellStartDate = value;
      }
    }

    public string discontinuedDate
    {
      get
      {
        return DiscontinuedDate;
      }
      set
      {
        DiscontinuedDate = value;
      }
    }

    #region IGFSerializable Members

    public UInt32 ClassId
    {
      get
      {
        return 0x07;
      }
    }

    public IGFSerializable FromData(DataInput input)
    {
      ProductID = input.ReadInt32();
      Name = input.ReadUTF();
      ProductNumber = input.ReadUTF();
      MakeFlag = input.ReadUTF();
      FinishedGoodsFlag = input.ReadUTF();
      Color = input.ReadUTF();
      SafetyStockLevel = input.ReadInt32();
      ReorderPoint = input.ReadInt32();
      StandardCost = input.ReadDouble();
      ListPrice = input.ReadDouble();
      DaysToManufacture = input.ReadInt32();
      SellStartDate = input.ReadUTF();
      DiscontinuedDate = input.ReadUTF();

      return this;

    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(ProductID);
      output.WriteUTF(Name);
      output.WriteUTF(ProductNumber);
      output.WriteUTF(MakeFlag);
      output.WriteUTF(FinishedGoodsFlag);
      output.WriteUTF(Color);
      output.WriteInt32(SafetyStockLevel);
      output.WriteInt32(ReorderPoint);
      output.WriteDouble(StandardCost);
      output.WriteDouble(ListPrice);
      output.WriteInt32(DaysToManufacture);
      output.WriteUTF(SellStartDate);
      output.WriteUTF(DiscontinuedDate);
    }

    public UInt32 ObjectSize
    {
      get
      {
        UInt32 objectSize = 0;
        objectSize += (UInt32)sizeof(Int32);
        objectSize += GetStringSize(Name);
        objectSize += GetStringSize(ProductNumber);
        objectSize += GetStringSize(MakeFlag);
        objectSize += GetStringSize(FinishedGoodsFlag);
        objectSize += GetStringSize(Color);
        objectSize += (UInt32)sizeof(Int32);
        objectSize += (UInt32)sizeof(Int32);
        objectSize += (UInt32)sizeof(double);
        objectSize += (UInt32)sizeof(double);
        objectSize += (UInt32)sizeof(double);
        objectSize += GetStringSize(SellStartDate);
        objectSize += GetStringSize(DiscontinuedDate);
        return objectSize;
      }
    }

    #endregion
  }
}
