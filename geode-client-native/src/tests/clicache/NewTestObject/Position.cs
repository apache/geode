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
  public class Position
    : IGFSerializable
  {
    #region Private members

    private long m_avg20DaysVol;
    private string m_bondRating;
    private double m_convRatio;
    private string m_country;
    private double m_delta;
    private long m_industry;
    private long m_issuer;
    private double m_mktValue;
    private double m_qty;
    private string m_secId;
    private string m_secLinks;
    private string m_secType;
    private int m_sharesOutstanding;
    private string m_underlyer;
    private long m_volatility;
    private int m_pid;

    private static int m_count = 0;

    #endregion

    #region Private methods

    private void Init()
    {
      m_avg20DaysVol = 0;
      m_bondRating = null;
      m_convRatio = 0.0;
      m_country = null;
      m_delta = 0.0;
      m_industry = 0;
      m_issuer = 0;
      m_mktValue = 0.0;
      m_qty = 0.0;
      m_secId = null;
      m_secLinks = null;
      m_secType = null;
      m_sharesOutstanding = 0;
      m_underlyer = null;
      m_volatility = 0;
      m_pid = 0;
    }

    private UInt32 GetObjectSize(IGFSerializable obj)
    {
      return (obj == null ? 0 : obj.ObjectSize);
    }

    #endregion

    #region Public accessors

    public string SecId
    {
      get
      {
        return m_secId;
      }
    }

    public int Id
    {
      get
      {
        return m_pid;
      }
    }

    public int SharesOutstanding
    {
      get
      {
        return m_sharesOutstanding;
      }
    }

    public static int Count
    {
      get
      {
        return m_count;
      }
      set
      {
        m_count = value;
      }
    }

    public override string ToString()
    {
      return "Position [secId=" + m_secId + " sharesOutstanding=" + m_sharesOutstanding + " type=" + m_secType + " id=" + m_pid + "]";
    }
    #endregion

    #region Constructors

    public Position()
    {
      Init();
    }

    //This ctor is for a data validation test
    public Position(Int32 iForExactVal)
    {
      Init();

      char[] id = new char[iForExactVal + 1];
      for (int i = 0; i <= iForExactVal; i++)
      {
        id[i] = 'a';
      }
      m_secId = id.ToString();
      m_qty = iForExactVal % 2 == 0 ? 1000 : 100;
      m_mktValue = m_qty * 2;
      m_sharesOutstanding = iForExactVal;
      m_secType = "a";
      m_pid = iForExactVal;
    }

    public Position(string id, int shares)
    {
      Init();
      m_secId = id;
      m_qty = shares * (m_count % 2 == 0 ? 10.0 : 100.0);
      m_mktValue = m_qty * 1.2345998;
      m_sharesOutstanding = shares;
      m_secType = "a";
      m_pid = m_count++;
    }

    #endregion

    #region IGFSerializable Members

    public IGFSerializable FromData(DataInput input)
    {
      m_avg20DaysVol = input.ReadInt64();
      m_bondRating = input.ReadUTF();
      m_convRatio = input.ReadDouble();
      m_country = input.ReadUTF();
      m_delta = input.ReadDouble();
      m_industry = input.ReadInt64();
      m_issuer = input.ReadInt64();
      m_mktValue = input.ReadDouble();
      m_qty = input.ReadDouble();
      m_secId = input.ReadUTF();
      m_secLinks = input.ReadUTF();
      m_secType = input.ReadUTF();
      m_sharesOutstanding = input.ReadInt32();
      m_underlyer = input.ReadUTF();
      m_volatility = input.ReadInt64();
      m_pid = input.ReadInt32();
      
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt64(m_avg20DaysVol);
      output.WriteUTF(m_bondRating);
      output.WriteDouble(m_convRatio);
      output.WriteUTF(m_country);
      output.WriteDouble(m_delta);
      output.WriteInt64(m_industry);
      output.WriteInt64(m_issuer);
      output.WriteDouble(m_mktValue);
      output.WriteDouble(m_qty);
      output.WriteUTF(m_secId);
      output.WriteUTF(m_secLinks);
      output.WriteUTF(m_secType);
      output.WriteInt32(m_sharesOutstanding);
      output.WriteUTF(m_underlyer);
      output.WriteInt64(m_volatility);
      output.WriteInt32(m_pid);
      
    }

    public UInt32 ObjectSize
    {
      get
      {
        UInt32 objectSize = 0;
        objectSize += (UInt32)sizeof(long);
        objectSize += (UInt32) (m_bondRating.Length * sizeof(char));
        objectSize += (UInt32)sizeof(double);
        objectSize += (UInt32)(m_country.Length * sizeof(char));
        objectSize += (UInt32)sizeof(double);
        objectSize += (UInt32)sizeof(Int64);
        objectSize += (UInt32)sizeof(Int64);
        objectSize += (UInt32)sizeof(double);
        objectSize += (UInt32)sizeof(double);
        objectSize += (UInt32)(m_secId.Length * sizeof(char));
        objectSize += (UInt32)(m_secLinks.Length * sizeof(char));
        objectSize += (UInt32)(m_secType == null ? 0 : sizeof(char) * m_secType.Length);
        objectSize += (UInt32)sizeof(Int32);
        objectSize += (UInt32)(m_underlyer.Length * sizeof(char));
        objectSize += (UInt32)sizeof(Int64);
        objectSize += (UInt32)sizeof(Int32);
        return objectSize;
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x07;
      }
    }

    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new Position();
    }
  }
}
