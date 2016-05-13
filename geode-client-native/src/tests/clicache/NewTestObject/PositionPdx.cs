//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using GemStone.GemFire.Cache.Generic;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  public class PositionPdx
    : IPdxSerializable
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

    public string secId
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

    public int getSharesOutstanding
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

    public PositionPdx()
    {
      Init();
    }

    //This ctor is for a data validation test
    public PositionPdx(Int32 iForExactVal)
    {
      Init();

      char[] id = new char[iForExactVal + 1];
      for (int i = 0; i <= iForExactVal; i++)
      {
        id[i] = 'a';
      }
      m_secId = new string(id);
      m_qty = iForExactVal % 2 == 0 ? 1000 : 100;
      m_mktValue = m_qty * 2;
      m_sharesOutstanding = iForExactVal;
      m_secType = "a";
      m_pid = iForExactVal;
    }

    public PositionPdx(string id, int shares)
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
/*
   
    public UInt32 ClassId
    {
      get
      {
        return 0x02;
      }
    }

  */

    public static IPdxSerializable CreateDeserializable()
    {
      return new PositionPdx();
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_avg20DaysVol = reader.ReadLong("avg20DaysVol");
      m_bondRating = reader.ReadString("bondRating");
      m_convRatio = reader.ReadDouble("convRatio");
      m_country = reader.ReadString("country");
      m_delta = reader.ReadDouble("delta");
      m_industry = reader.ReadLong("industry");
      m_issuer = reader.ReadLong("issuer");
      m_mktValue = reader.ReadDouble("mktValue");
      m_qty = reader.ReadDouble("qty");
      m_secId = reader.ReadString("secId");
      m_secLinks = reader.ReadString("secLinks");
      m_secType = reader.ReadString("secType");
      m_sharesOutstanding = reader.ReadInt("sharesOutstanding");
      m_underlyer = reader.ReadString("underlyer");
      m_volatility = reader.ReadLong("volatility");
      m_pid = reader.ReadInt("pid");

    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteLong("avg20DaysVol", m_avg20DaysVol)
      .MarkIdentityField("avg20DaysVol")
      .WriteString("bondRating", m_bondRating)
      .MarkIdentityField("bondRating")
      .WriteDouble("convRatio", m_convRatio)
      .MarkIdentityField("convRatio")
      .WriteString("country", m_country)
      .MarkIdentityField("country")
      .WriteDouble("delta", m_delta)
      .MarkIdentityField("delta")
      .WriteLong("industry", m_industry)
      .MarkIdentityField("industry")
      .WriteLong("issuer", m_issuer)
      .MarkIdentityField("issuer")
      .WriteDouble("mktValue", m_mktValue)
      .MarkIdentityField("mktValue")
      .WriteDouble("qty", m_qty)
      .MarkIdentityField("qty")
      .WriteString("secId", m_secId)
      .MarkIdentityField("secId")
      .WriteString("secLinks", m_secLinks)
      .MarkIdentityField("secLinks")
      .WriteString("secType", m_secType)
      .MarkIdentityField("secType")
      .WriteInt("sharesOutstanding", m_sharesOutstanding)
      .MarkIdentityField("sharesOutstanding")
      .WriteString("underlyer", m_underlyer)
      .MarkIdentityField("underlyer")
      .WriteLong("volatility", m_volatility)
      .MarkIdentityField("volatility")
      .WriteInt("pid", m_pid)
      .MarkIdentityField("pid");
      //identity field
      //writer.MarkIdentityField("pid");
    }

    #endregion
  }
}

