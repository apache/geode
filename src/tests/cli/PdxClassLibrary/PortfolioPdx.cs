//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using GemStone.GemFire.Cache.Generic;


namespace PdxTests
{
  /// <summary>
  /// User class for testing the put functionality for object.
  /// </summary>
  public class PortfolioPdx
    : IPdxSerializable
  {
    #region Private members and methods

    private int m_id;
    private string m_pkid;
    private PositionPdx m_position1;
    private PositionPdx m_position2;
    private Hashtable m_positions;
    private string m_type;
    private string m_status;
    private string[] m_names;
    private byte[] m_newVal;
    private DateTime m_creationDate;
    private byte[] m_arrayZeroSize;
    private byte[] m_arrayNull;

    private static string[] m_secIds = { "SUN", "IBM", "YHOO", "GOOG", "MSFT",
      "AOL", "APPL", "ORCL", "SAP", "DELL" };

    private UInt32 GetObjectSize(IGFSerializable obj)
    {
      return (obj == null ? 0 : obj.ObjectSize);
    }

    #endregion

    #region Public accessors

    public int ID
    {
      get
      {
        return m_id;
      }
    }

    public string Pkid
    {
      get
      {
        return m_pkid;
      }
    }

    public PositionPdx P1
    {
      get
      {
        return m_position1;
      }
    }

    public PositionPdx P2
    {
      get
      {
        return m_position2;
      }
    }

    public Hashtable Positions
    {
      get
      {
        return m_positions;
      }
    }

    public string Status
    {
      get
      {
        return m_status;
      }
    }

    public bool IsActive
    {
      get
      {
        return (m_status == "active");
      }
    }

    public byte[] NewVal
    {
      get
      {
        return m_newVal;
      }
    }

    public byte[] ArrayNull
    {
      get
      {
        return m_arrayNull;
      }
    }

    public byte[] ArrayZeroSize
    {
      get
      {
        return m_arrayZeroSize;
      }
    }

    public DateTime CreationDate
    {
      get
      {
        return m_creationDate;
      }
    }

    public string Type
    {
      get
      {
        return m_type;
      }
    }

    public static string[] SecIds
    {
      get
      {
        return m_secIds;
      }
    }

    public override string ToString()
    {
      string portStr = string.Format("Portfolio [ID={0} status={1} " +
        "type={2} pkid={3}]", m_id, m_status, m_type, m_pkid);
      portStr += string.Format("{0}\tP1: {1}",
        Environment.NewLine, m_position1);
      portStr += string.Format("{0}\tP2: {1}",
        Environment.NewLine, m_position2);
      portStr += string.Format("{0}Creation Date: {1}", Environment.NewLine,
        m_creationDate);
      return portStr;
    }

    #endregion

    #region Constructors

    public PortfolioPdx()
    {
      m_id = 0;
      m_pkid = null;
      m_type = null;
      m_status = null;
      m_newVal = null;
      m_creationDate = new DateTime();
    }

    public PortfolioPdx(int id)
      : this(id, 0)
    {
    }

    public PortfolioPdx(int id, int size)
      : this(id, size, null)
    {
    }

    public PortfolioPdx(int id, int size, string[] names)
    {
      m_names = names;
      m_id = id;
      m_pkid = id.ToString();
      m_status = (id % 2 == 0) ? "active" : "inactive";
      m_type = "type" + (id % 3);
      int numSecIds = m_secIds.Length;
      m_position1 = new PositionPdx(m_secIds[PositionPdx.Count % numSecIds],
        PositionPdx.Count * 1000);
      if (id % 2 != 0)
      {
        m_position2 = new PositionPdx(m_secIds[PositionPdx.Count % numSecIds],
          PositionPdx.Count * 1000);
      }
      else
      {
        m_position2 = null;
      }
      m_positions = new Hashtable();
      m_positions[m_secIds[PositionPdx.Count % numSecIds]] =
        m_position1;
      if (size > 0)
      {
        m_newVal = new byte[size];
        for (int index = 0; index < size; index++)
        {
          m_newVal[index] = (byte)'B';
        }
      }
      m_creationDate = DateTime.Now;
      m_arrayNull = null;
      m_arrayZeroSize = new byte[0];
    }

    #endregion
      
    public static IPdxSerializable CreateDeserializable()
    {
      return new PortfolioPdx();
    }

    #region IPdxSerializable Members

    public void FromData(IPdxReader reader)
    {
      m_id = reader.ReadInt("id");

      bool isIdentity =  reader.IsIdentityField("id");

      if (isIdentity == false)
        throw new IllegalStateException("Portfolio id is identity field");

      bool isId = reader.HasField("id");

      if (isId == false)
        throw new IllegalStateException("Portfolio id field not found");

      bool isNotId = reader.HasField("ID");

      if (isNotId == true)
        throw new IllegalStateException("Portfolio isNotId field found");

      m_pkid = reader.ReadString("pkid");
      m_position1 = (PositionPdx)reader.ReadObject("position1");
      m_position2 = (PositionPdx)reader.ReadObject("position2");
      m_positions = (Hashtable)reader.ReadObject("positions");
      m_type = reader.ReadString("type");
      m_status = reader.ReadString("status");
      m_names = reader.ReadStringArray("names");
      m_newVal = reader.ReadByteArray("newVal");
      m_creationDate = reader.ReadDate("creationDate");
      m_arrayNull = reader.ReadByteArray("arrayNull");
      m_arrayZeroSize = reader.ReadByteArray("arrayZeroSize");
    }

    public void ToData(IPdxWriter writer)
    {
      writer.WriteInt("id", m_id)
      //identity field
      .MarkIdentityField("id")
      .WriteString("pkid", m_pkid)
      .WriteObject("position1", m_position1)
      .WriteObject("position2", m_position2)
      .WriteObject("positions", m_positions)
      .WriteString("type", m_type)
      .WriteString("status", m_status)
      .WriteStringArray("names", m_names)
      .WriteByteArray("newVal", m_newVal)
      .WriteDate("creationDate", m_creationDate)
      .WriteByteArray("arrayNull", m_arrayNull)
      .WriteByteArray("arrayZeroSize", m_arrayZeroSize);      
    }

    #endregion
  }
}
