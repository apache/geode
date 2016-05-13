//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections.Generic;
namespace GemStone.GemFire.Cache.Tests.NewAPI
{

  using GemStone.GemFire.Cache.Generic;
  /// <summary>
  /// User class for testing the put functionality for object.
  /// </summary>
  public class Portfolio
    : IGFSerializable
  {
    #region Private members and methods

    private int m_id;
    private string m_pkid;
    private Position m_position1;
    private Position m_position2;
    private Dictionary<Object, Object> m_positions;
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

    public Position P1
    {
      get
      {
        return m_position1;
      }
    }

    public Position P2
    {
      get
      {
        return m_position2;
      }
    }

    public IDictionary<Object, Object> Positions
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

    public Portfolio()
    {
      m_id = 0;
      m_pkid = null;
      m_type = null;
      m_status = null;
      m_newVal = null;
      m_creationDate = DateTime.MinValue;
    }

    public Portfolio(int id)
      : this(id, 0)
    {
    }

    public Portfolio(int id, int size)
      : this(id, size, null)
    {
    }

    public Portfolio(int id, int size, string[] names)
    {
      m_names = names;
      m_id = id;
      m_pkid = id.ToString();
      m_status = (id % 2 == 0) ? "active" : "inactive";
      m_type = "type" + (id % 3);
      int numSecIds = m_secIds.Length;
      m_position1 = new Position(m_secIds[Position.Count % numSecIds],
        Position.Count * 1000);
      if (id % 2 != 0)
      {
        m_position2 = new Position(m_secIds[Position.Count % numSecIds],
          Position.Count * 1000);
      }
      else
      {
        m_position2 = null;
      }
      m_positions = new Dictionary<Object, Object>();
      m_positions[(m_secIds[Position.Count % numSecIds])] = m_position1;
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

    #region IGFSerializable Members

    public IGFSerializable FromData(DataInput input)
    {
      m_id = input.ReadInt32();
      m_pkid = input.ReadUTF();
      m_position1 = (Position)input.ReadObject();
      m_position2 = (Position)input.ReadObject();
      m_positions = new Dictionary<object,object>();
      input.ReadDictionary((System.Collections.IDictionary)m_positions);
      m_type = input.ReadUTF();
      m_status = input.ReadUTF();
      m_names = (string[])(object)input.ReadObject();
      m_newVal = input.ReadBytes();
      //m_creationDate = (System.DateTime)input.ReadGenericObject();
      m_creationDate = input.ReadDate();
      m_arrayNull = input.ReadBytes();
      m_arrayZeroSize = input.ReadBytes();
      
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_id);
      output.WriteUTF(m_pkid);
      output.WriteObject(m_position1);
      output.WriteObject(m_position2);
      output.WriteDictionary((System.Collections.IDictionary)m_positions);
      output.WriteUTF(m_type);
      output.WriteUTF(m_status);
      output.WriteObject(m_names);
      output.WriteBytes(m_newVal);
      //output.WriteObject((IGFSerializable)(object)m_creationDate); // VJR: TODO
      //output.WriteObject(CacheableDate.Create(m_creationDate));
      output.WriteDate(m_creationDate);
      output.WriteBytes(m_arrayNull);
      output.WriteBytes(m_arrayZeroSize);
    }
    
  public UInt32 ObjectSize
    {
      get
      {
        UInt32 objectSize = 0;
        objectSize += (UInt32)sizeof(Int32);
        objectSize += (UInt32)(m_pkid.Length * sizeof(char));
        objectSize += GetObjectSize(m_position1);
        objectSize += GetObjectSize(m_position2);
        objectSize += (UInt32)(m_type.Length * sizeof(char));
        objectSize += (UInt32)(m_status == null ? 0 : sizeof(char) * m_status.Length);
        objectSize += (uint)m_names.Length;//TODO:need to calculate properly
        objectSize += (UInt32)(m_newVal == null ? 0 : sizeof(byte) * m_newVal.Length);
        objectSize += 8; //TODO:need to calculate properly for m_creationDate.;
        objectSize += (UInt32)(m_arrayZeroSize == null ? 0 : sizeof(byte) * m_arrayZeroSize.Length);
        objectSize += (UInt32)(m_arrayNull == null ? 0 : sizeof(byte) * m_arrayNull.Length);
        return objectSize;
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x08;
      }
    }

    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new Portfolio();
    }
  }
}
