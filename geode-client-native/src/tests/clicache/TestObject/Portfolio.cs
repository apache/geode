//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;

namespace GemStone.GemFire.Cache.Tests
{
  /// <summary>
  /// User class for testing the put functionality for object.
  /// </summary>
  public class Portfolio
    : IGFSerializable
  {
    #region Private members and methods

    private int m_id;
    private CacheableString m_pkid;
    private Position m_position1;
    private Position m_position2;
    private CacheableHashMap m_positions;
    private CacheableString m_type;
    private string m_status;
    private CacheableStringArray m_names;
    private byte[] m_newVal;
    private CacheableDate m_creationDate;
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

    public CacheableString Pkid
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

    public CacheableHashMap Positions
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

    public CacheableDate CreationDate
    {
      get
      {
        return m_creationDate;
      }
    }

    public CacheableString Type
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
      m_creationDate = null;      
    }

    public Portfolio(int id)
      : this(id, 0)
    {
    }

    public Portfolio(int id, int size)
      : this(id, size, null)
    {
    }

    public Portfolio(int id, int size, CacheableStringArray names)
    {
      m_names = names;
      m_id = id;
      m_pkid = new CacheableString(id.ToString());
      m_status = (id % 2 == 0) ? "active" : "inactive";
      m_type = new CacheableString("type" + (id % 3));
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
      m_positions = new CacheableHashMap();
      m_positions[new CacheableString(m_secIds[Position.Count % numSecIds])] =
        m_position1;
      if (size > 0)
      {
        m_newVal = new byte[size];
        for (int index = 0; index < size; index++)
        {
          m_newVal[index] = (byte)'B';
        }
      }
      m_creationDate = new CacheableDate(DateTime.Now);
      m_arrayNull = null;
      m_arrayZeroSize = new byte[0];
    }

    #endregion

    #region IGFSerializable Members

    public IGFSerializable FromData(DataInput input)
    {
      m_id = input.ReadInt32();
      m_pkid = (CacheableString)input.ReadObject();
      m_position1 = (Position)input.ReadObject();
      m_position2 = (Position)input.ReadObject();
      m_positions = (CacheableHashMap)input.ReadObject();
      m_type = (CacheableString)input.ReadObject();
      m_status = input.ReadUTF();
      m_names = (CacheableStringArray)input.ReadObject();
      m_newVal = input.ReadBytes();
      m_creationDate = (CacheableDate)input.ReadObject();
      m_arrayNull = input.ReadBytes();
      m_arrayZeroSize = input.ReadBytes();
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_id);
      Log.Fine("Portoflio before writing data");
      output.WriteObject(m_pkid);
      Log.Fine("Portoflio after writing data");
      output.WriteObject(m_position1);
      output.WriteObject(m_position2);
      output.WriteObject(m_positions);
      output.WriteObject(m_type);
      output.WriteUTF(m_status);
      output.WriteObject(m_names);
      output.WriteBytes(m_newVal);
      Log.Fine("Portoflio before writing date");
      output.WriteObject(m_creationDate);
      Log.Fine("Portoflio after writing date");

      output.WriteBytes(m_arrayNull);
      output.WriteBytes(m_arrayZeroSize);
    }
    public UInt32 ObjectSize
    {
      get
      {
        UInt32 objectSize = 0;
        objectSize += (UInt32)sizeof(Int32);
        objectSize += GetObjectSize(m_pkid);
        objectSize += GetObjectSize(m_position1);
        objectSize += GetObjectSize(m_position2);
        objectSize += GetObjectSize(m_positions);
        objectSize += GetObjectSize(m_type);
        objectSize += (UInt32)(m_status == null ? 0 :
          sizeof(char) * m_status.Length);
        objectSize += GetObjectSize(m_names);
        objectSize += (UInt32)(m_newVal == null ? 0 :
          sizeof(byte) * m_newVal.Length);
        objectSize += GetObjectSize(m_creationDate);
        objectSize += (UInt32)(m_arrayZeroSize == null ? 0 :
          sizeof(byte) * m_arrayZeroSize.Length);
        objectSize += (UInt32)(m_arrayNull == null ? 0 :
          sizeof(byte) * m_arrayNull.Length);
        return objectSize;
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x03;
      }
    }

    #endregion

    public static IGFSerializable CreateDeserializable()
    {
      return new Portfolio();
    }
  }
}
