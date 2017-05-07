/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

using System;
using System.Collections.Generic;

namespace GemStone.GemFire.Cache.Examples
{
  class AccountHistory : IGFSerializable
  {
    #region Private members

    private List<string> m_history;

    #endregion

    public AccountHistory()
    {
      m_history = new List<string>();
    }

    public void ShowAccountHistory()
    {
      Console.WriteLine("AccountHistory:");
      foreach (string hist in m_history)
      {
        Console.WriteLine("\t{0}", hist);
      }
    }

    public void AddLog(string entry)
    {
      m_history.Add(entry);
    }

    public static IGFSerializable CreateInstance()
    {
      return new AccountHistory();
    }

    #region IGFSerializable Members

    public IGFSerializable FromData(DataInput input)
    {
      int len = input.ReadInt32();

      m_history.Clear();
      for (int i = 0; i < len; i++)
      {
        m_history.Add(input.ReadUTF());
      }
      return this;
    }

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_history.Count);
      foreach (string hist in m_history)
      {
        output.WriteUTF(hist);
      }
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x05;
      }
    }
    
    public UInt32 ObjectSize
    {
      get
      {
        UInt32 objectSize = 0;
        foreach (string hist in m_history)
        {
          objectSize += (UInt32)(hist == null ? 0 : sizeof(char) * hist.Length);
        }
        return objectSize;

      }
      
    }

    #endregion
  }
}
