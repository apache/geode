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
