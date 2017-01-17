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

namespace GemStone.GemFire.Cache.Examples
{
  class BankAccount : ICacheableKey
  {
    #region Private members

    private int m_customerId;
    private int m_accountId;

    #endregion

    #region Public accessors

    public int Customer
    {
      get
      {
        return m_customerId;
      }
    }

    public int Account
    {
      get
      {
        return m_accountId;
      }
    }

    #endregion

    public BankAccount(int customer, int account)
    {
      m_customerId = customer;
      m_accountId = account;
    }

    // Our TypeFactoryMethod
    public static IGFSerializable CreateInstance()
    {
      return new BankAccount(0, 0);
    }

    #region IGFSerializable Members

    public void ToData(DataOutput output)
    {
      output.WriteInt32(m_customerId);
      output.WriteInt32(m_accountId);
    }

    public IGFSerializable FromData(DataInput input)
    {
      m_customerId = input.ReadInt32();
      m_accountId = input.ReadInt32();
      return this;
    }

    public UInt32 ClassId
    {
      get
      {
        return 0x04;
      }
    }

    public UInt32 ObjectSize
    {
      get
      {
        return (UInt32)(sizeof(Int32) + sizeof(Int32));
      }
    
    }

    #endregion

    #region ICacheableKey Members

    public bool Equals(ICacheableKey other)
    {
      BankAccount otherAccount = other as BankAccount;
      if (otherAccount != null)
      {
        return (m_customerId == otherAccount.m_customerId) &&
          (m_accountId == otherAccount.m_accountId);
      }
      return false;
    }

    public override int GetHashCode()
    {
      return (m_customerId ^ m_accountId);
    }

    #endregion

    #region Overriden System.Object methods

    public override bool Equals(object obj)
    {
      BankAccount otherAccount = obj as BankAccount;
      if (otherAccount != null)
      {
        return (m_customerId == otherAccount.m_customerId) &&
          (m_accountId == otherAccount.m_accountId);
      }
      return false;
    }

    // Also override ToString to get a nice string representation.
    public override string ToString()
    {
      return string.Format("BankAccount( customer: {0}, account: {1} )",
        m_customerId, m_accountId);
    }

    #endregion
  }
}
