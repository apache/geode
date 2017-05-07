/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
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
