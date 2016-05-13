/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef SIMPLEBANKACCOUNT_HPP_
#define SIMPLEBANKACCOUNT_HPP_

/**
* @brief Example 4.7 The Simple Class BankAccount.
* The next example demonstrates a simple BankAccount class that encapsulates two ints:
* ownerId and accountId.
*/
class BankAccount
{
private:
  int m_ownerId;
  int m_accountId;
public:
  BankAccount( int owner, int account )
    : m_ownerId( owner ),
    m_accountId( account )
  {
  }
  int getOwner( )
  {
    return m_ownerId;
  }
  int getAccount( )
  {
    return m_accountId;
  }
};

#endif /* SIMPLEBANKACCOUNT_HPP_ */
