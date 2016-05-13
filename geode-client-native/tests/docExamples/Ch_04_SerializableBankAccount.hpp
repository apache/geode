/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef SERIALIZABLE_BANKACCOUNT_HPP_
#define SERIALIZABLE_BANKACCOUNT_HPP_

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"
#include "CacheHelper.hpp"

using namespace gemfire;
using namespace docExample;

/**
* @brief Example 4.8 Implementing a Serializable Class.
* The next example shows a code sample that demonstrates how to implement a serializable class.
*/
class BankAccount
  : public gemfire::Serializable
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
  // Add the following for the Serializable interface
  // Our TypeFactoryMethod
  static gemfire::Serializable* createInstance( )
  {
    return new BankAccount( 0, 0 );
  }
  virtual int32_t classId( )const
  {
    return 10; // must be unique per class.
  }

  virtual uint32_t objectSize() const
  {
    return 10;
  }
  virtual void toData( gemfire::DataOutput& output )const
  {
    output.writeInt( m_ownerId );
    output.writeInt( m_accountId );
  }
  virtual Serializable* fromData( gemfire::DataInput& input )
  {
    input.readInt( &m_ownerId );
    input.readInt( &m_accountId );
    return this;
  }
};
typedef gemfire::SharedPtr< BankAccount > BankAccountPtr;


class ExampleBankAccountSerialization
{
public:
  ExampleBankAccountSerialization();
  ~ExampleBankAccountSerialization();
  void startServer();
  void stopServer();
  void initRegion();

public:
  CacheFactoryPtr cacheFactoryPtr; // for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regionPtr1; //for examples.
};

#endif /* SERIALIZABLE_BANKACCOUNT_HPP_ */
