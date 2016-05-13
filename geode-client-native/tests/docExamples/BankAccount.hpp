/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef BANKACCOUNT_HPP_
#define BANKACCOUNT_HPP_

#include <gfcpp/GemfireCppCache.hpp>
#include <stdio.h>

using namespace gemfire;
using namespace docExample;

class BankAccount;
typedef gemfire::SharedPtr< BankAccount > BankAccountPtr;

/**
* Defines a custom type that can be used as a key in
* a gemfire region.
*/
class BankAccount : public gemfire::CacheableKey
{
private:
  int m_customerId;
  int m_accountId;

public:
  BankAccount( int customerNum, int accountNum );

  /**
  *@brief serialize this object
  **/
  virtual void toData( gemfire::DataOutput& output ) const;

  /**
  *@brief deserialize this object
  **/
  virtual gemfire::Serializable* fromData( gemfire::DataInput& input );

  /**
  * @brief creation function for strings.
  */
  static gemfire::Serializable* createDeserializable( );

  /**
  *@brief return the classId of the instance being serialized.
  * This is used by deserialization to determine what instance
  * type to create and derserialize into.
  */
  virtual int32_t classId( ) const;

  /** return true if this key matches other. */
  virtual bool operator==( const gemfire::CacheableKey& other ) const;

  /** return the hashcode for this key. */
  virtual uint32_t hashcode( ) const;

  /** Log the state of this in a pretty fashion. */
  void showAccountIdentifier( ) const;

  virtual uint32_t objectSize() const;

};

namespace gemfire {

  /** overload of gemfire::createKey to pass CacheableInt32Ptr */
  inline CacheableKeyPtr createKey( const BankAccountPtr& value )
  {
    return value;
  }

}

BankAccount::BankAccount( int customerNum, int accountNum )
: CacheableKey(),
m_customerId( customerNum ),
m_accountId( accountNum )
{
}

void BankAccount::toData( DataOutput& output ) const
{
  // write each field to the DataOutput.
  output.writeInt( m_customerId );
  output.writeInt( m_accountId );
}

Serializable* BankAccount::fromData( DataInput& input )
{
  // set each field from the data input.
  input.readInt( &m_customerId );
  input.readInt( &m_accountId );
  return this;
}

Serializable* BankAccount::createDeserializable( )
{
  // Create a new instance that will be initialized later by a call to fromData.
  return new BankAccount( 0, 0 );
}

int32_t BankAccount::classId( ) const
{
	return 11;
}

bool BankAccount::operator==( const CacheableKey& other ) const
{
  const BankAccount& rhs = static_cast< const BankAccount& >( other );
  return ( m_customerId == rhs.m_customerId )
    && ( m_accountId == rhs.m_accountId );
}

uint32_t BankAccount::hashcode( ) const
{
  return /* not the best hash.. */ m_customerId + ( m_accountId << 3 );
}

void BankAccount::showAccountIdentifier( ) const
{
  printf( "BankAccount( customer: %d, account: %d )\n",
    m_customerId, m_accountId );
}

uint32_t BankAccount::objectSize( ) const
{
  return sizeof(BankAccount) ;
}

#endif /* BANKACCOUNT_HPP_ 1*/
