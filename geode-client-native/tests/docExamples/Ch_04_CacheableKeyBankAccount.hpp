/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#ifndef CACHEABLEKEYBANKACCOUNT_HPP_
#define CACHEABLEKEYBANKACCOUNT_HPP_

#include <gfcpp/gf_types.hpp>
#include "gfcpp/GemfireCppCache.hpp"
#include "CacheHelper.hpp"

using namespace gemfire;

/**
* @brief Example 4.9 Extending a Serializable Class To Be a CacheableKey.
* The following example for sample code showing how to extend a serializable class to be a
* cacheable key.
*/
class BankAccount
  : public gemfire::CacheableKey
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
  // Add the following for the CacheableKey interface
  bool operator == ( const CacheableKey& other ) const
  {
    const BankAccount& otherBA =
      static_cast<const BankAccount&>( other );
    return (m_ownerId == otherBA.m_ownerId) &&
      (m_accountId == otherBA.m_accountId);
  }
  uint32_t hashcode( ) const
  {
    return m_ownerId;
  }
};
typedef gemfire::SharedPtr< BankAccount > BankAccountPtr;


class ExampleBankAccountCacheable
{
public:
  ExampleBankAccountCacheable();
  ~ExampleBankAccountCacheable();
  void startServer();
  void stopServer();
  void initRegion();
public:
  CacheFactoryPtr cacheFactoryPtr; // for examples.
  CachePtr cachePtr; //for examples.
  RegionPtr regionPtr1; //for examples.
};
#endif /* CACHEABLEKEYBANKACCOUNT_HPP_ */
