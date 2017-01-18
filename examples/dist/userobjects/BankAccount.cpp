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

#include "BankAccount.hpp"
#include "EClassIds.hpp"
#include <stdio.h>

using namespace apache::geode::client;

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
  return EClassIds::BankAccount;
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
