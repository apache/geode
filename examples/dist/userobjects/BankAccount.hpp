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

#ifndef __BankAccount_hpp__
#define __BankAccount_hpp__ 1


#include <gfcpp/GemfireCppCache.hpp>


class BankAccount;
typedef apache::geode::client::SharedPtr< BankAccount > BankAccountPtr;

/**
 * Defines a custom type that can be used as a key in
 * a gemfire region.
 */
class BankAccount : public apache::geode::client::CacheableKey
{
  private:

    int m_customerId; 
    int m_accountId;

  public:

  BankAccount( int customerNum, int accountNum );

   /**
   *@brief serialize this object
   **/
  virtual void toData( apache::geode::client::DataOutput& output ) const;

  /**
   *@brief deserialize this object
   **/
  virtual apache::geode::client::Serializable* fromData( apache::geode::client::DataInput& input );
  
  /**
   * @brief creation function for strings.
   */
  static apache::geode::client::Serializable* createDeserializable( );

  /**
   *@brief return the classId of the instance being serialized.
   * This is used by deserialization to determine what instance
   * type to create and derserialize into.
   */
  virtual int32_t classId( ) const;

  /** return true if this key matches other. */
  virtual bool operator==( const apache::geode::client::CacheableKey& other ) const;

  /** return the hashcode for this key. */
  virtual uint32_t hashcode( ) const;
  
  /** Log the state of this in a pretty fashion. */
  void showAccountIdentifier( ) const;
   
  virtual uint32_t objectSize() const;   
};

namespace apache {
namespace geode {
namespace client {

/** overload of apache::geode::client::createKey to pass CacheableInt32Ptr */
inline CacheableKeyPtr createKey( const BankAccountPtr& value )
{
  return value;
}

}

#endif

