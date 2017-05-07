
#ifndef __BankAccount_hpp__
#define __BankAccount_hpp__ 1


#include <gfcpp/GemfireCppCache.hpp>


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

#endif

