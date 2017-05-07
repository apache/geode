
#ifndef __AccountHistory_hpp__
#define __AccountHistory_hpp__ 1


#include <gfcpp/GemfireCppCache.hpp>
#include <string>
#include <vector>

class AccountHistory;
typedef gemfire::SharedPtr< AccountHistory > AccountHistoryPtr;

/** 
 * Defines a custom type that can be used as a value in a
 * gemfire region.
 */
class AccountHistory : public gemfire::Cacheable
{
  private:
    std::vector< std::string > m_history;

  public:

  AccountHistory( );

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

  /** Log the state of this in a pretty fashion. */
  void showAccountHistory( ) const;
  
  /** Add a entry to the history. */
  void addLog( const std::string& entry );
  
  virtual uint32_t objectSize() const;
   
};

#endif

