
#include "AccountHistory.hpp"
#include "EClassIds.hpp"
#include <stdio.h>

using namespace gemfire;

AccountHistory::AccountHistory( )
: Cacheable(),
  m_history()
{
}

/**
 * @brief serialize this object
 **/
void AccountHistory::toData( DataOutput& output ) const
{
  size_t itemCount = m_history.size();
  output.writeInt( (int32_t) itemCount );
  for( size_t idx = 0; idx < itemCount; idx++ ) {
    // copy each string to the serialization buffer, including the null
    // terminating character at the end of the string.
    output.writeBytes( (int8_t*) m_history[idx].c_str(), m_history[idx].size() + 1 );
  }
}

/**
 * @brief deserialize this object
 **/
Serializable* AccountHistory::fromData( DataInput& input )
{
  size_t buflen = 1000;
  char* readbuf = new char[buflen];
  uint32_t itemCount = 0;
  uint32_t itemLength = 0;

  input.readInt( (uint32_t*) &itemCount );
  for( size_t idx = 0; idx < itemCount; idx++ ) {
    input.readInt( (uint32_t*) &itemLength );
    // grow the read buffer if an item exceeds the length.
    if ( buflen <= itemLength ) {
      buflen = itemLength;
      delete [] readbuf;
      readbuf = new char[buflen];
    }
    // read from serialization buffer into a character array
    input.readBytesOnly((uint8_t*) readbuf, itemLength);
    // and store in the history list of strings.
    m_history.push_back( readbuf );
  }
  return this;
}

/**
 * @brief creation function for strings.
 */
Serializable* AccountHistory::createDeserializable( )
{
  return new AccountHistory();
}

/**
 *@brief return the classId of the instance being serialized.
 * This is used by deserialization to determine what instance
 * type to create and derserialize into.
 */
int32_t AccountHistory::classId( ) const
{
  return EClassIds::AccountHistory;
}

/** Log the state of this in a pretty fashion. */
void AccountHistory::showAccountHistory( ) const
{
  printf( "AccountHistory: \n" );
  for( size_t idx = 0; idx < m_history.size(); idx++ ) {
    printf( "  %s\n", m_history[idx].c_str() );
  }
}

/** Add a entry to the history. */
void AccountHistory::addLog( const std::string& entry )
{
  m_history.push_back( entry );
}

uint32_t AccountHistory::objectSize( ) const
{
  size_t itemCount = m_history.size();
  uint32_t size = sizeof(AccountHistory);
  size += sizeof(itemCount);
  for( size_t idx = 0; idx < itemCount; idx++ ) {
    size += sizeof(char) * (m_history[idx].size()+1);
  }
  return size;

}

