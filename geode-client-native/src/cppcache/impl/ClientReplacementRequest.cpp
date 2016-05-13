#include "ClientReplacementRequest.hpp"
#include "../DataOutput.hpp"
#include "../DataInput.hpp"
#include "GemfireTypeIdsImpl.hpp"
using namespace gemfire;
void ClientReplacementRequest::toData( DataOutput& output ) const
{
    ClientConnectionRequest::toData(output);
    this->m_serverLocation.toData(output);
}
Serializable* ClientReplacementRequest::fromData( DataInput& input )
{
  return NULL;  //not needed as of now and my guess is  it will never be needed.
}
int8_t ClientReplacementRequest::typeId( ) const
{
  return (int8_t)(GemfireTypeIdsImpl::ClientReplacementRequest);
}
