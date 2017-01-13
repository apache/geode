#include "ServerLocationRequest.hpp"
#include "GemfireTypeIdsImpl.hpp"
using namespace gemfire;
/*int8_t ServerLocationRequest::typeId( ) const
{
  return (int8_t)GemfireTypeIdsImpl::FixedIDByte;
}*/
int8_t ServerLocationRequest::DSFID() const {
  return static_cast<int8_t>(GemfireTypeIdsImpl::FixedIDByte);
}

int32_t ServerLocationRequest::classId() const { return 0; }
