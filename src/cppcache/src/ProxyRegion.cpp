#include "ProxyRegion.hpp"
#include "ThinClientRegion.hpp"
#include "UserAttributes.hpp"

void ProxyRegion::unSupportedOperation(const char* operationName) const {
  char msg[256] = {'\0'};
  ACE_OS::snprintf(
      msg, 256,
      "%s operation is not supported when Region instance is logical.",
      operationName);
  throw UnsupportedOperationException(msg);
}
