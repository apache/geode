
#include <gfcpp/gfcpp_globals.hpp>

#include <gfcpp/CacheableKey.hpp>

#include <ace/OS.h>
#include <typeinfo>

namespace gemfire {

int32_t CacheableKey::logString(char* buffer, int32_t maxLength) const {
  return ACE_OS::snprintf(buffer, maxLength, "%s( @0x%08lX )",
                          typeid(*this).name(), (unsigned long)this);
}
}
