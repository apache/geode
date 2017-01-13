#include "AppDomainContext.hpp"

namespace gemfire {

AppDomainContext* nullAppDomainContext() { return nullptr; }

AppDomainContext::factory createAppDomainContext = &nullAppDomainContext;
}