#include "AppDomainContext.hpp"

namespace GemStone {
namespace GemFire {
namespace Cache {
namespace Generic {
gemfire::AppDomainContext* createAppDomainContext() {
  return new AppDomainContext();
}
}
}
}
}
