#ifndef __gemfire_AppDomainContext_h__
#define __gemfire_AppDomainContext_h__

#include <functional>

namespace gemfire {

class AppDomainContext {
 public:
  typedef AppDomainContext* (*factory)();
  typedef std::function<void()> runnable;

  virtual void run(runnable func) = 0;
};

extern AppDomainContext::factory createAppDomainContext;
}

#endif // __gemfire_AppDomainContext_h__

