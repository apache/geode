#pragma once

#include <functional>
#include <vcclr.h>
#include <AppDomainContext.hpp>

namespace GemStone {
namespace GemFire {
namespace Cache {
namespace Generic {

using namespace System;

/**
 * Internal managed wrapper for invoking function through to attach
 * current thread to AppDomain associatd with this instance.
 */
ref class AppDomainContextWrapper {
public:
  delegate void Delegate(std::function<void()>);
  typedef void(__stdcall *Function)(std::function<void()>);

  void run(std::function<void()> func) {
    func();
  }
};

/**
 * Captures the current thread's AppDomain for later use in associating a native
 * thread with this instanaces AppDomain.
 */
class AppDomainContext : public gemfire::AppDomainContext {
public:
  AppDomainContext() {
    functionDelegate = gcnew AppDomainContextWrapper::Delegate(gcnew AppDomainContextWrapper(),
                                                               &AppDomainContextWrapper::run);
    functionPointer = (AppDomainContextWrapper::Function)
      System::Runtime::InteropServices::Marshal::GetFunctionPointerForDelegate(functionDelegate).ToPointer();
  }
  
  void run(runnable func) {
    functionPointer(func);
  }

private:
  gcroot<AppDomainContextWrapper::Delegate^> functionDelegate;
  AppDomainContextWrapper::Function functionPointer;
};

gemfire::AppDomainContext* createAppDomainContext();

}
}
}
}