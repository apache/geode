/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
class AppDomainContext : public apache::geode::client::AppDomainContext {
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

apache::geode::client::AppDomainContext* createAppDomainContext();

}
}
}
}
