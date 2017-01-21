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

#include "ManagedWrapper.hpp"

using namespace Apache::Geode::Client;
using namespace Apache::Geode::Client::Tests;

ManagedWrapper::ManagedWrapper(int len) : m_nativePtr(new NativeType())
{
  m_str = gcnew System::String('A', len);
}

ManagedWrapper::~ManagedWrapper()
{
  Apache::Geode::Client::Log::Info("Invoked Dispose of ManagedWrapper");
  InternalCleanup();
}

ManagedWrapper::!ManagedWrapper()
{
  Apache::Geode::Client::Log::Info("Invoked Finalizer of ManagedWrapper");
  InternalCleanup();
}

void ManagedWrapper::InternalCleanup()
{
  delete m_nativePtr;
  m_nativePtr = nullptr;
}

bool ManagedWrapper::UnsafeDoOp(int size, int numOps)
{
  Apache::Geode::Client::Log::Info("Managed string length: {0}", m_str->Length);
  return UnsafeNativePtr->doOp(size, numOps, (numOps / 5) + 1);
}

bool ManagedWrapper::SafeDoOp1(int size, int numOps)
{
  return SafeNativePtr1->doOp(size, numOps, (numOps / 5) + 1);
}

bool ManagedWrapper::SafeDoOp2(int size, int numOps)
{
  bool res = UnsafeNativePtr->doOp(size, numOps, (numOps / 5) + 1);
  GC::KeepAlive(this);
  return res;
}

bool ManagedWrapper::SafeDoOp3(int size, int numOps)
{
  return SafeNativePtr2->doOp(size, numOps, (numOps / 5) + 1);
}
