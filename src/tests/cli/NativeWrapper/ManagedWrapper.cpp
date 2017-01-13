/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "ManagedWrapper.hpp"

using namespace GemStone::GemFire::Cache;
using namespace GemStone::GemFire::Cache::Tests;

ManagedWrapper::ManagedWrapper(int len) : m_nativePtr(new NativeType())
{
  m_str = gcnew System::String('A', len);
}

ManagedWrapper::~ManagedWrapper()
{
  GemStone::GemFire::Cache::Generic::Log::Info("Invoked Dispose of ManagedWrapper");
  InternalCleanup();
}

ManagedWrapper::!ManagedWrapper()
{
  GemStone::GemFire::Cache::Generic::Log::Info("Invoked Finalizer of ManagedWrapper");
  InternalCleanup();
}

void ManagedWrapper::InternalCleanup()
{
  delete m_nativePtr;
  m_nativePtr = nullptr;
}

bool ManagedWrapper::UnsafeDoOp(int size, int numOps)
{
  GemStone::GemFire::Cache::Generic::Log::Info("Managed string length: {0}", m_str->Length);
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
