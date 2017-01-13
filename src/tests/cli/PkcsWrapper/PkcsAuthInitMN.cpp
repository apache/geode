/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "PkcsAuthInitMN.hpp"
#include <gfcpp/Properties.hpp>
#include "impl/ManagedString.hpp"

using namespace System;
using namespace GemStone::GemFire::Cache::Tests::NewAPI;
using namespace GemStone::GemFire::Cache::Generic;

PkcsAuthInit::PkcsAuthInit() 
{

}

PkcsAuthInit::~PkcsAuthInit() 
{

}

void PkcsAuthInit::Close() 
{
}

//generic <class TPropKey, class TPropValue>
GemStone::GemFire::Cache::Generic::Properties<String^, Object^>^
PkcsAuthInit::GetCredentials(
  GemStone::GemFire::Cache::Generic::Properties<String^, String^> ^props, System::String ^server)
{
  GemStone::GemFire::Cache::Generic::ManagedString mg_server( server );
  gemfire::PropertiesPtr propsPtr = NULLPTR;
  if (props != nullptr) {
    propsPtr = (gemfire::Properties*)props->NativeIntPtr;
  }
  gemfire::PKCSAuthInitInternal* nativeptr = new gemfire::PKCSAuthInitInternal(true); 
  gemfire::PropertiesPtr& newPropsPtr = nativeptr->getCredentials(propsPtr, mg_server.CharPtr);     
  return GemStone::GemFire::Cache::Generic::Properties<String^, Object^>::
    CreateFromVoidPtr<String^, Object^>(newPropsPtr.ptr());
}
