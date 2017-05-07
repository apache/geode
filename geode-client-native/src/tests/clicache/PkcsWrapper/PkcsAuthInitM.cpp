/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "PkcsAuthInitM.hpp"
#include "Properties.hpp"
#include "impl/ManagedString.hpp"

using namespace GemStone::GemFire::Cache::Tests;
using namespace GemStone::GemFire::Cache;

PkcsAuthInit::PkcsAuthInit() 
{

}

PkcsAuthInit::~PkcsAuthInit() 
{

}

void PkcsAuthInit::Close() 
{
}

Properties^ PkcsAuthInit::GetCredentials(Properties ^props, System::String ^server)
{
  ManagedString mg_server( server );
  gemfire::PropertiesPtr propsPtr = NULLPTR;
  if (props != nullptr) {
    propsPtr = (gemfire::Properties*)props->NativeIntPtr;
  }
  gemfire::PKCSAuthInitInternal* nativeptr = new gemfire::PKCSAuthInitInternal(); 
  gemfire::PropertiesPtr& newPropsPtr = nativeptr->getCredentials(propsPtr, mg_server.CharPtr);     
  return Properties::CreateFromVoidPtr(newPropsPtr.ptr());
}
