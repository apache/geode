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
