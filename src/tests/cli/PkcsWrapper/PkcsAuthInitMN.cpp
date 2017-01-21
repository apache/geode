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
using namespace Apache::Geode::Client::Tests::NewAPI;
using namespace Apache::Geode::Client;

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
Apache::Geode::Client::Properties<String^, Object^>^
PkcsAuthInit::GetCredentials(
  Apache::Geode::Client::Properties<String^, String^> ^props, System::String ^server)
{
  Apache::Geode::Client::ManagedString mg_server( server );
  apache::geode::client::PropertiesPtr propsPtr = NULLPTR;
  if (props != nullptr) {
    propsPtr = (apache::geode::client::Properties*)props->NativeIntPtr;
  }
  apache::geode::client::PKCSAuthInitInternal* nativeptr = new apache::geode::client::PKCSAuthInitInternal(true); 
  apache::geode::client::PropertiesPtr& newPropsPtr = nativeptr->getCredentials(propsPtr, mg_server.CharPtr);     
  return Apache::Geode::Client::Properties<String^, Object^>::
    CreateFromVoidPtr<String^, Object^>(newPropsPtr.ptr());
}
