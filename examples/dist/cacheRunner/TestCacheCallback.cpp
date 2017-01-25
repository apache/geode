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

/**
 * @file TestCacheCallback.cpp
 * @since   1.0
 * @version 1.0
 * @see
*/
#include <gfcpp/CacheableBuiltins.hpp>
#include <gfcpp/GeodeTypeIds.hpp>
#include "TestCacheCallback.hpp"
#include "ExampleObject.hpp"
#include "User.hpp"

// ----------------------------------------------------------------------------

TestCacheCallback::TestCacheCallback(void)
: m_bInvoked(false)
{
}

// ----------------------------------------------------------------------------

TestCacheCallback::~TestCacheCallback(void)
{
}

// ----------------------------------------------------------------------------
/**
  * CacheCallback virtual method
  */
// ----------------------------------------------------------------------------

void TestCacheCallback::close( const RegionPtr& region )
{
  m_bInvoked = true;
}

// ----------------------------------------------------------------------------
/**
  * Returns whether or not one of this <code>CacheListener</code>
  * methods was invoked.  Before returning, the <code>invoked</code>
  * flag is cleared.
  */
// ----------------------------------------------------------------------------

bool TestCacheCallback::wasInvoked( )
{
  bool bInvoked = m_bInvoked;
  m_bInvoked = false;
  return bInvoked;
}

// ----------------------------------------------------------------------------
/**
  * This method will do nothing.  Note that it will not throw an
  * exception.
  */
// ----------------------------------------------------------------------------
/*
int TestCacheCallback::close2( const RegionPtr& )
{
  return ErrorCodes::Success;
}
*/
// ----------------------------------------------------------------------------
/**
  * Returns a description of the given <code>CacheEvent</code>.
  */
// ----------------------------------------------------------------------------
std::string TestCacheCallback::printEntryValue(CacheablePtr& valueBytes)
{
   std::string sValue = "null";
   CacheableStringPtr cStrPtr;
   CacheableInt32Ptr cIntPtr;
   CacheableBytesPtr cBytePtr;
   ExampleObjectPtr cobjPtr;
   UserPtr cUserPtr;

   if (valueBytes != NULLPTR) {
     int32_t id = valueBytes->typeId();
     if (id == GeodeTypeIds::CacheableBytes) {
       cBytePtr = staticCast<CacheableBytesPtr>(valueBytes);
       const uint8_t* bytType = cBytePtr->value();
       const uint32_t len = cBytePtr->length();
       char buff[1024];
       sprintf(buff,"%s",(char*)bytType);
       buff[len] = '\0';
       std::string byteType(buff);
       sValue = byteType;
     }
     else if (id == GeodeTypeIds::CacheableInt32) {
       cIntPtr= staticCast<CacheableInt32Ptr>(valueBytes);
       int32_t intval = cIntPtr->value();
       int len = sizeof(intval);
       char buff[1024];
       sprintf(buff,"%d",intval);
       buff[len] = '\0';
       std::string intType(buff);
       sValue = intType;
     }
     else if (instanceOf<CacheableStringPtr>(valueBytes)) {
       cStrPtr = staticCast<CacheableStringPtr>(valueBytes);
       sValue = cStrPtr->toString();
     }
     else if (instanceOf<ExampleObjectPtr>(valueBytes)) {
       cobjPtr= staticCast<ExampleObjectPtr>(valueBytes);
       sValue = cobjPtr->toString()->asChar();
#if 0
       int ownId = cobjPtr->getOwner();
       int len = sizeof(ownId);
       char buff1[1024];
       sprintf(buff1,"%d",ownId);
       buff1[len] = '\0';
       std::string ownIdStr(buff1);
       sValue = ownIdStr;
#endif
     }
     else if (instanceOf<UserPtr>(valueBytes)) {
       cUserPtr = staticCast<UserPtr>(valueBytes);
       sValue = cUserPtr->toString()->asChar();
     }
     else {
       sValue = "Unknown object type";
     }
   }
   else {
     sValue = "No value in cache.";
   }
   return sValue;
}

std::string TestCacheCallback::printEvent( const EntryEvent& event )
{
  std::string sReturnValue;

  sReturnValue = "\nPrint Event: ";

  sReturnValue += getBoolValues(event.remoteOrigin());

  sReturnValue += "\nRegion is : ";
  RegionPtr rptr = event.getRegion();
  sReturnValue += (rptr == NULLPTR) ? "NULL" : rptr->getFullPath();

  CacheableStringPtr key = dynCast<CacheableStringPtr>( event.getKey() );
  sReturnValue += "\nKey is : ";
  if (key == NULLPTR) {
    sReturnValue += "NULL";
  }
  else {
    sReturnValue += key->asChar();
  }

  sReturnValue += "\nOld value is : ";
  CacheablePtr oldStringValue = event.getOldValue();
  // std::string testvalue = printEntryValue(oldStringValue);
  if (oldStringValue == NULLPTR) {
    sReturnValue += "NULL";
  }
  else{
    sReturnValue += printEntryValue(oldStringValue);
  }

  sReturnValue += "\nNew value is : ";
  if (event.getNewValue() == NULLPTR) {
    sReturnValue += "NULL";
  }
  else {
    CacheablePtr newStringValue = event.getNewValue();
    sReturnValue += printEntryValue(newStringValue);
  }

  return sReturnValue;
}

// ----------------------------------------------------------------------------
/**
  * Returns a description of the given <code>CacheEvent</code>.
  */
// ----------------------------------------------------------------------------

std::string TestCacheCallback::printEvent( const RegionEvent& event )
{
  std::string sReturnValue;

  sReturnValue = "\nPrint Event: ";

  sReturnValue += getBoolValues( event.remoteOrigin() );

  sReturnValue += "\nRegion is : ";
  RegionPtr region = event.getRegion();
  sReturnValue += (region == NULLPTR) ? "NULL" : region->getFullPath();

  return sReturnValue;
}

// ----------------------------------------------------------------------------
/**
  * Returns a description of the given load event.
  */
// ----------------------------------------------------------------------------

std::string TestCacheCallback::printEvent(const RegionPtr& rp,
    const CacheableKeyPtr& key, const UserDataPtr& aCallbackArgument)
{
  std::string sReturnValue;
  CacheableStringPtr cStrPtr;
  char szLogString[50];

  sReturnValue += "\nRegion is : ";
  sReturnValue += (rp == NULLPTR) ? "NULL" : rp->getFullPath();

  sReturnValue += "\nKey is : ";
  if (key == NULLPTR) {
    sReturnValue += "NULL";
  }
  else if (instanceOf<CacheableStringPtr> (key)) {
    cStrPtr = staticCast<CacheableStringPtr> (key);
    sReturnValue += cStrPtr->toString();
  }
  else {
    key->logString(szLogString, sizeof(szLogString));
    sReturnValue += szLogString;
  }

  return sReturnValue;
}

// ----------------------------------------------------------------------------
/**
  * return string of boolValues
  */
// ----------------------------------------------------------------------------

std::string TestCacheCallback::getBoolValues( bool isRemote )
{
  std::string sReturnValue;

  if ( isRemote ) {
    sReturnValue += "\nCache Event Origin Remote";
  } else {
    sReturnValue += "\nCache Event Origin Local";
  }

  return sReturnValue;
}

// ----------------------------------------------------------------------------
