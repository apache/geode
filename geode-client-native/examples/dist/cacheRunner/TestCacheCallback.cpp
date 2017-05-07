/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
 * @file TestCacheCallback.cpp
 * @since   1.0
 * @version 1.0
 * @see
*/
#include <gfcpp/CacheableBuiltins.hpp>
#include <gfcpp/GemfireTypeIds.hpp>
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
     if (id == GemfireTypeIds::CacheableBytes) {
       cBytePtr = staticCast<CacheableBytesPtr>(valueBytes);
       const uint8_t* bytType = cBytePtr->value();
       const uint32_t len = cBytePtr->length();
       char buff[1024];
       sprintf(buff,"%s",(char*)bytType);
       buff[len] = '\0';
       std::string byteType(buff);
       sValue = byteType;
     }
     else if (id == GemfireTypeIds::CacheableInt32) {
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
