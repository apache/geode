/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*=========================================================================
*/

/**
* @file    UserObjectTest.cpp
* @since   1.0
* @version 1.0
* @see
*
*/

// ----------------------------------------------------------------------------

#include <GemfireCppCache.hpp>
#include <SystemProperties.hpp>
#include <gfcpp_globals.hpp>

#include "UserObjectTest.hpp"
#include "fwklib/PerfFwk.hpp"
#include "fwklib/GsRandom.hpp"
#include "fwklib/FwkStrCvt.hpp"
#include "fwklib/PaceMeter.hpp"
#include "fwklib/FwkExport.hpp"
#include "ExampleObject.hpp"
#include "User.hpp"

#include "ace/OS.h"
#include <ace/Time_Value.h>
#include <stdlib.h>

#ifdef WIN32
  #ifdef llabs
    #undef llabs
  #endif
  #define llabs(x) ( ((x) < 0) ? ((x) * -1LL) : (x) )
#endif


using namespace gemfire;
using namespace gemfire::testframework;

static UserObjectTest * g_test = NULL;


// ----------------------------------------------------------------------------

TESTTASK initialize( const char * initArgs ) {
  int32_t result = FWK_SUCCESS;
  if ( g_test == NULL ) {
    FWKINFO( "Initializing UserObjectTest library." );
    try {
      g_test = new UserObjectTest( initArgs );
    } catch( const FwkException &ex ) {
      FWKSEVERE( "initialize: caught exception: " << ex.getMessage() );
      result = FWK_SEVERE;
    }
  }
  return result;
}

TESTTASK finalize() {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "Finalizing UserObjectTest library." );
  if ( g_test != NULL ) {
    g_test->cacheFinalize();
    delete g_test;
    g_test = NULL;
  }
  return result;
}

// ----------------------------------------------------------------------------

void UserObjectTest::checkTest( const char * taskId ) {
  SpinLockGuard guard( m_lck );
  setTask( taskId );
  if (m_cache == NULLPTR) {
    PropertiesPtr pp = Properties::create();

    cacheInitialize( pp );

    // UserObjectTest specific initialization
    const std::string bbName = getStringValue( "BBName" );
    if ( !bbName.empty() ) {
      m_bb = bbName;
    }
    // after initialized the cache, we should register the user objects
    Serializable::registerType( ExampleObject::createInstance);
    Serializable::registerType( User::createInstance);
  }
}

// ----------------------------------------------------------------------------

TESTTASK doVerifyAll( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doVerify called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    result = g_test->mverify();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doVerify caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------

TESTTASK doPuts( const char * taskId ) {
  int32_t result = FWK_SUCCESS;
  FWKINFO( "doPuts called for task: " << taskId );

  try {
    g_test->checkTest( taskId );
    result = g_test->mput();
  } catch (const FwkException &ex ) {
    result = FWK_SEVERE;
    FWKSEVERE( "doPuts caught exception: " << ex.getMessage() );
  }
  return result;
}

// ----------------------------------------------------------------------------

RegionPtr UserObjectTest::getRegion( const char * rname ) {
  RegionPtr regionPtr;
  if (m_cache == NULLPTR) {
    return regionPtr;
  }
  std::string name;
  if ( rname != NULL ) {
    name = rname;
  }
  else {
    name = getStringValue( "regionName" );
    if ( name.empty() ) {
      VectorOfRegion regVec;
      m_cache->rootRegions( regVec );
      int32_t siz = regVec.size();
      if ( siz > 0 ) {
        regionPtr = regVec.at( 0 );
      }
      return regionPtr;
    }
  }
  regionPtr = m_cache->getRegion( name.c_str() );
  return regionPtr;
}

// ----------------------------------------------------------------------------

int32_t UserObjectTest::mput() {
  FWKINFO( "mput called." );

  int32_t fwkResult = FWK_SUCCESS;
  int32_t result = g_test->initFromParameters();
  if (result != FWK_SUCCESS) {
    return result;
  }

  int32_t low = m_KeyIndexBegin;
  low = (low > 0) ? low : 0;
  int32_t high = low + m_EntryCount;

  RegionPtr regionPtr = getRegion();
  if (regionPtr == NULLPTR) {
    fwkResult = FWK_SEVERE;
    FWKSEVERE( "UserObjectTest::mput(): No region to perform operations on." );
    return fwkResult;
  }

  std::string objValueStr = std::string(getStringValue("objValue"));
  int32_t m_objValue = getIntValue( "objValue" );
  m_objValue = ( m_objValue > 0 ) ? m_objValue: 0;

  int32_t siz = static_cast<int32_t>(classList.size());

  try {
    for (int32_t j=0; j<siz; j++) {
      if (strcmp(classList[j].c_str(), "ExampleObject")==0) {
        ExampleObjectPtr newObj(new ExampleObject(objValueStr));
        for (int i=low; i<high; i++) {
          regionPtr->put(m_KeysA[j*m_EntryCount+i-low], newObj);
        }
      } else if (strcmp(classList[j].c_str(), "User")==0) {
        UserPtr newUser(new User(objValueStr, m_objValue));
        for (int i=low; i<high; i++) {
          regionPtr->put(m_KeysA[j*m_EntryCount+i-low], newUser);
        }
      } else {
        FWKSEVERE( "The user object class is not supported: " << classList[j].c_str());
        fwkResult = FWK_SEVERE;
        break;
      }
    } // for
  } catch ( TimeoutException &e ) {
    fwkResult = FWK_SEVERE;
    FWKSEVERE( "Caught unexpected timeout exception during put "
      << " operation: " << e.getMessage() << " continuing with test." );
  } catch ( EntryExistsException &ignore ) { ignore.getMessage();
  } catch ( EntryNotFoundException &ignore ) { ignore.getMessage();
  } catch ( EntryDestroyedException &ignore ) { ignore.getMessage();
  } catch ( Exception &e ) {
    fwkResult = FWK_SEVERE;
    FWKSEVERE( "Caught unexpected exception during entry put "
      << " operation: " << e.getMessage() << " exiting task." );
  }

  FWKINFO("mput finished.");
  return fwkResult;
}

// ----------------------------------------------------------------------------

int32_t UserObjectTest::mverify() {
  FWKINFO( "mverify called." );

  int32_t fwkResult = FWK_SUCCESS;
  int32_t result = g_test->initFromParameters();
  if (result != FWK_SUCCESS) {
    return result;
  }

  int32_t low = m_KeyIndexBegin;
  low = (low > 0) ? low : 0;
  int32_t high = low + m_EntryCount;

  RegionPtr regionPtr = getRegion();
  if (regionPtr == NULLPTR) {
    fwkResult = FWK_SEVERE;
    FWKSEVERE( "UserObjectTest::mput(): No region to perform operations on." );
    return fwkResult;
  }

  std::string objValueStr = getStringValue("objValue");
  int32_t m_objValue = getIntValue( "objValue" );
  m_objValue = ( m_objValue > 0 ) ? m_objValue: 0;

  ExampleObjectPtr newObj(new ExampleObject(objValueStr));
  UserPtr newUser(new User(objValueStr, m_objValue));
  ExampleObject* cObjPtr = NULL;
  User* cUserPtr = NULL;
  int32_t siz = static_cast<int32_t>(classList.size());

  try {
    for (int32_t j=0; j<siz; j++) {
      for (int32_t i=low; i<high; i++) {
        CacheablePtr valuePtr = regionPtr->get(m_KeysA[j*m_EntryCount+i-low]);
        if (valuePtr != NULLPTR) {
          if ((cObjPtr = dynamic_cast<ExampleObject *>(valuePtr.ptr())) != NULL) {
            if (strcmp(cObjPtr->toString()->asChar(), newObj->toString()->asChar())!=0) {
              fwkResult = FWK_SEVERE;
              FWKSEVERE( "Verify failed for class: " << classList[j].c_str() << ", at count: " << i << ", expect:" << std::string(newObj->toString()->asChar()) << ", actual:" << std::string(cObjPtr->toString()->asChar()));
              break;
            }
          } else if ((cUserPtr = dynamic_cast<User *>(valuePtr.ptr())) != NULL) {
            if (strcmp(cUserPtr->toString()->asChar(), newUser->toString()->asChar())!=0) {
              fwkResult = FWK_SEVERE;
              FWKSEVERE( "Verify failed for class: " << classList[j].c_str() << ", at count: " << i << ", expect:" << std::string(newUser->toString()->asChar()) << ", actual:" << std::string(cUserPtr->toString()->asChar()));
              break;
            }
          } else { // the value is invalid
            fwkResult = FWK_SEVERE;
            FWKSEVERE( "The value is invalid. ");
            break;
          }
        } else {
          FWKSEVERE( "Verify failed for class: " << classList[j].c_str() << ", at count: " << i << ". The value got from region is NULL." );
        }
      }
    }
  } catch ( TimeoutException &e ) {
    fwkResult = FWK_SEVERE;
    FWKSEVERE( "Caught unexpected timeout exception during get "
      << " operation: " << e.getMessage() << " continuing with test." );
  } catch ( EntryExistsException &ignore ) { ignore.getMessage();
  } catch ( EntryNotFoundException &ignore ) { ignore.getMessage();
  } catch ( EntryDestroyedException &ignore ) { ignore.getMessage();
  } catch ( Exception &e ) {
    fwkResult = FWK_SEVERE;
    FWKSEVERE( "Caught unexpected exception during entry get "
      << " operation: " << e.getMessage() << " exiting task." );
  }

  return fwkResult;
}

// ----------------------------------------------------------------------------

void UserObjectTest::clearup() {
  int32_t siz = static_cast<int32_t>(classList.size());
  if ( m_KeysA != NULL && !classList.empty()) {
    FWKINFO( "clearup called." );
    for (int32_t j=0; j<siz; j++) {
      // std::string className = classList[j];
      for ( int32_t i = 0; i < m_EntryCount; i++ ) {
        m_KeysA[j*m_EntryCount+i] = NULLPTR;
      }
    }
    delete [] m_KeysA;
    m_KeysA = NULL;
    m_EntryCount = 0;
    classList.clear();
  }
}

// ----------------------------------------------------------------------------

int32_t UserObjectTest::initFromParameters() {
  FWKINFO( "initFromParameters called." );
  std::string className;

  clearup();

  while (1) {
    className = getStringValue( "className" );
    if ( className.empty() ) {
	break;
    } else {
      classList.push_back(std::string(className));
    }
  }

  std::string keyType = getStringValue( "keyType" ); // int is only value to use
  if (keyType.empty()) {
    keyType = "str";
  }
  if (strcmp(keyType.c_str(), "int")==0) {
    FWKSEVERE( "keyType can not be int for userobject container");
    return FWK_SEVERE;
  }

  m_EntryCount = getIntValue( "entryCount" );
  m_EntryCount = ( m_EntryCount < 1 ) ? 1 : m_EntryCount;

  int32_t low = getIntValue( "keyIndexBegin" );
  low = (low > 0) ? low : 0;
  int32_t high = low + m_EntryCount;
  FWKINFO("entryCount: " << m_EntryCount << " low: " << low);
  m_KeyIndexBegin = low;


  int32_t total_key_num = m_EntryCount* static_cast<uint32_t>(classList.size());
  if (total_key_num == 0) {
    FWKSEVERE( "UserObjectTest::initFromParameters(): entryCount or className are not specified." );
    return FWK_SEVERE;
  }
  m_KeysA = new CacheableKeyPtr[total_key_num];

  char buf[128];
  int32_t siz = static_cast<int32_t>(classList.size());
  if (strcmp(keyType.c_str(),"str")==0) {
    for (int32_t j=0; j<siz; j++) {
      for (int32_t i = low; i < high; i++) {
        sprintf( buf, "%s%10d", classList[j].c_str(), i);
        m_KeysA[j*m_EntryCount + i - low] = CacheableKey::create( buf );
      }
    }
  } else if (strcmp(keyType.c_str(),"obj")==0) {
    for (int32_t j=0; j<siz; j++) {
      for (int32_t i = low; i < high; i++) {
        sprintf( buf, "%s%10d", classList[j].c_str(), i);
        m_KeysA[j*m_EntryCount + i - low] = new ExampleObject(i);
      }
    }
  }
  FWKINFO("m_EntryCount: " << m_EntryCount << " low: " << low << " high: " << high << " keyType:" << keyType << " Total keyNum: " << total_key_num);
  return FWK_SUCCESS;
}

