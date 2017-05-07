/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
 #include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include "ThinClientSecurity.hpp"

/* Test Coverage DH Algo
BF1 - Blowfish:128 , BF2 - Blowfish:448
AES1- AES:128,  AES2- AES:192, AES3- AES:256
DES- DESede:192

ATTENTION:  Blowfish:448, AES:192 and AES:256 needs Unlimited security strength policy. For this
1- Downloaded jce_policy-6.zip from http://java.sun.com/javase/downloads/index.jsp.
2- Unzip and replace 2 jar files in $gfe.dir/jre/lib/security folder.
   Above mentioned Algo are commented as we can't ship product folder with above mentioned Jar files.
 To test this test fully, please make above changes and uncomment related Algo portion in this test.
*/

#define BF1  "Blowfish:128"
#define BF2  "Blowfish:448"
#define AES1  "AES:128"
#define AES2  "AES:192"
#define AES3  "AES:256"
#define DES  "DESede"



#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define CLIENT3 s2p1
#define LOCATORSERVER s2p2

#define CORRECT_CREDENTIALS 'C'
#define INCORRECT_CREDENTIALS 'I'

using namespace gemfire;
using namespace test;

bool isLocalServer = false;
//bool isLocator = false;
const char * endPoints = CacheHelper::getTcrEndpoints( isLocalServer, 3 );
const char * locHostPort = CacheHelper::getLocatorHostPort( isLocator, 1 );
const char *regionNamesAuth[] = { "DistRegionAck"  , "DistRegionNoAck" } ;
CredentialGeneratorPtr credentialGeneratorHandler;

std::string getXmlPath() {
  char xmlPath[1000] = { '\0' } ;
  const char *path = ACE_OS::getenv( "TESTSRC" );
  ASSERT( path != NULL, "Environment variable TESTSRC for test source directory is not set." );
  strncpy(xmlPath,path,strlen(path)-strlen("cppcache"));
  strcat(xmlPath,"xml/Security/");
  return std::string(xmlPath);
}

void initCredentialGenerator( )
{
  static int loopNum = 1;

  switch(loopNum) {
    case 1:
      {
        credentialGeneratorHandler = CredentialGenerator::create("DUMMY");
        LOG("Creating Dummy Credential Generator");
        break ;
      }
    case 2:
      {
        credentialGeneratorHandler = CredentialGenerator::create("LDAP");
        LOG("Creating LDAP Credential Generator");
        break;
      }
    default:
    case 3:
      {
        credentialGeneratorHandler = CredentialGenerator::create("PKCS");
        LOG("Creating PKCS Credential Generator");
        break;
      }
  }

  if( credentialGeneratorHandler == NULLPTR) {
    FAIL("credentialGeneratorHandler is NULL");
  }

  loopNum++;
  if(loopNum > 2)
    loopNum = 1;
}

static PropertiesPtr userCreds;

void initClientAuth( char credentialsType, const char* dhAlgo)
{
  printf("Initializing Client with %s credential and %s DH Algo\n",
          credentialsType == CORRECT_CREDENTIALS ? "Valid" : "Invalid",dhAlgo);

  PropertiesPtr config = Properties::create();
  userCreds = Properties::create();

  config->insert("security-client-dhalgo", dhAlgo);
  std::string testsrc = ACE_OS::getenv( "TESTSRC" );
  testsrc += "/keystore/gemfire.pem";
  printf("KeyStore Path is: %s", testsrc.c_str());
  config->insert("security-client-kspath", testsrc.c_str());

  if(credentialGeneratorHandler == NULLPTR ) {
    FAIL("credentialGeneratorHandler is NULL");
  }
  bool insertAuthInit = true;
  switch(credentialsType) {
    case CORRECT_CREDENTIALS :
      credentialGeneratorHandler->getValidCredentials(userCreds);
      userCreds->insert("security-password" , userCreds->find("security-username")->asChar() );
     printf("Username is %s and Password is %s ",userCreds->find("security-username")->asChar(),userCreds->find("security-password")->asChar());
      break;
    case INCORRECT_CREDENTIALS :
      credentialGeneratorHandler->getInvalidCredentials(userCreds);
      userCreds->insert("security-password" , "junk");
     printf("Username is %s and Password is %s ",userCreds->find("security-username")->asChar(),userCreds->find("security-password")->asChar());
      break;
    default  :
      insertAuthInit = false;
      break;
  }
  if ( insertAuthInit ) {
  //  credentialGeneratorHandler->getAuthInit(config);
  }

  try {
    initClient(true ,config);
  } catch (...) {
      throw ;
  }
}

void InitIncorrectClients(const char* dhAlgo )
{
  try {
    initClientAuth(INCORRECT_CREDENTIALS,dhAlgo);
  } catch ( const gemfire::Exception& other ) {
    other.printStackTrace();
    LOG( other.getMessage() );
  }

  try {
    createRegionForSecurity( regionNamesAuth[0], USE_ACK, endPoints, false, NULLPTR, false, -1, true, 0);
    PoolPtr pool = getPool(regionNamesAuth[0]);
    LOG("hitesh 6");
    if (pool != NULLPTR) 
    {
      LOG("hitesh 7");
      RegionServicePtr virtualCache = getVirtualCache(userCreds, pool);
      LOG("hitesh 8");
      virtualCache->getRegion(regionNamesAuth[0])->put(keys[0], vals[0]);
      LOG("Operation allowed, something is wrong.");
    }
    FAIL( "Should have thrown AuthenticationFailedException." );
  } catch ( const gemfire::AuthenticationFailedException& other ) {
    other.printStackTrace();
    LOG( other.getMessage() );
  } catch ( const gemfire::Exception& other ) {
    other.printStackTrace();
    LOG( other.getMessage() );
    FAIL( "Only AuthenticationFailedException is expected");
  }
  LOG("InitIncorrectClients Completed");
}

void InitCorrectClients(const char* dhAlgo )
{
  try {
    initClientAuth(CORRECT_CREDENTIALS, dhAlgo);
  } catch ( const gemfire::Exception& other ) {
    other.printStackTrace();
    LOG( other.getMessage() );
  }
  try {
    //createRegionForSecurity( regionNamesAuth[0], USE_ACK, endPoints, true);
    createRegionForSecurity( regionNamesAuth[0], USE_ACK, endPoints, false, NULLPTR, false, -1, true, 0);
    PoolPtr pool = getPool(regionNamesAuth[0]);
    LOG("hitesh 6");
     
    LOG("hitesh 7");
    RegionServicePtr virtualCache = getVirtualCache(userCreds, pool);
    LOG("hitesh 8");
    RegionPtr regionPtr = virtualCache->getRegion(regionNamesAuth[0]);
    
    for(int i =0 ; i< 100; i++)
      regionPtr->put(keys[0], vals[0]);
  } catch ( const gemfire::Exception& other ) {
      other.printStackTrace();
      FAIL( other.getMessage() );
  }
     LOG("Handshake  and  Authentication successfully completed");
}

void DoNetSearch()
{
  try {
    //createRegionForSecurity( regionNamesAuth[1], USE_ACK, endPoints, true);
    createRegionForSecurity( regionNamesAuth[1], USE_ACK, endPoints, false, NULLPTR, false, -1, true, 0);
    PoolPtr pool = getPool(regionNamesAuth[1]);
    LOG("hitesh 6");
    
    LOG("hitesh 7");
    RegionServicePtr virtualCache = getVirtualCache(userCreds, pool);
    LOG("hitesh 8");
    RegionPtr regionPtr = virtualCache->getRegion(regionNamesAuth[1]);
    
    CacheableKeyPtr keyPtr = CacheableKey::create( keys[0] );
    CacheableStringPtr checkPtr = dynCast<CacheableStringPtr>( regionPtr->get( keyPtr) );
    if(checkPtr != NULLPTR && !strcmp(vals[0], checkPtr->asChar())){
      LOG("checkPtr is not null");
      char buf[1024];
      sprintf( buf, "In net search, get returned %s for key %s", checkPtr->asChar(), keys[0] );
      LOG( buf );
      }else{
      LOG("checkPtr is NULL");
      }
  } catch ( const gemfire::Exception& other ) {
      other.printStackTrace();
      FAIL( other.getMessage() );
  }
  LOG("Handshake  and  Authentication successfully completed after FailOver");
}

void initSecurityServer(int instance)
{
  std::string cmdServerAuthenticator ;
  if(credentialGeneratorHandler == NULLPTR) {
    FAIL("credentialGeneratorHandler is NULL");
  }

  try {
    if ( isLocalServer ) {
      cmdServerAuthenticator = credentialGeneratorHandler->getServerCmdParams("authenticator",
                                                                              getXmlPath());

      std::string testsrc = ACE_OS::getenv( "TESTSRC" );
      if(instance == 1) {
        testsrc += "/keystore/gemfire1.keystore";
        cmdServerAuthenticator += " security-server-kspath=";
        cmdServerAuthenticator += testsrc;
        cmdServerAuthenticator += " security-server-ksalias=gemfire1 security-server-kspasswd=gemfire";
      } else if(instance == 2) {
        testsrc += "/keystore/gemfire2.keystore";
        cmdServerAuthenticator += " security-server-kspath=";
        cmdServerAuthenticator += testsrc;
        cmdServerAuthenticator += " security-server-ksalias=gemfire2 security-server-kspasswd=gemfire";
      }

      printf("Input to server cmd is -->  %s\n",cmdServerAuthenticator.c_str());
      CacheHelper::initServer( instance,NULL,locHostPort , (char*)cmdServerAuthenticator.c_str());
    }
  } catch (...) {
     printf("this is some exception");
  }
}

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateLocator)
{
  if ( isLocator )
    CacheHelper::initLocator( 1 );
    LOG("Locator1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateServer1)
{
  initCredentialGenerator();
  initSecurityServer(1);
  LOG("Server1 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER, CreateServer2)
{
  initSecurityServer(2);
  LOG("Server2 started");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, C1UpDownIncorrectBF1)
{
  initCredentialGenerator();
  InitIncorrectClients(BF1);
  LOG("Client created");
  cleanProc();
  LOG("Client closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, C2UpDownIncorrectAES1)
{
  initCredentialGenerator();
  InitIncorrectClients(AES1);
  LOG("Client created");
  cleanProc();
  LOG("Client closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, C3UpDownIncorrectDES)
{
  initCredentialGenerator();
  InitIncorrectClients(DES);
  LOG("Client created");
  cleanProc();
  LOG("Client closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, C1UpCorrectBF1)
{
  InitCorrectClients(BF1);
  LOG("Client created");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, C2UpCorrectAES1)
{
  InitCorrectClients(AES1);
  LOG("Client created");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, C3UpCorrectDES)
{
  InitCorrectClients(DES);
  LOG("Client created");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, C1UpDownIncorrectBF2)
{
  InitIncorrectClients(BF2);
  LOG("Client created");
  cleanProc();
  LOG("Client closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, C2UpDownIncorrectAES2)
{
  InitIncorrectClients(AES2);
  LOG("Client created");
  cleanProc();
  LOG("Client closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, C3UpDownIncorrectAES3)
{
  InitIncorrectClients(AES3);
  LOG("Client created");
  cleanProc();
  LOG("Client closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, C1UpDownCorrectBF2)
{
  InitCorrectClients(BF2);
  LOG("Client created");
  cleanProc();
  LOG("Client closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2, C2UpDownCorrectAES2)
{
  InitCorrectClients(AES2);
  LOG("Client created");
  cleanProc();
  LOG("Client closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3, C3UpDownCorrectAES3)
{
  InitCorrectClients(AES3);
  LOG("Client created");
  cleanProc();
  LOG("Client closed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,C1NetSearch)
{
  SLEEP(1000);
  DoNetSearch();
  LOG("StepFive Completed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,C2NetSearch)
{
  DoNetSearch();
  LOG("StepFive Completed");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3,C3NetSearch)
{
  DoNetSearch();
  LOG("StepFive Completed");
}
END_TASK_DEFINITION


DUNIT_TASK_DEFINITION(CLIENT1,CloseCache1)
{
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,CloseCache2)
{
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3,CloseCache3)
{
  cleanProc();
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER,CloseServer1)
{
  if ( isLocalServer ) {
    CacheHelper::closeServer( 1 );
    LOG("SERVER1 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER,CloseServer2)
{
  if ( isLocalServer ) {
    CacheHelper::closeServer( 2 );
    LOG("SERVER2 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER,CloseLocator)
{
  if ( isLocator ) {
    CacheHelper::closeLocator( 1 );
    LOG("Locator1 stopped");
  }
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1,SetTestCase1C1)
{
  setGlobals(1);
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1,SetTestCase2C1)
{
  setGlobals(2);
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT1,SetTestCase3C1)
{
  setGlobals(3);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT2,SetTestCase1C2)
{
  setGlobals(1);
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2,SetTestCase2C2)
{
  setGlobals(2);
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT2,SetTestCase3C2)
{
  setGlobals(3);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT3,SetTestCase1C3)
{
  setGlobals(1);
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT3,SetTestCase2C3)
{
  setGlobals(2);
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(CLIENT3,SetTestCase3C3)
{
  setGlobals(3);
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(LOCATORSERVER,SetTestCase1LS)
{
  setGlobals(1);
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(LOCATORSERVER,SetTestCase2LS)
{
  setGlobals(2);
}
END_TASK_DEFINITION
DUNIT_TASK_DEFINITION(LOCATORSERVER,SetTestCase3LS)
{
  setGlobals(3);
}
END_TASK_DEFINITION

void callSetGlobals(int testCase)
{
  switch (testCase){
    case 1: {      // normal case: pool == false, locators == false
      CALL_TASK(SetTestCase1C1);
      CALL_TASK(SetTestCase1C2);
      CALL_TASK(SetTestCase1C3);
      CALL_TASK(SetTestCase1LS);
      break;
    }
    case 2: {      // pool-with-endpoints case: pool == true, locators == false
      CALL_TASK(SetTestCase2C1);
      CALL_TASK(SetTestCase2C2);
      CALL_TASK(SetTestCase2C3);
      CALL_TASK(SetTestCase2LS);
      break;
    }
    case 3: {      // pool-with-locator case: pool == true, locators == true
      CALL_TASK(SetTestCase3C1);
      CALL_TASK(SetTestCase3C2);
      CALL_TASK(SetTestCase3C3);
      CALL_TASK(SetTestCase3LS);
      break;
    }
  }
}

void doThinClientSecurityDH( int testCase)
{
  callSetGlobals(testCase);

  int i;
  for(i = 1 ; i <=2 ; i++)
  {
    CALL_TASK(CreateLocator);
    CALL_TASK(CreateServer1);
    CALL_TASK(C1UpDownIncorrectBF1);
    CALL_TASK(C2UpDownIncorrectAES1);
    CALL_TASK(C3UpDownIncorrectDES);
    CALL_TASK(C1UpCorrectBF1);
    CALL_TASK(C2UpCorrectAES1);
    CALL_TASK(C3UpCorrectDES);
    CALL_TASK(CreateServer2);
    CALL_TASK(CloseServer1);
    CALL_TASK(C1NetSearch);
    CALL_TASK(C2NetSearch);
    CALL_TASK(C3NetSearch);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseCache2);
    CALL_TASK(CloseCache3);

    // Commented for Unlimited Security strength policy : See comment at top of testThinClientSecurityDH.cpp
    // CALL_TASK(C1UpDownIncorrectBF2);
    // CALL_TASK(C2UpDownIncorrectAES2);
    // CALL_TASK(C3UpDownIncorrectAES3);
    // CALL_TASK(C1UpDownCorrectBF2);
    // CALL_TASK(C2UpDownCorrectAES2);
    // CALL_TASK(C3UpDownCorrectAES3);
    CALL_TASK(CloseServer2);
    CALL_TASK(CloseLocator);
  }
}

DUNIT_MAIN
{
 // doThinClientSecurityDH(1); // normal case: pool == false, locators == false

  doThinClientSecurityDH(2); // pool-with-endpoints case: pool == true, locators == false

 // doThinClientSecurityDH(3); // pool-with-locator case: pool == true, locators == true
}
END_MAIN
