/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "fw_dunit.hpp"
#include <gfcpp/GemfireCppCache.hpp>
#include "BuiltinCacheableWrappers.hpp"

#include <ace/OS.h>
#include <ace/High_Res_Timer.h>

#include <cstring>

#define ROOT_NAME "testThinClientCacheablesLimits"
#define ROOT_SCOPE DISTRIBUTED_ACK

#include "CacheHelper.hpp"
#include "ThinClientHelper.hpp"

using namespace gemfire;
using namespace test;

#define CLIENT1 s1p1
#define CLIENT2 s1p2
#define SERVER1 s2p1

static bool isLocator = false;
static bool isLocalServer = true;
static int numberOfLocators = 1;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);
#include "LocatorHelper.hpp"

static int keyArr[] = {123,   125,   126,   127,   128,   129,   250,   251,
                       252,   253,   254,   255,   256,   257,   16380, 16381,
                       16382, 16383, 16384, 16385, 16386, 32765, 32766, 32767,
                       32768, 32769, 65533, 65534, 65535, 65536, 65537};
static char charArray[] = {
    'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n',
    'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', 'A', 'B',
    'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P',
    'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', '_', '-', '+'};
#define KEY_BYTE "key_byte"
#define KEY_EMPTY_BYTESARR "key_empty_bytes_array"
#define KEY_STRING "key_string"
#define FAIL_MESSAGE                                               \
  "Byte sent and received at boundAry condition are not same for " \
  "CacheableBytes or CacheableString for item %d"

void createRegion(const char* name, bool ackMode,
                  bool clientNotificationEnabled = false) {
  LOG("createRegion() entered.");
  fprintf(stdout, "Creating region --  %s  ackMode is %d\n", name, ackMode);
  fflush(stdout);
  RegionPtr regPtr = getHelper()->createRegion(name, ackMode, false, NULLPTR,
                                               clientNotificationEnabled);
  ASSERT(regPtr != NULLPTR, "Failed to create region.");
  LOG("Region created.");
}
uint8_t* createRandByteArray(int size) {
  uint8_t* ptr = new uint8_t[size];
  for (int i = 0; i < size; i++) {
    ptr[i] = (rand()) % 256;
  }
  return ptr;
}
char* createRandCharArray(int size) {
  char* ch;
  GF_ALLOC(ch, char, size + 1);
  ch[size] = '\0';
  int length = sizeof(charArray) / sizeof(char);
  for (int i = 0; i < size; i++) {
    ch[i] = charArray[(rand()) % (length)];
  }
  return ch;
}
const char* _regionNames[] = {"DistRegionAck", "DistRegionNoAck"};

DUNIT_TASK_DEFINITION(CLIENT1, StepOne)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(_regionNames[1], NO_ACK, locatorsG,
                                    "__TEST_POOL1__", false, false);
    LOG("StepOne complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, PutsTask)
  {
    int sizeArray = sizeof(keyArr) / sizeof(int);
    RegionPtr verifyReg = getHelper()->getRegion(_regionNames[1]);
    for (int count = 0; count < sizeArray; count++) {
      uint8_t* ptr = createRandByteArray(keyArr[count]);
      char* ptrChar = createRandCharArray(keyArr[count]);

      CacheableBytesPtr emptyBytesArr = CacheableBytes::create();
      CacheableBytesPtr bytePtrSent =
          CacheableBytes::createNoCopy(ptr, keyArr[count]);
      CacheableStringPtr stringPtrSent =
          CacheableString::createNoCopy(ptrChar, keyArr[count]);

      verifyReg->put(KEY_BYTE, bytePtrSent);
      verifyReg->put(KEY_STRING, stringPtrSent);
      verifyReg->put(KEY_EMPTY_BYTESARR, emptyBytesArr);

      char msgAssert1[100];
      char msgAssert2[100];

      sprintf(msgAssert1, "Contains Key byte failed for item %d ", count);
      sprintf(msgAssert2, "Contains Key String failed for item %d ", count);

      ASSERT(!verifyReg->containsKey(KEY_BYTE), msgAssert1);
      ASSERT(!verifyReg->containsKey(KEY_STRING), msgAssert2);
      ASSERT(!verifyReg->containsKey(KEY_EMPTY_BYTESARR),
             "Contains key failed for empty bytes array");
      ASSERT(!verifyReg->containsValueForKey(KEY_EMPTY_BYTESARR),
             "Contains value key failed for empty bytes array");

      CacheableBytesPtr bytePtrReturn =
          dynCast<CacheableBytesPtr>(verifyReg->get(KEY_BYTE));
      CacheableStringPtr stringPtrReturn =
          dynCast<CacheableStringPtr>(verifyReg->get(KEY_STRING));
      CacheableBytesPtr emptyBytesArrReturn =
          dynCast<CacheableBytesPtr>(verifyReg->get(KEY_EMPTY_BYTESARR));

      ASSERT(bytePtrReturn != NULLPTR, "Byte val is NULL");
      ASSERT(stringPtrReturn != NULLPTR, "String val is NULL");
      ASSERT(emptyBytesArrReturn != NULLPTR, "Empty Bytes Array ptr is NULL");

      bool isSameBytes = (bytePtrReturn->length() == bytePtrSent->length() &&
                          !memcmp(bytePtrReturn->value(), bytePtrSent->value(),
                                  bytePtrReturn->length()));
      bool isSameString =
          (stringPtrReturn->length() == stringPtrSent->length() &&
           !memcmp(stringPtrReturn->asChar(), stringPtrSent->asChar(),
                   stringPtrReturn->length()));
      if (isSameBytes && isSameString) {
        char logMSG[100];
        sprintf(logMSG, "Compare %d Passed for length %d", count,
                keyArr[count]);
        LOG(logMSG);
      }
      char failmsg[250];
      sprintf(failmsg, FAIL_MESSAGE, count);
      ASSERT((isSameBytes && isSameString), failmsg);

      ASSERT(emptyBytesArrReturn->length() == 0,
             "Empty Bytes Array  length is not 0.");

      verifyReg->put(KEY_EMPTY_BYTESARR,
                     emptyBytesArr);  // put the empty byte array second time
      ASSERT(!verifyReg->containsKey(KEY_EMPTY_BYTESARR),
             "Contains key failed for empty bytes array");
      ASSERT(!verifyReg->containsValueForKey(KEY_EMPTY_BYTESARR),
             "Contains value key failed for empty bytes array");

      CacheableBytesPtr emptyBytesArrReturn1 =
          dynCast<CacheableBytesPtr>(verifyReg->get(KEY_EMPTY_BYTESARR));
      ASSERT(emptyBytesArrReturn1 != NULLPTR, "Empty Bytes Array ptr is NULL");
      ASSERT(emptyBytesArrReturn1->length() == 0,
             "Empty Bytes Array  length is not 0.");
    }
    LOG("Doing clean exit");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT1, CloseCache1)
  { cleanProc(); }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER1, CloseServer1)
  {
    if (isLocalServer) {
      CacheHelper::closeServer(1);
      LOG("SERVER1 stopped");
    }
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    // CacheableHelper::registerBuiltins();
    CALL_TASK(CreateLocator1);
    CALL_TASK(CreateServer1_With_Locator);
    CALL_TASK(StepOne);
    CALL_TASK(PutsTask);
    CALL_TASK(CloseCache1);
    CALL_TASK(CloseServer1);
    CALL_TASK(CloseLocator1);
  }
END_MAIN
