/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include <gfcpp/gf_base.hpp>

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

#define ROOT_SCOPE DISTRIBUTED_ACK

#include <gfcpp/GemfireCppCache.hpp>
#include <SerializationRegistry.hpp>
#include <gfcpp/CacheableString.hpp>
#include <gfcpp/GemfireTypeIds.hpp>
#include <GemfireTypeIdsImpl.hpp>

// Use to init lib when testing components.
#include <CppCacheLibrary.hpp>

#include "CacheHelper.hpp"

using namespace gemfire;

#include "locator_globals.hpp"

int32_t g_classIdToReturn = 0x04;
int32_t g_classIdToReturn2 = 0x1234;
int32_t g_classIdToReturn4 = 0x123456;

template <class T>
SharedPtr<T> duplicate(const SharedPtr<T>& orig) {
  SharedPtr<T> result;
  DataOutput dout;
  SerializationRegistry::serialize(orig, dout);
  // dout.writeObject(orig);

  uint32_t length = 0;
  const uint8_t* buffer = dout.getBuffer(&length);
  DataInput din(buffer, length);
  result = static_cast<T*>(SerializationRegistry::deserialize(din).ptr());
  // din.readObject(result);

  return result;
}

struct CData {
  int a;
  bool b;
  char c;
  double d;
  uint64_t e;
};

class OtherType : public Serializable {
 public:
  CData m_struct;
  int32_t m_classIdToReturn;

  explicit OtherType(int32_t classIdToReturn = g_classIdToReturn)
      : m_classIdToReturn(classIdToReturn) {
    m_struct.a = 0;
    m_struct.b = 0;
    m_struct.c = 0;
    m_struct.d = 0;
  }

  virtual void toData(DataOutput& output) const {
    output.writeBytes((uint8_t*)&m_struct, sizeof(CData));
    output.writeInt(m_classIdToReturn);
  }

  virtual uint32_t objectSize() const { return sizeof(CData); }

  virtual Serializable* fromData(DataInput& input) {
    int32_t size = 0;
    input.readArrayLen(&size);
    input.readBytesOnly(reinterpret_cast<uint8_t*>(&m_struct), size);
    input.readInt(&m_classIdToReturn);
    return this;
  }

  static Serializable* createDeserializable() {
    return new OtherType(g_classIdToReturn);
  }

  static Serializable* createDeserializable2() {
    return new OtherType(g_classIdToReturn2);
  }

  static Serializable* createDeserializable4() {
    return new OtherType(g_classIdToReturn4);
  }

  virtual int32_t classId() const { return m_classIdToReturn; }

  uint32_t size() const { return sizeof(CData); }

  static CacheablePtr uniqueCT(int32_t i,
                               int32_t classIdToReturn = g_classIdToReturn) {
    OtherType* ot = new OtherType(classIdToReturn);
    ot->m_struct.a = (int)i;
    ot->m_struct.b = (i % 2 == 0) ? true : false;
    ot->m_struct.c = static_cast<char>(65) + i;
    ot->m_struct.d = ((2.0) * static_cast<double>(i));

    printf("Created OtherType: %d, %s, %c, %e\n", ot->m_struct.a,
           ot->m_struct.b ? "true" : "false", ot->m_struct.c, ot->m_struct.d);

    printf("double hex 0x%016" PRIX64 "\n", ot->m_struct.e);

    return CacheablePtr(ot);
  }

  static void validateCT(int32_t i, const CacheablePtr otPtr) {
    char logmsg[1000];
    sprintf(logmsg, "validateCT for %d", i);
    LOG(logmsg);
    XASSERT(otPtr != NULLPTR);
    OtherType* ot = static_cast<OtherType*>(otPtr.ptr());
    XASSERT(ot != NULL);

    printf("Validating OtherType: %d, %s, %c, %e\n", ot->m_struct.a,
           ot->m_struct.b ? "true" : "false", ot->m_struct.c, ot->m_struct.d);

    printf("double hex 0x%016" PRIX64 "\n", ot->m_struct.e);

    XASSERT(ot->m_struct.a == (int)i);
    XASSERT(ot->m_struct.b == ((i % 2 == 0) ? true : false));
    XASSERT(ot->m_struct.c == (char)65 + i);
    XASSERT((ot->m_struct.d == (((double)2.0) * (double)i)));
  }
};

#define NoDist s2p2
#define Sender s1p1
#define Receiver s1p2

DUNIT_TASK(NoDist, SerializeInMemory)
  {
    CppCacheLibrary::initLib();

    CacheableStringPtr str = CacheableString::create("hello");
    ASSERT(str->length() == 5, "expected length 5.");

    CacheableStringPtr copy = duplicate(str);

    ASSERT(*str == *copy, "expected copy to be hello.");
    ASSERT(str != copy, "expected copy to be different object.");

    str = CacheableString::create("");
    copy = duplicate(str);
    ASSERT(copy != NULLPTR, "error null copy.");
    ASSERT(copy->length() == 0, "expected 0 length.");

    CacheableInt32Ptr intkey = CacheableInt32::create(1);
    CacheableInt32Ptr intcopy = duplicate(intkey);
    ASSERT(intcopy->value() == 1, "expected value 1.");

    CacheableInt64Ptr longkey = CacheableInt64::create(0x1122334455667788LL);
    CacheableInt64Ptr longcopy = duplicate(longkey);
    ASSERT(longcopy->value() == 0x1122334455667788LL,
           "expected value 0x1122334455667788.");

    struct blob {
      int m_a;
      bool m_b;
      char m_name[100];
    };
    struct blob borig;
    borig.m_a = 1;
    borig.m_b = true;
    strcpy(borig.m_name, "Joe Cool Coder");

    CacheableBytesPtr bytes = CacheableBytes::create(
        reinterpret_cast<uint8_t*>(&borig), sizeof(blob));
    CacheableBytesPtr bytesCopy = duplicate(bytes);
    struct blob* bcopy = (struct blob*)bytesCopy->value();
    ASSERT(0 == strcmp(bcopy->m_name, borig.m_name), "expected Joe Cool Coder");
    ASSERT(1 == bcopy->m_a, "expected value 1");
  }
ENDTASK

DUNIT_TASK(NoDist, OtherTypeInMemory)
  {
    Serializable::registerType(OtherType::createDeserializable);
    SharedPtr<OtherType> ot(new OtherType());
    ot->m_struct.a = 1;
    ot->m_struct.b = true;
    ot->m_struct.c = 2;
    ot->m_struct.d = 3.0;

    SharedPtr<OtherType> copy = duplicate(ot);

    ASSERT(copy->classId() == g_classIdToReturn, "unexpected classId");
    if (copy->classId() > 0xFFFF) {
      ASSERT(
          copy->typeId() == GemfireTypeIdsImpl::CacheableUserData4,
          "typeId should be equal to GemfireTypeIdsImpl::CacheableUserData4.");
    } else if (copy->classId() > 0xFF) {
      ASSERT(
          copy->typeId() == GemfireTypeIdsImpl::CacheableUserData2,
          "typeId should be equal to GemfireTypeIdsImpl::CacheableUserData2.");
    } else {
      ASSERT(
          copy->typeId() == GemfireTypeIdsImpl::CacheableUserData,
          "typeId should be equal to GemfireTypeIdsImpl::CacheableUserData.");
    }
    ASSERT(copy != ot, "expected different instance.");
    ASSERT(copy->m_struct.a == 1, "a == 1");
    ASSERT(copy->m_struct.b == true, "b == true");
    ASSERT(copy->m_struct.c == 2, "c == 2");
    ASSERT(copy->m_struct.d == 3.0, "d == 3.0");
  }
ENDTASK

RegionPtr regionPtr;

DUNIT_TASK(Receiver, SetupR)
  {
    CacheHelper::initLocator(1);
    CacheHelper::initServer(1, "cacheserver_notify_subscription.xml",
                            locatorsG);
    LOG("SERVER started");
  }
ENDTASK

DUNIT_TASK(Sender, SetupAndPutInts)
  {
    Serializable::registerType(OtherType::createDeserializable);
    Serializable::registerType(OtherType::createDeserializable2);
    Serializable::registerType(OtherType::createDeserializable4);
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion("DistRegionAck", USE_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);
    LOG("SenderInit complete.");

    regionPtr = getHelper()->getRegion("DistRegionAck");
    for (int32_t i = 0; i < 10; i++) {
      regionPtr->put(i, CacheableInt32::create(i));
    }
  }
ENDTASK

DUNIT_TASK(Sender, SendCT)
  {
    for (int32_t i = 0; i < 30; i += 3) {
      try {
        regionPtr->put(i, OtherType::uniqueCT(i, g_classIdToReturn));
        regionPtr->put(i + 1, OtherType::uniqueCT(i + 1, g_classIdToReturn2));
        regionPtr->put(i + 2, OtherType::uniqueCT(i + 2, g_classIdToReturn4));
      } catch (const gemfire::TimeoutException&) {
      }
    }
  }
ENDTASK

DUNIT_TASK(Sender, RValidateCT)
  {
    for (int32_t i = 0; i < 30; i += 3) {
      OtherType::validateCT(i, regionPtr->get(i));
      OtherType::validateCT(i + 1, regionPtr->get(i + 1));
      OtherType::validateCT(i + 2, regionPtr->get(i + 2));
    }
  }
ENDTASK

DUNIT_TASK(Receiver, CloseCacheR)
  {
    CacheHelper::closeServer(1);
    CacheHelper::closeLocator(1);
    LOG("SERVER closed");
  }
ENDTASK

DUNIT_TASK(Sender, CloseCacheS)
  {
    regionPtr = NULLPTR;
    cleanProc();
  }
ENDTASK
