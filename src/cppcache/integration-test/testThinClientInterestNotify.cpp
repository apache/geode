/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fw_dunit.hpp"
#include "ThinClientHelper.hpp"

/* Testing Parameters              Param's Value
Server notify-by-subscription:                   true

Descripton:  This is to test the receiveValues flag
in register interest APIs. Client events
delivered should include creates/updates or not include them (converted into
invalidates).
*/

#define CLIENT_NBS_TRUE s1p1
/*
#define CLIENT_NBS_FALSE s1p2
#define CLIENT_NBS_DEFAULT s2p1
*/
#define SERVER_AND_FEEDER s2p2
#define SERVER1 s2p2  // duplicate definition required for a helper file

class EventListener : public CacheListener {
 public:
  int m_creates;
  int m_updates;
  int m_invalidates;
  int m_destroys;
  std::string m_name;

  void check(const EntryEvent& event, const char* eventType) {
    char buf[256] = {'\0'};

    try {
      CacheableStringPtr keyPtr = dynCast<CacheableStringPtr>(event.getKey());
      CacheableInt32Ptr valuePtr =
          dynCast<CacheableInt32Ptr>(event.getNewValue());

      sprintf(
          buf, "%s: %s: Key = %s, NewValue = %s", m_name.c_str(), eventType,
          keyPtr->asChar(),
          (valuePtr == NULLPTR ? "NULLPTR" : valuePtr->toString()->asChar()));
      LOG(buf);
    } catch (const Exception& excp) {
      sprintf(buf, "%s: %s: %s: %s", m_name.c_str(), eventType, excp.getName(),
              excp.getMessage());
      LOG(buf);
    } catch (...) {
      sprintf(buf, "%s: %s: unknown exception", m_name.c_str(), eventType);
      LOG(buf);
    }
  }

 public:
  explicit EventListener(const char* name)
      : m_creates(0),
        m_updates(0),
        m_invalidates(0),
        m_destroys(0),
        m_name(name) {}

  ~EventListener() {}

  virtual void afterCreate(const EntryEvent& event) {
    check(event, "afterCreate");
    m_creates++;
  }

  virtual void afterUpdate(const EntryEvent& event) {
    check(event, "afterUpdate");
    m_updates++;
  }

  virtual void afterInvalidate(const EntryEvent& event) {
    check(event, "afterInvalidate");
    m_invalidates++;
  }

  virtual void afterDestroy(const EntryEvent& event) {
    check(event, "afterDestroy");
    m_destroys++;
  }

  void reset() {
    m_creates = 0;
    m_updates = 0;
    m_invalidates = 0;
    m_destroys = 0;
  }

  // validate expected event counts
  void validate(int creates, int updates, int invalidates, int destroys) {
    char logmsg[256] = {'\0'};
    sprintf(logmsg, "VALIDATE CALLED for %s", m_name.c_str());
    LOG(logmsg);
    sprintf(logmsg, "creates: expected = %d, actual = %d", creates, m_creates);
    LOG(logmsg);
    ASSERT(m_creates == creates, logmsg);
    sprintf(logmsg, "updates: expected = %d, actual = %d", updates, m_updates);
    LOG(logmsg);
    ASSERT(m_updates == updates, logmsg);
    sprintf(logmsg, "invalidates: expected = %d, actual = %d", invalidates,
            m_invalidates);
    LOG(logmsg);
    ASSERT(m_invalidates == invalidates, logmsg);
    sprintf(logmsg, "destroys: expected = %d, actual = %d", destroys,
            m_destroys);
    LOG(logmsg);
    ASSERT(m_destroys == destroys, logmsg);
  }
};
typedef SharedPtr<EventListener> EventListenerPtr;

void setCacheListener(const char* regName, EventListenerPtr monitor) {
  RegionPtr reg = getHelper()->getRegion(regName);
  AttributesMutatorPtr attrMutator = reg->getAttributesMutator();
  attrMutator->setCacheListener(monitor);
}

// clientXXRegionYY where XX is NBS setting and YY is receiveValue setting in
// RegisterInterest API calls,
// RegionOther means no interest registered so no events should arrive except
// invalidates when NBS == false.

EventListenerPtr clientTrueRegionTrue = NULLPTR;
EventListenerPtr clientTrueRegionFalse = NULLPTR;
EventListenerPtr clientTrueRegionOther = NULLPTR;
/*
EventListenerPtr clientFalseRegionTrue = NULLPTR;
EventListenerPtr clientFalseRegionFalse = NULLPTR;
EventListenerPtr clientFalseRegionOther = NULLPTR;
EventListenerPtr clientDefaultRegionTrue = NULLPTR;
EventListenerPtr clientDefaultRegionFalse = NULLPTR;
EventListenerPtr clientDefaultRegionOther = NULLPTR;
*/

const char* regions[] = {"RegionTrue", "RegionFalse", "RegionOther"};
const char* keysForRegex[] = {"key-regex-1", "key-regex-2", "key-regex-3"};

#include "ThinClientDurableInit.hpp"
#include "ThinClientTasks_C2S2.hpp"
#include "LocatorHelper.hpp"

void initClientForInterestNotify(EventListenerPtr& mon1, EventListenerPtr& mon2,
                                 EventListenerPtr& mon3, const char* nbs,
                                 const char* clientName) {
  PropertiesPtr props = Properties::create();
  // props->insert( "notify-by-subscription-override", nbs );

  initClient(true, props);

  LOG("CLIENT: Setting pool with locator.");
  getHelper()->createPoolWithLocators("__TESTPOOL1_", locatorsG, true, 0, 1);
  createRegionAndAttachPool(regions[0], USE_ACK, "__TESTPOOL1_", true);
  createRegionAndAttachPool(regions[1], USE_ACK, "__TESTPOOL1_", true);
  createRegionAndAttachPool(regions[2], USE_ACK, "__TESTPOOL1_", true);

  std::string name1 = clientName;
  name1 += "_";
  name1 += regions[0];
  std::string name2 = clientName;
  name2 += "_";
  name2 += regions[1];
  std::string name3 = clientName;
  name3 += "_";
  name3 += regions[2];

  // Recreate listeners
  mon1 = new EventListener(name1.c_str());
  mon2 = new EventListener(name2.c_str());
  mon3 = new EventListener(name3.c_str());

  setCacheListener(regions[0], mon1);
  setCacheListener(regions[1], mon2);
  setCacheListener(regions[2], mon3);

  LOG("initClientForInterestNotify complete.");
}

void feederPuts(int count) {
  for (int region = 0; region < 3; region++) {
    for (int key = 0; key < 3; key++) {
      // if you create entry with value == 0 it does check for
      // value not exist and fails so start the entry count from 1.
      for (int entry = 1; entry <= count; entry++) {
        createIntEntry(regions[region], keys[key], entry);
        createIntEntry(regions[region], keysForRegex[key], entry);
      }
    }
  }
}

void feederInvalidates() {
  for (int region = 0; region < 3; region++) {
    for (int key = 0; key < 3; key++) {
      invalidateEntry(regions[region], keys[key]);
      invalidateEntry(regions[region], keysForRegex[key]);
    }
  }
}

void feederDestroys() {
  for (int region = 0; region < 3; region++) {
    for (int key = 0; key < 3; key++) {
      destroyEntry(regions[region], keys[key]);
      destroyEntry(regions[region], keysForRegex[key]);
    }
  }
}

void registerInterests(const char* region, bool durable, bool receiveValues) {
  RegionPtr regionPtr = getHelper()->getRegion(region);

  VectorOfCacheableKey keysVector;

  keysVector.push_back(CacheableKey::create(keys[0]));
  keysVector.push_back(CacheableKey::create(keys[1]));
  keysVector.push_back(CacheableKey::create(keys[2]));

  regionPtr->registerKeys(keysVector, durable, true, receiveValues);

  regionPtr->registerRegex("key-regex.*", durable, NULLPTR, true,
                           receiveValues);
}

void unregisterInterests(const char* region) {
  RegionPtr regionPtr = getHelper()->getRegion(region);

  VectorOfCacheableKey keysVector;

  keysVector.push_back(CacheableKey::create(keys[0]));
  keysVector.push_back(CacheableKey::create(keys[1]));
  keysVector.push_back(CacheableKey::create(keys[2]));

  regionPtr->unregisterKeys(keysVector);

  regionPtr->unregisterRegex("key-regex.*");
}

void closeClient() {
  getHelper()->disconnect();
  cleanProc();
  LOG("CLIENT CLOSED");
}

DUNIT_TASK_DEFINITION(SERVER_AND_FEEDER, StartServerWithLocator_NBS)
  {
    if (isLocalServer) {
      CacheHelper::initServer(1, "cacheserver_interest_notify.xml", locatorsG);
    }
    LOG("SERVER with NBS=false started with locator");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER_AND_FEEDER, FeederUpAndFeed)
  {
    initClientWithPool(true, "__TEST_POOL1__", locatorsG, "ServerGroup1",
                       NULLPTR, 0, true);
    getHelper()->createPooledRegion(regions[0], USE_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);
    getHelper()->createPooledRegion(regions[1], USE_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);
    getHelper()->createPooledRegion(regions[2], USE_ACK, locatorsG,
                                    "__TEST_POOL1__", true, true);

    feederPuts(1);

    LOG("FeederUpAndFeed complete.");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_NBS_TRUE, ClientNbsTrue_Up)
  {
    initClientForInterestNotify(clientTrueRegionTrue, clientTrueRegionFalse,
                                clientTrueRegionOther, "true", "clientNbsTrue");
    LOG("ClientNbsTrue_Up complete");
  }
END_TASK_DEFINITION

/*
DUNIT_TASK_DEFINITION(CLIENT_NBS_FALSE, ClientNbsFalse_Up)
{
  initClientForInterestNotify( clientFalseRegionTrue ,
  clientFalseRegionFalse, clientFalseRegionOther, "false", "clientNbsFalse" );
  LOG("ClientNbsFalse_Up complete");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_NBS_DEFAULT, ClientNbsDefault_Up)
{
  initClientForInterestNotify( clientDefaultRegionTrue ,
  clientDefaultRegionFalse, clientDefaultRegionOther, "server",
"clientNbsDefault" );
  LOG("ClientNbsDefault_Up complete");
}
END_TASK_DEFINITION
*/

DUNIT_TASK_DEFINITION(CLIENT_NBS_TRUE, ClientNbsTrue_Register)
  {
    registerInterests(regions[0], false, true);
    registerInterests(regions[1], false, false);

    // We intentionally DO NOT  register interest  in the third region to
    // check that we don't get unexpected events based on the NBS setting.
    // registerInterests(regions[2], false, true);

    LOG("ClientNbsTrue_Register complete");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_NBS_TRUE, ClientNbsTrue_Unregister)
  {
    unregisterInterests(regions[0]);
    unregisterInterests(regions[1]);
    // unregisterInterests(regions[2]);
    LOG("ClientNbsTrue_Unregister complete");
  }
END_TASK_DEFINITION

/*
DUNIT_TASK_DEFINITION(CLIENT_NBS_FALSE, ClientNbsFalse_Register)
{
  registerInterests(regions[0], false, true);
  registerInterests(regions[1], false, false);

  // We intentionally DO NOT register interest in the third region to
  // check that we don't get unexpected events based on the NBS setting.
  //registerInterests(regions[2], false, true);

  LOG("ClientNbsFalse_Register complete");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_NBS_FALSE, ClientNbsFalse_Unregister)
{
  unregisterInterests(regions[0]);
  unregisterInterests(regions[1]);
  //unregisterInterests(regions[2]);
  LOG("ClientNbsFalse_Unregister complete");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_NBS_DEFAULT, ClientNbsDefault_Register)
{
  registerInterests(regions[0], false, true);
  registerInterests(regions[1], false, false);

  // We intentionally DO NOT  register interest  in the third region to
  // check that we don't get unexpected events based on the NBS setting.
  //registerInterests(regions[2], false, true);

  LOG("ClientNbsDefault_Register complete");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_NBS_DEFAULT, ClientNbsDefault_Unregister)
{
  unregisterInterests(regions[0]);
  unregisterInterests(regions[1]);
  //unregisterInterests(regions[2]);
  LOG("ClientNbsDefault_Unregister complete");
}
END_TASK_DEFINITION
*/

DUNIT_TASK_DEFINITION(SERVER_AND_FEEDER, FeederDoOps)
  {
    // Do 3 puts, 1 invalidate and 1 destroy for each of the 6 keys
    feederPuts(3);
    feederInvalidates();
    feederDestroys();
    LOG("FeederDoOps complete.");
  }
END_TASK_DEFINITION

// VERIFICATION COUNTS:
// Each regon has 6 keys, for each key feeder does:
// 3 puts, 1 invalidate, 1 destroy.
// Invalidate operations from the feeder are not sent to the server so
// registered clients do not receive those invalidates only those
// that are registered interest or due to notify-by-subscription=false.
// Feeder does the above steps 3 times, once when clients have registered
// interest, then when clients unregister interest, then again when clients
// re-register interest.

DUNIT_TASK_DEFINITION(CLIENT_NBS_TRUE, ClientNbsTrue_Verify)
  {
    // validate expected creates, updates, invalidates and destroys in that
    // order
    // Verify only events received while client had registered interest
    clientTrueRegionTrue->validate(6, 30, 0, 12);
    clientTrueRegionFalse->validate(0, 0, 36, 12);
    clientTrueRegionOther->validate(0, 0, 0, 0);
    LOG("ClientNbsTrue_Verify complete");
  }
END_TASK_DEFINITION

/*
DUNIT_TASK_DEFINITION(CLIENT_NBS_FALSE, ClientNbsFalse_Verify)
{
  //validate expected creates, updates, invalidates and destroys in that order
  // Verify only events received while client had registered interest
  clientFalseRegionTrue->validate(0, 0, 54, 18);
  clientFalseRegionFalse->validate(0, 0, 54, 18);
  clientFalseRegionOther->validate(0, 0, 54, 18);
  LOG("ClientNbsFalse_Verify complete");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_NBS_DEFAULT, ClientNbsDefault_Verify)
{
  //validate expected creates, updates, invalidates and destroys in that order
  // Verify only events received while client had registered interest
  clientDefaultRegionTrue->validate(0, 0, 54, 18);
  clientDefaultRegionFalse->validate(0, 0, 54, 18);
  clientDefaultRegionOther->validate(0, 0, 54, 18);
  LOG("ClientNbsDefault_Verify complete");
}
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(CLIENT_NBS_DEFAULT, ClientNbsDefault_Close)
{
  cleanProc();
  LOG("ClientNbsDefault_Close complete");
}
END_TASK_DEFINITION
*/

DUNIT_TASK_DEFINITION(CLIENT_NBS_TRUE, ClientNbsTrue_Close)
  {
    cleanProc();
    LOG("ClientNbsTrue_Close complete");
  }
END_TASK_DEFINITION

/*
DUNIT_TASK_DEFINITION(CLIENT_NBS_FALSE, ClientNbsFalse_Close)
{
  cleanProc();
  LOG("ClientNbsFalse_Close complete");
}
END_TASK_DEFINITION
*/

DUNIT_TASK_DEFINITION(SERVER_AND_FEEDER, CloseFeeder)
  {
    cleanProc();
    LOG("FEEDER closed");
  }
END_TASK_DEFINITION

DUNIT_TASK_DEFINITION(SERVER_AND_FEEDER, CloseServer)
  {
    CacheHelper::closeServer(1);
    LOG("SERVER closed");
  }
END_TASK_DEFINITION

DUNIT_MAIN
  {
    CALL_TASK(StartLocator);
    CALL_TASK(StartServerWithLocator_NBS);

    CALL_TASK(FeederUpAndFeed);

    CALL_TASK(ClientNbsTrue_Up);
    /*
    CALL_TASK( ClientNbsFalse_Up );
    CALL_TASK( ClientNbsDefault_Up );
    */

    CALL_TASK(ClientNbsTrue_Register);
    /*
    CALL_TASK( ClientNbsFalse_Register );
    CALL_TASK( ClientNbsDefault_Register );
    */

    // Do 3 puts, 1 invalidate and 1 destroy for
    // each of the 6 keys while client has registered interest
    CALL_TASK(FeederDoOps);

    // wait for queues to drain
    SLEEP(10000);

    CALL_TASK(ClientNbsTrue_Unregister);
    /*
    CALL_TASK( ClientNbsFalse_Unregister );
    CALL_TASK( ClientNbsDefault_Unregister );
    */

    // Do 3 puts, 1 invalidate and 1 destroy for
    // each of the 6 keys while client has UN-registered interest
    CALL_TASK(FeederDoOps);

    CALL_TASK(ClientNbsTrue_Register);
    /*
    CALL_TASK( ClientNbsFalse_Register );
    CALL_TASK( ClientNbsDefault_Register );
    */

    // Do 3 puts, 1 invalidate and 1 destroy for
    // each of the 6 keys while client has RE-registered interest
    CALL_TASK(FeederDoOps);

    // wait for queues to drain
    SLEEP(10000);

    // Verify only events received while client had registered interest
    CALL_TASK(ClientNbsTrue_Verify);
    /*
    CALL_TASK( ClientNbsFalse_Verify );
    CALL_TASK( ClientNbsDefault_Verify );
    */

    CALL_TASK(ClientNbsTrue_Close);
    /*
    CALL_TASK( ClientNbsFalse_Close );
    CALL_TASK( ClientNbsDefault_Close );
    */

    CALL_TASK(CloseFeeder);

    CALL_TASK(CloseServer);

    closeLocator();
  }
END_MAIN
