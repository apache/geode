/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#ifndef __FrameworkTest_hpp__
#define __FrameworkTest_hpp__

#include "fwklib/TimeSync.hpp"
#include "fwklib/ClientTask.hpp"
#include "fwklib/FwkBBClient.hpp"

#include <string>

namespace gemfire {
namespace testframework {

class FrameworkTest  // Base class all test classes written for xml testing
                     // should derive from.
{
 private:
  TestDriver* m_coll;
  int32_t m_id;
  FwkBBClient* m_bbc;
  int32_t volatile m_deltaMicros;
  TimeSync* m_timeSync;
  FwkTask* m_task;

 protected:
  CachePtr m_cache;
  // bool m_istransaction;
  CacheTransactionManagerPtr txManager;
  static SpinLock m_lck;

#ifdef _WIN32
  bool m_doneSetNewAndDelete;

  void setNewAndDelete() {
    char* envsetting = ACE_OS::getenv("BUG481");
    if (envsetting != NULL && strlen(envsetting) > 0) {
      gemfire::setNewAndDelete(&operator new, & operator delete);
      FWKINFO("setNewAndDelete() was called");
    }
    m_doneSetNewAndDelete = true;
  }
#endif

 public:
#ifdef _WIN32
  FrameworkTest() : m_doneSetNewAndDelete(false) {}
#endif

  FrameworkTest(const char* initArgs);
  ~FrameworkTest();

  int32_t initialize(const char* initArgs) { return FWK_SUCCESS; }
  int32_t finalize() { return FWK_SUCCESS; }

  void cacheInitialize(PropertiesPtr& props,
                       const CacheAttributesPtr& cAttrs = NULLPTR);

  void cacheFinalize();

  void destroyAllRegions();

  void localDestroyRegion(RegionPtr& region);

  void incClientCount();

  void parseEndPoints(int32_t ep, std::string label, bool isServer);

  void createPool();

  std::string poolAttributesToString(PoolPtr& pool);

  void setTestScheme();

  QueryServicePtr checkQueryService();

  void setTask(const char* taskId) {
    m_task = (FwkTask*)(m_coll->getTaskById(taskId));
  }

  const std::string getTaskId() {
    std::string id;
    if (m_task != NULL) {
      return m_task->getTaskId();
    }
    return id;
  }

  int32_t getWaitTime() {
    if (m_task != NULL) {
      return m_task->getWaitTime();
    }
    return 0;
  }
  CacheTransactionManagerPtr gettxManager() { return txManager; }
  inline int32_t getClientId() { return m_id; }

  inline int32_t getDeltaMicros() { return m_deltaMicros; }

  inline int64_t getAdjustedNowMicros() {
    return m_timeSync->adjustedNowMicros();
  }

  /** brief Get std::string */
  const std::string getStringValue(const char* name) const {
    if (m_task == NULL) {
      return m_coll->getStringValue(name);
    }
    return m_task->getStringValue(name);
  }

  /** brief Get int32_t seconds in time string */
  int32_t getTimeValue(const char* name) const {
    return FwkStrCvt::toSeconds(getStringValue(name));
  }

  /** brief Get int32_t */
  int32_t getIntValue(const char* name) const {
    return FwkStrCvt::toInt32(getStringValue(name));
  }

  /** brief Get bool */
  bool getBoolValue(const char* name) const {
    return FwkStrCvt::toBool(getStringValue(name));
  }

  const FwkRegion* getSnippet(const std::string& name) const;

  const FwkPool* getPoolSnippet(const std::string& name) const;

  std::vector<std::string> getRoundRobinEP() const;

  void resetValue(const char* name) const {
    FwkData* data = (FwkData*)getData(name);
    if (data != NULL) data->reset();
  }

  /** brief Get FwkData pointer */
  const FwkData* getData(const char* name) const {
    if (m_task == NULL) {
      return m_coll->getData(name);
    }
    return m_task->getData(name);
  }

  /** @brief dump all data
    * @param sResult result of dump
    * @retval true = Success, false = Failed
    */
  inline std::string bbDump() const { return m_bbc->dump(); }

  /** @brief dump BB data
    * @param pszBBName name of BB
    * @param sResult result of dump
    * @retval true = Success, false = Failed
    */
  inline std::string bbDump(const std::string& bb) const {
    return m_bbc->dump(bb);
  }

  /** @brief clear BB data
    * @param pszBBName name of BB
    * @retval true = Success, false = Failed
    */
  inline void bbClear(const std::string& bb) const { m_bbc->clear(bb); }

  /** @brief get BB key value
    * @param bb name of BB
    * @param key name of key in BB
    * @retval value from BB
    */
  inline std::string bbGetString(const std::string& bb,
                                 const std::string& key) const {
    return m_bbc->getString(bb, key);
  }

  /** @brief get BB counter value
    * @param bb name of BB
    * @param cntr name of counter
    * @retval value from BB
    */
  inline int64_t bbGet(const std::string& bb, const std::string& cntr) const {
    return m_bbc->get(bb, cntr);
  }

  /** @brief set BB key value
    * @param bb name of BB
    * @param key name of key in BB
    * @param val value to set
    */
  inline void bbSet(const std::string& bb, const std::string& key,
                    const std::string& val) const {
    m_bbc->set(bb, key, val);
  }

  /** @brief set BB counter value
    * @param bb name of BB
    * @param cntr name of counter
    * @param val value to set
    */
  inline void bbSet(const std::string& bb, const std::string& cntr,
                    const int64_t val) const {
    m_bbc->set(bb, cntr, val);
  }

  /** @brief add BB counter value
    * @param bb name of BB
    * @param cntr name of counter
    * @param val value to add to counter
    * @retval value of after add
    */
  inline int64_t bbAdd(const std::string& bb, const std::string& cntr,
                       const int64_t val) const {
    return m_bbc->add(bb, cntr, val);
  }

  /** @brief increment BB counter value by 1
    * @param bb name of BB
    * @param cntr name of counter
    * @retval value after increment
    */
  inline int64_t bbIncrement(const std::string& bb,
                             const std::string& cntr) const {
    return m_bbc->increment(bb, cntr);
  }

  /** @brief decrement BB counter value by 1
    * @param bb name of BB
    * @param cntr name of counter
    * @retval value after decrement
    */
  inline int64_t bbDecrement(const std::string& bb,
                             const std::string& cntr) const {
    return m_bbc->decrement(bb, cntr);
  }

  /** @brief setIfGreater BB counter value is greater
    * @param bb name of BB
    * @param cntr name of counter
    * @param val value to set
    * @retval value after setIfGreater
    */
  inline int64_t bbSetIfGreater(const std::string& bb, const std::string& cntr,
                                const int64_t val) const {
    return m_bbc->setIfGreater(bb, cntr, val);
  }

  /** @brief setIfLess BB counter value is less
    * @param bb name of BB
    * @param cntr name of counter
    * @param val value to set
    * @retval value after setIfLess
    */
  inline int64_t bbSetIfLess(const std::string& bb, const std::string& cntr,
                             const int64_t val) const {
    return m_bbc->setIfLess(bb, cntr, val);
  }
};

}  // testframework
}  // gemfire
#endif  // __FrameworkTest_hpp__
