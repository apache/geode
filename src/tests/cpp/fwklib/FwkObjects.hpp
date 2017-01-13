/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

/**
  * @file    FwkObjects.hpp
  * @since   1.0
  * @version 1.0
  * @see
  */

// ----------------------------------------------------------------------------

#ifndef __FWK_OBJECTS_HPP__
#define __FWK_OBJECTS_HPP__

#include <gfcpp/Properties.hpp>
#include <gfcpp/ExpirationAction.hpp>
#include <gfcpp/RegionAttributes.hpp>
#include <gfcpp/AttributesFactory.hpp>
#include <gfcpp/PoolManager.hpp>

#include "fwklib/FwkStrCvt.hpp"
#include "fwklib/FwkLog.hpp"

#include "fwklib/GsRandom.hpp"

#include <vector>
#include <list>
#include <map>
#include <ace/OS.h>
#include <errno.h>

#include <xercesc/dom/DOM.hpp>

XERCES_CPP_NAMESPACE_USE

using namespace gemfire::testframework;

#define HOSTGROUP_TAG "hostGroup"
#define LOCALFILE_TAG "localFile"
#define DATA_TAG "data"
#define DATASET_TAG "data-set"
#define CLIENTSET_TAG "client-set"
#define TEST_TAG "test"
#define ONEOF_TAG "oneof"
#define LIST_TAG "list"
#define RANGE_TAG "range"
#define SNIPPET_TAG "snippet"
#define REGION_TAG "region"
#define POOL_TAG "pool"
#define TASK_TAG "task"
#define CLIENT_TAG "client"
#define ITEM_TAG "item"
#define REGIONTIMETOLIVE_TAG "region-time-to-live"
#define REGIONIDLETIME_TAG "region-idle-time"
#define ENTRYTIMETOLIVE_TAG "entry-time-to-live"
#define ENTRYIDLETIME_TAG "entry-idle-time"
#define CACHELOADER_TAG "cache-loader"
#define CACHELISTENER_TAG "cache-listener"
#define CACHEWRITER_TAG "cache-writer"
#define PERSISTENCEMANAGER_TAG "persistence-manager"
#define PROPERTIES_TAG "properties"
#define PROPERTY_TAG "property"

#define CLIENT_STATUS_BB "clientStatusBB"

namespace gemfire {
namespace testframework {

// ----------------------------------------------------------------------------

#define FWK_SUCCESS 0
#define FWK_WARNING 1
#define FWK_ERROR 2
#define FWK_SEVERE 3

// ----------------------------------------------------------------------------

/** @brief Table of object enumeration data types */
typedef enum eFwkDataType {
  DATA_TYPE_NULL,
  DATA_TYPE_CONTENT,
  DATA_TYPE_LIST,
  DATA_TYPE_ONEOF,
  DATA_TYPE_RANGE,
  DATA_TYPE_SNIPPET
} tFwkDataType;

// ----------------------------------------------------------------------------

class FwkObject {
  std::string m_name;

 public:
  FwkObject() {}
  virtual ~FwkObject() {}

  void setName(std::string name) { m_name = name; }
  const std::string& getName() const { return m_name; }

  /** @brief Get key string */
  virtual const std::string& getKey() const = 0;
  virtual void print() const = 0;
};

// ----------------------------------------------------------------------------

/**
  * @class TFwkSet
  *
  * @brief Framework base data object set template
  */

template <class FWK_OBJECT>
class TFwkSet {
 private:
  std::string m_name;
  bool m_deleteOnClear;
  std::vector<const FWK_OBJECT*> m_vec;

 public:
  TFwkSet() : m_deleteOnClear(true) {}

  ~TFwkSet() { clear(); }

  const std::string& getName() const { return m_name; }

  void setName(std::string name) { m_name = name; }

  void setNoDelete() { m_deleteOnClear = false; }

  /** @brief Prints collection of objects */
  void print() const {
    const FWK_OBJECT* obj = getFirst();
    while (obj != NULL) {
      obj->print();
      obj = getNext(obj);
    }
  }

  /** @brief Find a object in collection
    * @param key Object key to find
    */
  const FWK_OBJECT* find(const std::string& key) const {
    const FWK_OBJECT* obj = NULL;
    int32_t pos = findIdx(key);
    if (pos != -1) {
      obj = m_vec.at(pos);
    }
    return obj;
  }

  /** @brief Get first object in collection */
  const FWK_OBJECT* getFirst() const {
    const FWK_OBJECT* obj = NULL;
    int32_t siz = static_cast<int32_t>(m_vec.size());
    if (siz > 0) {
      return m_vec.at(0);
    }
    return obj;
  }

  /** @brief Get next object in collection */
  const FWK_OBJECT* getNext(const FWK_OBJECT* prev) const {
    const FWK_OBJECT* obj = NULL;
    if (prev == NULL) {
      return getFirst();
    }
    int32_t siz = static_cast<int32_t>(m_vec.size());
    if (siz > 0) {
      const std::string key = prev->getKey();
      int32_t pos = findIdx(key);
      if (++pos < siz) {
        return m_vec.at(pos);
      }
    }
    return obj;
  }

  /** @brief Clears collection of objects */
  void clear() {
    int32_t siz = static_cast<int32_t>(m_vec.size());
    while (siz > 0) {
      const FWK_OBJECT* obj = m_vec.back();
      m_vec.pop_back();
      if (m_deleteOnClear) {
        delete obj;
      }
      siz = static_cast<int32_t>(m_vec.size());
    }
  }

  /** @brief Add an object
    * @param obj Object to add
    */
  void add(const FWK_OBJECT* obj) {
    if (obj != NULL) {
      m_vec.push_back(obj);
    }
  }

  const FWK_OBJECT* at(int32_t idx) const {
    if ((idx < 0) || (idx > size())) return NULL;
    return m_vec.at(idx);
  }

  /** @brief Get count of objects in collection */
  int32_t size() const { return static_cast<int32_t>(m_vec.size()); }

 private:
  int32_t findIdx(const std::string& key) const {
    int32_t idx = -1;
    if (!key.empty()) {
      int32_t pos = 0;
      int32_t max = static_cast<int32_t>(m_vec.size());
      while (pos < max) {
        const FWK_OBJECT* curr = (const FWK_OBJECT*)m_vec.at(pos);
        if (curr->getKey() == key) {
          idx = pos;
          max = -1;
        }
        pos++;
      }
    }
    return idx;
  }
};

// ----------------------------------------------------------------------------

typedef std::vector<std::string> StringVector;

// ----------------------------------------------------------------------------

/** @class XMLStringConverter
  * @brief  This is a simple class that lets us do easy (though not
  * terribly efficient) trancoding of char* data to XMLCh data.
  */
class XMLStringConverter {
 public:
  XMLStringConverter(const char* const toTranscode) {
    m_charForm = NULL;
    // Call the private transcoding method
    m_unicodeForm = XMLString::transcode(toTranscode);
  }

  XMLStringConverter(const XMLCh* toTranscode) {
    m_unicodeForm = NULL;
    m_charForm = XMLString::transcode(toTranscode);
  }

  ~XMLStringConverter() {
    if (m_unicodeForm) XMLString::release(&m_unicodeForm);
    if (m_charForm) XMLString::release(&m_charForm);
  }

  /** @brief  get unicode methods */
  const XMLCh* unicodeForm() const { return m_unicodeForm; }

  /** @brief  get char methods */
  std::string charForm() const {
    std::string sCharForm;
    if (m_charForm) sCharForm = m_charForm;
    return sCharForm;
  }

 private:
  XMLCh* m_unicodeForm;
  char* m_charForm;
};

// ----------------------------------------------------------------------------

/** @brief convert XMLCh to a string */
#define XMLChToStr(str) XMLStringConverter(str).charForm()

/** @brief convert string to XMLCh */
#define StrToXMLCh(str) XMLStringConverter(str).unicodeForm()

// ----------------------------------------------------------------------------

class ActionPair {
  std::string m_libraryName;
  std::string m_libraryFunctionName;

  void setLibraryName(std::string name) { m_libraryName = name; }
  void setLibraryFunctionName(std::string name) {
    m_libraryFunctionName = name;
  }

 public:
  ActionPair(const DOMNode* node);

  const char* getLibraryName() { return m_libraryName.c_str(); }
  const char* getLibraryFunctionName() { return m_libraryFunctionName.c_str(); }
};

// ----------------------------------------------------------------------------

class ExpiryAttributes {
  int32_t m_timeout;
  ExpirationAction::Action m_action;

  void setTimeout(std::string str) { m_timeout = FwkStrCvt::toInt32(str); }

  void setAction(std::string action) {
    if (action == "invalidate") {
      m_action = ExpirationAction::INVALIDATE;
    } else if (action == "destroy") {
      m_action = ExpirationAction::DESTROY;
    } else if (action == "local-invalidate") {
      m_action = ExpirationAction::LOCAL_INVALIDATE;
    } else if (action == "local-destroy") {
      m_action = ExpirationAction::LOCAL_DESTROY;
    }
  }

 public:
  ExpiryAttributes(const DOMNode* node);

  ExpirationAction::Action getAction() { return m_action; }
  int32_t getTimeout() { return m_timeout; }
};

// ----------------------------------------------------------------------------

class PersistManager {
  std::string m_libraryName;
  std::string m_libraryFunctionName;
  PropertiesPtr m_properties;

  void setLibraryName(std::string name) { m_libraryName = name; }
  void setLibraryFunctionName(std::string name) {
    m_libraryFunctionName = name;
  }
  void addProperties(const DOMNode* node);
  void addProperty(const DOMNode* node);

 public:
  PersistManager(const DOMNode* node);
  ~PersistManager() { m_properties = NULLPTR; }

  const char* getLibraryName() { return m_libraryName.c_str(); }
  const char* getLibraryFunctionName() { return m_libraryFunctionName.c_str(); }
  PropertiesPtr& getProperties() { return m_properties; }
};

// ----------------------------------------------------------------------------

class Attributes {
  AttributesFactory m_factory;
  bool m_isLocal;
  bool m_withPool;

  void setCachingEnabled(std::string val) {
    m_factory.setCachingEnabled(FwkStrCvt::toBool(val));
  }

  void setLoadFactor(std::string val) {
    m_factory.setLoadFactor(FwkStrCvt::toFloat(val));
  }

  void setConcurrencyLevel(std::string val) {
    m_factory.setConcurrencyLevel((uint8_t)FwkStrCvt::toInt32(val));
  }

  void setLruEntriesLimit(std::string val) {
    m_factory.setLruEntriesLimit(FwkStrCvt::toUInt32(val));
  }

  void setInitialCapacity(std::string val) {
    m_factory.setInitialCapacity(FwkStrCvt::toInt32(val));
  }

  void setDiskPolicy(std::string val) {
    m_factory.setDiskPolicy(DiskPolicyType::fromName(val.c_str()));
  }
  void setCloningEnabled(std::string val) {
    m_factory.setCloningEnabled(FwkStrCvt::toBool(val));
  }

  void setRegionTimeToLive(ExpiryAttributes* val) {
    m_factory.setRegionTimeToLive(val->getAction(), val->getTimeout());
    delete val;
  }

  void setRegionIdleTime(ExpiryAttributes* val) {
    m_factory.setRegionIdleTimeout(val->getAction(), val->getTimeout());
    delete val;
  }
  void setEntryTimeToLive(ExpiryAttributes* val) {
    m_factory.setEntryTimeToLive(val->getAction(), val->getTimeout());
    delete val;
  }
  void setEntryIdleTime(ExpiryAttributes* val) {
    m_factory.setEntryIdleTimeout(val->getAction(), val->getTimeout());
    delete val;
  }

  void setCacheLoader(ActionPair* val) {
    m_factory.setCacheLoader(val->getLibraryName(),
                             val->getLibraryFunctionName());
  }

  void setCacheListener(ActionPair* val) {
    m_factory.setCacheListener(val->getLibraryName(),
                               val->getLibraryFunctionName());
  }

  void setCacheWriter(ActionPair* val) {
    m_factory.setCacheWriter(val->getLibraryName(),
                             val->getLibraryFunctionName());
  }
  void setConcurrencyCheckEnabled(std::string val) {
    m_factory.setConcurrencyChecksEnabled(FwkStrCvt::toBool(val));
  }
  void setPersistenceManager(PersistManager* val) {
    m_factory.setPersistenceManager(val->getLibraryName(),
                                    val->getLibraryFunctionName(),
                                    val->getProperties());
  }

  ExpiryAttributes* getExpiryAttributes(const DOMNode* node);

 public:
  Attributes(const DOMNode* node);

  RegionAttributesPtr getAttributes() {
    return m_factory.createRegionAttributes();
  }

  void setPoolName(std::string val) {
    if (!val.empty()) {
      m_factory.setPoolName(val.c_str());
      m_withPool = true;
    } else {
      m_factory.setPoolName(val.c_str());
      m_withPool = false;
    }
  }

  bool isLocal() { return m_isLocal; }

  bool isWithPool() { return m_withPool; }
};
// ----------------------------------------------------------------------------

class FwkRegion {
  std::string m_name;
  Attributes* m_attributes;

  void setName(std::string name) { m_name = name; }
  void setAttributes(Attributes* attributes) { m_attributes = attributes; }

 public:
  FwkRegion(const DOMNode* node);
  ~FwkRegion() {
    if (m_attributes != NULL) {
      delete m_attributes;
      m_attributes = NULL;
    }
  }

  const std::string& getName() const { return m_name; }
  Attributes* getAttributes() { return m_attributes; }
  const RegionAttributesPtr getAttributesPtr() const {
    return m_attributes->getAttributes();
  }
  void print() const { FWKINFO("FwkRegion " << m_name); }
};

// ---------------------------------------------------------------------------------

class FwkPool {
  std::string m_name;
  PoolFactoryPtr m_poolFactory;
  bool m_locators;
  bool m_servers;

  void setName(std::string name) { m_name = name; }
  void setAttributesToFactory(const DOMNode* node);

  void setFreeConnectionTimeout(std::string val) {
    m_poolFactory->setFreeConnectionTimeout(FwkStrCvt::toInt32(val));
  }

  void setLoadConditioningInterval(std::string val) {
    m_poolFactory->setLoadConditioningInterval(FwkStrCvt::toInt32(val));
  }

  void setSocketBufferSize(std::string val) {
    m_poolFactory->setSocketBufferSize(FwkStrCvt::toInt32(val));
  }

  void setReadTimeout(std::string val) {
    m_poolFactory->setReadTimeout(FwkStrCvt::toInt32(val));
  }

  void setMinConnections(std::string val) {
    m_poolFactory->setMinConnections(FwkStrCvt::toInt32(val));
  }

  void setMaxConnections(std::string val) {
    m_poolFactory->setMaxConnections(FwkStrCvt::toInt32(val));
  }

  void setIdleTimeout(std::string val) {
    m_poolFactory->setIdleTimeout(FwkStrCvt(val).toLong());
  }

  void setRetryAttempts(std::string val) {
    m_poolFactory->setRetryAttempts(FwkStrCvt::toInt32(val));
  }

  void setPingInterval(std::string val) {
    m_poolFactory->setPingInterval(FwkStrCvt(val).toLong());
  }

  void setStatisticInterval(std::string val) {
    m_poolFactory->setStatisticInterval(FwkStrCvt::toInt32(val));
  }

  void setServerGroup(std::string val) {
    m_poolFactory->setServerGroup(val.c_str());
  }

  void setSubscriptionEnabled(std::string val) {
    m_poolFactory->setSubscriptionEnabled(FwkStrCvt::toBool(val));
  }

  void setSubscriptionRedundancy(std::string val) {
    m_poolFactory->setSubscriptionRedundancy(FwkStrCvt::toInt32(val));
  }

  void setSubscriptionMessageTrackingTimeout(std::string val) {
    m_poolFactory->setSubscriptionMessageTrackingTimeout(
        FwkStrCvt::toInt32(val));
  }

  void setSubscriptionAckInterval(std::string val) {
    m_poolFactory->setSubscriptionAckInterval(FwkStrCvt::toInt32(val));
  }

  void setThreadLocalConnections(std::string val) {
    m_poolFactory->setThreadLocalConnections(FwkStrCvt::toBool(val));
  }
  void setPRSingleHopEnabled(std::string val) {
    m_poolFactory->setPRSingleHopEnabled(FwkStrCvt::toBool(val));
  }

  void setLocatorsFlag(std::string val) { m_locators = FwkStrCvt::toBool(val); }

  void setServersFlag(std::string val) { m_servers = FwkStrCvt::toBool(val); }

 public:
  FwkPool(const DOMNode* node);
  ~FwkPool() {
    if (m_poolFactory != NULLPTR) {
      // TODO:Close factory
    }
  }
  bool isPoolWithLocators() { return m_locators; }
  bool isPoolWithServers() { return m_servers; }

  void addLocator(std::string ep) {
    size_t position = ep.find_first_of(":");
    if (position != std::string::npos) {
      std::string hostname = ep.substr(0, position);
      int portnumber = atoi((ep.substr(position + 1)).c_str());
      m_poolFactory->addLocator(hostname.c_str(), portnumber);
    }
  }

  void addServer(std::string ep) {
    FWKINFO("GG: Adding Server EP:" << ep);
    size_t position = ep.find_first_of(":");
    if (position != std::string::npos) {
      std::string hostname = ep.substr(0, position);
      int portnumber = atoi((ep.substr(position + 1)).c_str());
      m_poolFactory->addServer(hostname.c_str(), portnumber);
    }
  }

  PoolPtr createPoolForPerf() { return m_poolFactory->create(m_name.c_str()); }

  PoolPtr createPool() const {
    if (m_name.empty()) {
      FWKEXCEPTION("Pool name not specified.");
    } else {
      return m_poolFactory->create(m_name.c_str());
    }
    return NULLPTR;
  }
  const std::string& getName() const { return m_name; }
  void print() const { FWKINFO("FwkPool " << m_name); }
};

// ----------------------------------------------------------------------------

class DataSnippet {
  std::string m_name;
  FwkRegion* m_region;
  FwkPool* m_pool;

  void setName(std::string name) { m_name = name; }
  void setRegion(FwkRegion* region) { m_region = region; }
  void setPool(FwkPool* pool) { m_pool = pool; }

 public:
  DataSnippet(const DOMNode* node);
  ~DataSnippet() {
    if (m_region != NULL) {
      delete m_region;
      m_region = NULL;
    }
    if (m_pool != NULL) {
      delete m_pool;
      m_pool = NULL;
    }
  }

  std::string& getName() { return m_name; }
  const FwkRegion* getRegion() const { return m_region; }
  const FwkPool* getPool() const { return m_pool; }
  void print() const {
    FWKINFO("Snippet: " << m_name << " has region: ");
    m_region->print();
  }
};

// ----------------------------------------------------------------------------

class DataOneof {
  StringVector m_values;
  std::string m_empty;

 public:
  DataOneof(const DOMNode* node);

  ~DataOneof() { m_values.clear(); }

  const std::string& getValue() const {
    int32_t maxIdx = static_cast<int32_t>(m_values.size()) - 1;
    if (maxIdx < 0) {
      return m_empty;
    }
    return m_values.at(GsRandom::random(maxIdx));
  }

  void addValue(std::string val) { m_values.push_back(val); }
  void print() const {
    FWKINFO("DataOneof has values:");
    int32_t maxIdx = static_cast<int32_t>(m_values.size()) - 1;
    if (maxIdx < 0) {
      return;
    }
    for (int32_t idx = 0; idx <= maxIdx; idx++) {
      FWKINFO(m_values.at(idx));
    }
  }
};

// ----------------------------------------------------------------------------

typedef std::map<ACE_thread_t, int32_t> TlsMap;
typedef std::map<ACE_thread_t, int32_t>::const_iterator TlsMapIter;

class DataList {
  mutable TlsMap m_map;
  StringVector m_values;
  std::string m_empty;

  const std::string& getFirstListItem(ACE_thread_t tid) const {
    StringVector::const_iterator idx = m_values.begin();
    if (idx != m_values.end()) {
      m_map[tid] = 0;
      return (*idx);
    }
    return m_empty;
  }

  const std::string& getNextListItem(TlsMapIter iter) const {
    int32_t idx = (*iter).second;

    if (++idx >= (int32_t)m_values.size()) {
      idx = -1;
    }
    m_map[(*iter).first] = idx;
    if (idx > -1) {
      return m_values.at(idx);
    }
    return m_empty;
  }

 public:
  DataList(const DOMNode* node);

  ~DataList() {
    m_map.clear();
    m_values.clear();
  }

  const std::string& getValue() const {
    ACE_thread_t tid = ACE_OS::thr_self();
    TlsMapIter iter = m_map.find(tid);
    if (iter == m_map.end()) {
      // thread not in map, return the first list item
      return getFirstListItem(tid);
    }
    // We have an iterator, return the next in the list
    return getNextListItem(iter);
  }

  void reset() const { m_map.erase(ACE_OS::thr_self()); }
  void addValue(std::string val) { m_values.push_back(val); }
  void print() const {
    FWKINFO("DataList has values:");
    //        ACE_thread_t maxIdx = static_cast<ACE_thread_t> (m_values.size())
    //        - 1;
    //        if ( maxIdx < 0 ) {
    //          return;
    //        }
    //        for ( ACE_thread_t idx = 0; idx <= maxIdx; idx++ ) {
    //          FWKINFO( m_values.at( idx ) );
    //        }
  }
};

// ----------------------------------------------------------------------------

class DataRange {
  double m_low;
  double m_high;
  std::string m_last;

  void setLow(std::string str) { m_low = FwkStrCvt::toDouble(str); }
  void setHigh(std::string str) { m_high = FwkStrCvt::toDouble(str); }
  void setLow(double low) { m_low = low; }
  void setHigh(double high) { m_high = high; }

 public:
  DataRange(const DOMNode* node);

  const std::string getValue() const {
    return FwkStrCvt(GsRandom::random(m_low, m_high)).asString();
  }
  void print() const {
    FWKINFO("DataRange: low = " << m_low << "  high = " << m_high);
  }
};

// ----------------------------------------------------------------------------

/**
 * @class FwkData
 * @brief Framework Data object
 */
class FwkData : public FwkObject {
 public:
  FwkData(const DOMNode* node);

  ~FwkData() {
    if (m_dataList != NULL) {
      delete m_dataList;
      m_dataList = NULL;
    }
    if (m_dataOneof != NULL) {
      delete m_dataOneof;
      m_dataOneof = NULL;
    }
    if (m_dataRange != NULL) {
      delete m_dataRange;
      m_dataRange = NULL;
    }
    if (m_snippet != NULL) {
      delete m_snippet;
      m_snippet = NULL;
    }
  }

  void setList(const DataList* dataList) {
    m_dataList = dataList;
    m_dataType = DATA_TYPE_LIST;
  }

  void setOneof(const DataOneof* dataOneof) {
    m_dataOneof = dataOneof;
    m_dataType = DATA_TYPE_ONEOF;
  }

  void setContent(std::string content) {
    m_content = content;
    m_dataType = DATA_TYPE_CONTENT;
  }

  void setRange(DataRange* range) {
    m_dataRange = range;
    m_dataType = DATA_TYPE_RANGE;
  }

  void setSnippet(DataSnippet* snippet) {
    m_snippet = snippet;
    m_dataType = DATA_TYPE_SNIPPET;
  }

  const FwkRegion* getSnippet() const {
    if (m_dataType == DATA_TYPE_SNIPPET) {
      return m_snippet->getRegion();
    }
    return NULL;
  }

  const FwkPool* getPoolSnippet() const {
    if (m_dataType == DATA_TYPE_SNIPPET) {
      return m_snippet->getPool();
    }
    return NULL;
  }

  const std::string getValue() const {
    switch (m_dataType) {
      case DATA_TYPE_LIST:
        return m_dataList->getValue();
        break;
      case DATA_TYPE_ONEOF:
        return m_dataOneof->getValue();
        break;
      case DATA_TYPE_RANGE:
        return m_dataRange->getValue();
        break;
      case DATA_TYPE_SNIPPET:
        break;
      default:
        break;
    }
    return m_content;
  }

  const std::string& getContent() const { return m_content; }

  void reset() const {
    if (DATA_TYPE_LIST == m_dataType) {
      m_dataList->reset();
    }
  }

  virtual const std::string& getKey() const { return getName(); }

  void print() const {
    FWKINFO("Data: " << getName() << " has content: ");
    switch (m_dataType) {
      case DATA_TYPE_LIST:
        m_dataList->print();
        break;
      case DATA_TYPE_ONEOF:
        m_dataOneof->print();
        break;
      case DATA_TYPE_RANGE:
        m_dataRange->print();
        break;
      case DATA_TYPE_SNIPPET:
        m_snippet->print();
        break;
      default:
        FWKINFO("Content: " << m_content);
        break;
    }
  }

 private:
  std::string m_content;
  const DataList* m_dataList;
  const DataOneof* m_dataOneof;
  const DataRange* m_dataRange;
  const DataSnippet* m_snippet;

  tFwkDataType m_dataType;
};

// ----------------------------------------------------------------------------

/**
  * @class FwkDataSet
  *
  * @brief Container to hold FwkData objects
  * @see FwkData
  */
class FwkDataSet : public TFwkSet<FwkData> {
 public:
  FwkDataSet() {}
  FwkDataSet(const DOMNode* node);
};

// ----------------------------------------------------------------------------

/**
  * @class FwkClient
  *
  * @brief FwkClient object
  */
class FwkClient : public FwkObject {
 public:
  FwkClient(const DOMNode* node);

  FwkClient(std::string name)
      : m_program(NULL), m_arguments(NULL), m_remaining(false) {
    setName(name);
  }

  ~FwkClient() {
    if (m_program != NULL) {
      free((void*)m_program);
      m_program = NULL;
    }
    if (m_arguments != NULL) {
      free((void*)m_arguments);
      m_arguments = NULL;
    }
  }

  void print() const { FWKINFO("Client: " << getName()); }

  void setProgram(std::string str) {
    if (!str.empty()) m_program = strdup(str.c_str());
  }

  void setArguments(std::string str) {
    if (!str.empty()) m_arguments = strdup(str.c_str());
  }

  void setHost(std::string str) {
    if ((m_host.empty()) && (!str.empty())) m_host = str;
  }

  void setHostGroup(const std::string& grp) { m_hostGroup = grp; }
  const std::string getHostGroup() const { return m_hostGroup; }

  void setRemaining(bool remaining) { m_remaining = remaining; }
  bool getRemaining() const { return m_remaining; }

  const char* getProgram() const { return m_program; }
  const char* getArguments() const { return m_arguments; }
  const std::string& getHost() const { return m_host; }
  virtual const std::string& getKey() const { return getName(); }

 private:
  const char* m_program;
  const char* m_arguments;
  std::string m_hostGroup;
  bool m_remaining;
  std::string m_host;
};

// ----------------------------------------------------------------------------

/**
  * @class FwkClientSet
  *
  * @brief Container to hold FwkClient objects
  * @see FwkClient
  */
class FwkClientSet : public TFwkSet<FwkClient> {
  bool m_exclude;
  int32_t m_count;
  int32_t m_begin;
  std::string m_hostGroup;
  bool m_remaining;

  void addClient(FwkClient* client) { add(client); }

  void setCount(std::string str) { m_count = FwkStrCvt::toInt32(str); }

  void setBegin(std::string str) { m_begin = FwkStrCvt::toInt32(str); }

 public:
  static const std::string m_defaultGroup;

  FwkClientSet()
      : m_exclude(false), m_count(1), m_begin(1), m_remaining(false) {}
  FwkClientSet(const DOMNode* node);

  void setExclude(bool exclude) { m_exclude = exclude; }

  void setExclude(std::string str) { m_exclude = FwkStrCvt::toBool(str); }

  void setHostGroup(const std::string str) { m_hostGroup = str; }

  bool getExclude() const { return m_exclude; }
  int32_t getCount() const { return m_count; }
  int32_t getBegin() const { return m_begin; }
  std::string getHostGroup() const { return m_hostGroup; }

  void setRemaining(bool remaining) { m_remaining = remaining; }
  void setRemaining(std::string str) { m_remaining = FwkStrCvt::toBool(str); }
  bool getRemaining() const { return m_remaining; }

  void print() const {
    FWKINFO("ClientSet exclude: " << getExclude() << "  count: " << getCount()
                                  << "  begin: " << getBegin());
    TFwkSet<FwkClient>::print();
  }
};

// ----------------------------------------------------------------------------

typedef int32_t (*FwkAction)(const char* taskId);

typedef std::list<uint32_t> TaskClientIdxList;

// ----------------------------------------------------------------------------
class FwkTest;

/**
  * @class FwkTask
  *
  * @brief FwkTask object
  */
class FwkTask : public FwkObject {
 public:
  FwkTask(const DOMNode* node);

  ~FwkTask() {
    if (m_clientSet != NULL) {
      delete m_clientSet;
      m_clientSet = NULL;
    }
    if (m_dataSet != NULL) {
      delete m_dataSet;
      m_dataSet = NULL;
    }
    m_clients.clear();
  }

  /** @brief Get number of times task has ran */
  uint32_t getTimesRan() const { return m_timesRan; }

  /** @brief Increment number of times task has ran */
  void incTimesRan() { m_timesRan++; }

  /** @brief reset number of times task has ran to zero*/
  void resetTimesRan() { m_timesRan = 0; }

  /** @brief Get number of threads task should run on */
  int32_t getThreadCount() const { return m_threadCount; }

  /** @brief Is this task to run if test has failures? */
  bool continueOnError() const { return m_continue; }

  /** @brief Get number of times to run task */
  uint32_t getTimesToRun() const { return m_timesToRun; }

  /** @brief Get action string */
  std::string getAction() const { return m_action; }

  /** @brief Get class string */
  std::string getClass() const { return m_class; }

  /** @brief Get container string */
  std::string getContainer() const { return m_container; }

  /** @brief Get seconds to wait */
  uint32_t getWaitTime() const { return m_waitTime; }

  /** @brief Is this task to run parallel with other tasks? */
  bool isParallel() const { return m_parallel; }

  /** brief Get FwkDataSet pointer */
  const FwkDataSet* getDataSet(const char* name) const;

  /** brief Get FwkData pointer */
  const FwkData* getData(const char* name) const;

  /** brief Get char pointer */
  const std::string getStringValue(const char* name) const {
    const FwkData* data = getData(name);
    if (data) {
      return data->getValue();
    }
    return m_empty;
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

  /** brief Get char pointer */
  const FwkRegion* getSnippet(const std::string& name) const {
    const FwkRegion* value = NULL;
    const FwkData* data = getData(name.c_str());
    if (data) {
      value = data->getSnippet();
    }
    return value;
  }

  const FwkPool* getPoolSnippet(const std::string& name) const {
    const FwkPool* value = NULL;
    const FwkData* data = getData(name.c_str());
    if (data) {
      value = data->getPoolSnippet();
    }
    return value;
  }

  void resetValue(const char* name) const {
    FwkData* data = (FwkData*)getData(name);
    if (data != NULL) data->reset();
  }

  TaskClientIdxList* getTaskClients() { return &m_clients; }

  const std::string& getTaskId() const { return m_id; }

  virtual const std::string& getKey() const { return m_id; }

  void setParent(FwkTest* parent) { m_parent = parent; }

  FwkClientSet* getClientSet() { return m_clientSet; }

  void print() const {
    FWKINFO("Task: " << m_id << "  " << m_container << "  " << m_class << "  "
                     << m_action << "  WaitTime: " << m_waitTime
                     << "  TimesToRun: " << m_timesToRun << "  Threads: "
                     << m_threadCount << "  Parallel: " << m_parallel
                     << "  ContinueOnError: " << m_continue);
    if (m_clientSet != NULL) {
      FWKINFO("Clients: ");
      m_clientSet->print();
    }
    if (m_dataSet != NULL) {
      FWKINFO("Data: ");
      m_dataSet->print();
    }
    FWKINFO("End Task: " << m_id);
  }

  void setKey(int32_t cnt);

  /** @brief Get number of times to run task */
  void setTimesToRun(uint32_t cnt) { m_timesToRun = cnt; }

 private:
  void setAction(std::string action) { m_action = action; }

  void setClass(std::string clas) { m_class = clas; }

  void setContainer(std::string container) { m_container = container; }

  void setWaitTime(std::string waitTime) {
    m_waitTime = FwkStrCvt::toSeconds(waitTime.c_str());
  }

  /** @brief Get number of times to run task */
  void setTimesToRun(std::string str) {
    m_timesToRun = FwkStrCvt::toUInt32(str);
  }

  void setParallel(std::string str) { m_parallel = FwkStrCvt::toBool(str); }

  void setContinueOnError(std::string str) {
    m_continue = FwkStrCvt::toBool(str);
  }

  /** @brief Set number of threads task should run on */
  void setThreadCount(std::string str) {
    m_threadCount = FwkStrCvt::toInt32(str);
  }

  void setDataSet(FwkDataSet* dataSet) {
    if (m_dataSet == NULL) m_dataSet = dataSet;
  }

  /** @brief Add ClientSet
    * @param set ClientSet to add
    */
  void addClientSet(FwkClientSet* set) {
    const FwkClient* client = set->getFirst();
    while (client != NULL) {
      addClient(client);
      client = set->getNext(client);
    }
    if (set->getExclude()) {
      m_clientSet->setExclude(true);
    }
  }

  void addClient(const FwkClient* client) {
    if (m_clientSet == NULL) {
      m_clientSet = new FwkClientSet();
    }
    m_clientSet->add(client);
  }

  void addData(FwkData* data) {
    if (m_dataSet == NULL) {
      m_dataSet = new FwkDataSet();
    }
    m_dataSet->add(data);
  }

  std::string m_action;
  std::string m_class;
  std::string m_container;
  // UNUSED FwkAction m_func;
  uint32_t m_waitTime;
  uint32_t m_timesToRun;
  int32_t m_threadCount;
  bool m_parallel;
  std::string m_id;
  uint32_t m_timesRan;
  bool m_continue;

  FwkClientSet* m_clientSet;
  FwkDataSet* m_dataSet;

  FwkTest* m_parent;

  TaskClientIdxList m_clients;

  std::string m_empty;
};

// ----------------------------------------------------------------------------

/**
* @class FwkTaskSet
*
* @brief Container to hold FwkTask objects
* @see FwkTask
*/
class FwkTaskSet : public TFwkSet<FwkTask> {};

// ----------------------------------------------------------------------------
class TestDriver;

/**
  * @class FwkTest
  *
  * @brief FwkTest object
  */
class FwkTest : public FwkObject {
 public:
  FwkTest(const DOMNode* node);

  /** brief Get FwkDataSet pointer */
  const FwkDataSet* getDataSet(const char* name) const;

  /** brief Get FwkData pointer */
  const FwkData* getData(const char* name) const;

  /** @brief Get description string */
  const char* getDescription() const { return m_description.c_str(); }

  /** @brief Get wait time */
  uint32_t getWaitTime() const { return m_waitTime; }

  /** @brief Get number of times to run task */
  uint32_t getTimesToRun() const { return m_timesToRun; }

  const FwkTask* getFirst() { return m_taskSet.getFirst(); }

  const FwkTask* getNext(const FwkTask* prev) {
    return m_taskSet.getNext(prev);
  }

  const FwkTask* getTaskById(std::string& id) {
    FwkTask* task = (FwkTask*)m_taskSet.getFirst();
    while (task != NULL) {
      if (task->getTaskId() == id) {
        return task;
      }
      task = (FwkTask*)m_taskSet.getNext(task);
    }
    return task;
  }

  const FwkTask* find(const char* name) { return m_taskSet.find(name); }

  size_t getCount() const { return m_taskSet.size(); }

  virtual const std::string& getKey() const { return m_id; }

  void print() const {
    FWKINFO("Test: " << m_id << "  " << m_description
                     << "  WaitTime: " << m_waitTime);
    FWKINFO("Tasks: ");
    m_taskSet.print();
    FWKINFO("End Test: " << m_id);
  }

  void setParent(TestDriver* parent) { m_parent = parent; }

  void setKey(int32_t cnt) {
    if (!m_id.empty()) return;
    std::ostringstream ostr;
    ostr << getName() << "::" << cnt;
    m_id = ostr.str();
  }

  /** @brief Get number of times to run task */
  void setTimesToRun(std::string str) {
    m_timesToRun = FwkStrCvt::toUInt32(str);
  }

 private:
  void setWaitTime(std::string waitTime) {
    m_waitTime = FwkStrCvt::toSeconds(waitTime);
  }

  void setDescription(std::string description) { m_description = description; }

  void addTask(FwkTask* task) {
    m_taskSet.add(task);
    task->setParent(this);
  }

  std::string m_id;
  std::string m_description;
  uint32_t m_waitTime;
  uint32_t m_timesToRun;
  FwkTaskSet m_taskSet;
  TestDriver* m_parent;
};

// ----------------------------------------------------------------------------

/**
* @class FwkTestSet
*
* @brief Container to hold FwkTest objects
* @see FwkTest
*/
class FwkTestSet : public TFwkSet<FwkTest> {};

// ----------------------------------------------------------------------------

class LocalFile : public FwkObject {
  std::string m_description;
  std::string m_content;
  bool m_append;

  void setContent(std::string content) { m_content = content; }
  void setDescription(std::string description) { m_description = description; }
  void setAppend(std::string append) { m_append = FwkStrCvt::toBool(append); }

 public:
  LocalFile(const DOMNode* node);

  const std::string& getContent() const { return m_content; }

  const std::string& getKey() const { return getName(); }

  bool getAppend() const { return m_append; }

  void print() const {
    FWKINFO("LocalFile: " << getName() << " append: " << getAppend()
                          << " has content: " << getContent());
  }
};

// ----------------------------------------------------------------------------

class LocalFileSet : public TFwkSet<LocalFile> {};

// ----------------------------------------------------------------------------

class FwkDomErrorHandler : public DOMErrorHandler {
 public:
  FwkDomErrorHandler() : m_hadErrors(false) {}
  ~FwkDomErrorHandler() {
    if (m_hadErrors) {
      FWKEXCEPTION("Encountered errors during parse.");
    }
  }

  bool hadErrors() const { return m_hadErrors; }
  bool handleError(const DOMError& domError);
  void resetErrors() { m_hadErrors = false; }

 private:
  bool m_hadErrors;
};

// ----------------------------------------------------------------------------

class TestDriver {
  LocalFileSet m_localFiles;
  FwkDataSet m_data;
  std::map<std::string, FwkDataSet*> m_dataSetMap;
  std::vector<FwkClientSet*> m_clientSets;
  FwkClientSet m_clients;
  FwkTestSet m_tests;
  FwkTaskSet m_tasks;
  std::string m_empty;
  StringVector m_hostGroups;

  void addHostGroup(std::string name) { m_hostGroups.push_back(name); }

  void addLocalFile(LocalFile* file) { m_localFiles.add(file); }

  void addData(FwkData* data) { m_data.add(data); }

  void addDataSet(FwkDataSet* dataSet) {
    m_dataSetMap[dataSet->getName()] = dataSet;
  }

  void addClientSet(FwkClientSet* clientSet) {
    m_clientSets.push_back(clientSet);
    FwkClient* client = (FwkClient*)clientSet->getFirst();
    std::string grp = clientSet->getHostGroup();
    bool remaining = clientSet->getRemaining();
    while (client != NULL) {
      client->setHostGroup(grp);
      client->setRemaining(remaining);
      m_clients.add(client);
      client = (FwkClient*)clientSet->getNext(client);
    }
  }

  void addTest(FwkTest* test) {
    m_tests.add(test);
    test->setParent(this);
    test->setKey(m_tests.size());
    const FwkTask* task = test->getFirst();
    int32_t cnt = 1;
    while (task != NULL) {
      ((FwkTask*)task)->setKey(cnt++);
      m_tasks.add(task);
      task = test->getNext(task);
    }
  }

  void clearClientSets() {
    int32_t siz = static_cast<int32_t>(m_clientSets.size());
    while (siz > 0) {
      const FwkClientSet* obj = m_clientSets.back();
      m_clientSets.pop_back();
      delete obj;
      siz = static_cast<int32_t>(m_clientSets.size());
    }
  }

  void clearDataSets() {
    std::map<std::string, FwkDataSet*>::iterator iter;
    iter = m_dataSetMap.begin();
    while (iter != m_dataSetMap.end()) {
      const FwkDataSet* obj = (*iter).second;
      delete obj;
      m_dataSetMap.erase(iter);
      iter = m_dataSetMap.begin();
    }
  }

  void writeLocalFile(const LocalFile* fil) {
    const char* mode;
    std::string what("write");
    if (fil->getAppend()) {
      mode = "ab";
      what = "append";
    } else {
      mode = "wb";
    }
    errno = 0;
    FILE* out = fopen(fil->getName().c_str(), mode);
    if (out == (FILE*)0) {
      int32_t err = errno;
      FWKEXCEPTION("Unable to open file " << fil->getName() << " with mode: "
                                          << what << " error: " << err);
    }
    FWKDEBUG("Opened local file: " << fil->getName() << " to " << what
                                   << " content::>" << fil->getContent()
                                   << "<::");
    fprintf(out, "%s", fil->getContent().c_str());
    fclose(out);
  }

 public:
  TestDriver(const char* file);
  ~TestDriver() {
    m_clients.setNoDelete();
    m_tasks.setNoDelete();
    clearClientSets();
    clearDataSets();
  }

  void fromXmlNode(const DOMNode* node);

  void writeFiles() {
    const LocalFile* fil = m_localFiles.getFirst();
    //        FILE * out = fopen( "local.files", "wb" );
    while (fil != NULL) {
      writeLocalFile(fil);
      //          fprintf( out, "%s\n", fil->getName().c_str() );
      fil = m_localFiles.getNext(fil);
    }
    //        fclose( out );
  }

  FwkClientSet* getClients() { return &m_clients; }

  const FwkTask* getTaskById(std::string id) {
    const FwkTask* task = m_tasks.getFirst();
    while (task != NULL) {
      if (id == task->getTaskId()) return task;
      task = m_tasks.getNext(task);
    }
    return NULL;
  }

  const StringVector& getHostGroups() { return m_hostGroups; }

  const FwkTest* nextTest(const FwkTest* test) const {
    return m_tests.getNext(test);
  }

  const FwkTest* firstTest() const { return m_tests.getFirst(); }

  const LocalFile* nextLocalFile(const LocalFile* file) const {
    return m_localFiles.getNext(file);
  }

  const LocalFile* firstLocalFile() const { return m_localFiles.getFirst(); }

  /** brief Get FwkDataSet pointer */
  const FwkDataSet* getDataSet(const char* name) const {
    if (name == NULL) return NULL;

    std::map<std::string, FwkDataSet*>::const_iterator iter =
        m_dataSetMap.find(name);

    if (iter != m_dataSetMap.end()) {
      return (*iter).second;
    }
    return NULL;
  }

  /** brief Get FwkData pointer */
  const FwkData* getData(const char* name) const { return m_data.find(name); }

  //      /** brief Get char pointer */
  //      const char * getValue( const char * name ) const {
  //        const char * value = NULL;
  //        const FwkData * data = getData( name );
  //        if ( data ) {
  //          std::string val = data->getValue();
  //          if ( !val.empty() ) {
  //            value = val.c_str();
  //          }
  //        }
  //        return value;
  //      }

  /** brief Get char pointer */
  const std::string getStringValue(const char* name) const {
    const FwkData* data = getData(name);
    if (data) {
      return data->getValue();
    }
    return m_empty;
  }

  /** brief Get int32_t seconds in time string */
  int32_t getTimeValue(const char* name) const {
    return FwkStrCvt::toSeconds(getStringValue(name));
  }

  /** brief Get int32_t */
  int32_t getIntValue(const char* name) const {
    return FwkStrCvt::toInt32(getStringValue(name));
  }

  void print() const {
    FWKINFO("TestDriver:   LocalFiles:");
    m_localFiles.print();
    FWKINFO("TestDriver:   Data:");
    m_data.print();
    FWKINFO("TestDriver:   DataSets: <skipping>");
    FWKINFO("TestDriver:   Clients:");
    m_clients.print();
    FWKINFO("TestDriver:   Tests:");
    m_tests.print();
  }
};

// ----------------------------------------------------------------------------

}  // namespace testframework
}  // namepace gemfire

#endif  // __FWK_OBJECTS_HPP__
