#ifndef __GEMFIRE_CACHEXMLPARSER_HPP__
#define __GEMFIRE_CACHEXMLPARSER_HPP__
/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

#include <stack>
#include <map>
#include <libxml/parser.h>
#include <gfcpp/Cache.hpp>
#include <gfcpp/CacheAttributes.hpp>
#include "CacheXml.hpp"
#include "RegionXmlCreation.hpp"
#include "CacheXmlCreation.hpp"
#include <gfcpp/ExpirationAction.hpp>
#include <gfcpp/ExpirationAttributes.hpp>
#include <gfcpp/CacheLoader.hpp>
#include <gfcpp/CacheListener.hpp>
#include <gfcpp/PartitionResolver.hpp>
#include <gfcpp/ExceptionTypes.hpp>
#include <gfcpp/RegionShortcut.hpp>

namespace gemfire {

// Factory function typedefs to register the managed
// cacheloader/writer/listener/resolver
typedef CacheLoader* (*LibraryCacheLoaderFn)(const char* assemblyPath,
                                             const char* factFuncName);
typedef CacheListener* (*LibraryCacheListenerFn)(const char* assemblyPath,
                                                 const char* factFuncName);
typedef PartitionResolver* (*LibraryPartitionResolverFn)(
    const char* assemblyPath, const char* factFuncName);
typedef CacheWriter* (*LibraryCacheWriterFn)(const char* assemblyPath,
                                             const char* factFuncName);
typedef PersistenceManager* (*LibraryPersistenceManagerFn)(
    const char* assemblyPath, const char* factFuncName);

class CPPCACHE_EXPORT CacheXmlParser : public CacheXml {
 private:
  std::stack<void*> _stack;
  xmlSAXHandler m_saxHandler;
  CacheXmlCreation* m_cacheCreation;
  std::string m_error;
  int32_t m_nestedRegions;
  PropertiesPtr m_config;
  std::string m_parserMessage;
  bool m_flagCacheXmlException;
  bool m_flagIllegalStateException;
  bool m_flagAnyOtherException;
  bool m_flagExpirationAttribute;
  std::map<std::string, RegionAttributesPtr> namedRegions;
  PoolFactory* m_poolFactory;

  /** Pool helper */
  void setPoolInfo(PoolFactory* poolFactory, const char* name,
                   const char* value);

  void handleParserErrors(int res);

 public:
  CacheXmlParser();
  ~CacheXmlParser();
  static CacheXmlParser* parse(const char* cachexml);
  void parseFile(const char* filename);
  void parseMemory(const char* buffer, int size);
  void setAttributes(Cache* cache);
  void create(Cache* cache);
  void endRootRegion();
  void endSubregion();
  void endRegion(bool isRoot);
  void startExpirationAttributes(const xmlChar** atts);
  void startPersistenceManager(const xmlChar** atts);
  void startPersistenceProperties(const xmlChar** atts);
  void startRegionAttributes(const xmlChar** atts);
  void startRootRegion(const xmlChar** atts);
  void startSubregion(const xmlChar** atts);
  void startPdx(const xmlChar** atts);
  void endPdx();
  void startRegion(const xmlChar** atts, bool isRoot);
  void startCache(void* ctx, const xmlChar** atts);
  void endCache();
  void startCacheLoader(const xmlChar** atts);
  void startCacheListener(const xmlChar** atts);
  void startPartitionResolver(const xmlChar** atts);
  void startCacheWriter(const xmlChar** atts);
  void endEntryIdleTime();
  void endEntryTimeToLive();
  void endRegionIdleTime();
  void endRegionTimeToLive();
  void endRegionAttributes();
  void endPersistenceManager();
  void setError(const std::string& s);
  const std::string& getError() const;
  void incNesting() { m_nestedRegions++; }
  void decNesting() { m_nestedRegions--; }
  bool isRootLevel() { return (m_nestedRegions == 1); }

  /** Pool handlers */
  void startPool(const xmlChar** atts);
  void endPool();
  void startLocator(const xmlChar** atts);
  void startServer(const xmlChar** atts);

  // getters/setters for flags and other members
  inline bool isCacheXmlException() const { return m_flagCacheXmlException; }

  inline void setCacheXmlException() { m_flagCacheXmlException = true; }

  inline bool isIllegalStateException() const {
    return m_flagIllegalStateException;
  }

  inline void setIllegalStateException() { m_flagIllegalStateException = true; }

  inline bool isAnyOtherException() const { return m_flagAnyOtherException; }

  inline void setAnyOtherException() { m_flagAnyOtherException = true; }

  inline bool isExpirationAttribute() const {
    return m_flagExpirationAttribute;
  }

  inline void setExpirationAttribute() { m_flagExpirationAttribute = true; }

  inline const std::string& getParserMessage() const { return m_parserMessage; }

  inline void setParserMessage(const std::string& str) {
    m_parserMessage = str;
  }

  // hooks for .NET managed cache listener/loader/writers
  static LibraryCacheLoaderFn managedCacheLoaderFn;
  static LibraryCacheListenerFn managedCacheListenerFn;
  static LibraryPartitionResolverFn managedPartitionResolverFn;
  static LibraryCacheWriterFn managedCacheWriterFn;
  static LibraryPersistenceManagerFn managedPersistenceManagerFn;
};

};      // namespace gemfire
#endif  // ifndef __GEMFIRE_CACHEXMLPARSER_HPP__
