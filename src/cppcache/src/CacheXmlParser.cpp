/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
#include "CacheXmlParser.hpp"
#include "CacheRegionHelper.hpp"
#include <gfcpp/PoolManager.hpp>
#include <gfcpp/PoolFactory.hpp>
#include "AutoDelete.hpp"
#include "CacheImpl.hpp"

#if defined(_WIN32)
#include <windows.h>
#else
#include <dlfcn.h>
#endif

using namespace gemfire;

namespace gemfire_impl {
void* getFactoryFunc(const char* lib, const char* funcName);
}

using namespace gemfire_impl;

namespace {
std::vector<std::pair<std::string, int>> parseEndPoints(
    const std::string& str) {
  std::vector<std::pair<std::string, int>> endPoints;
  std::string::size_type start = 0;
  std::string::size_type pos = str.find_first_of(',');
  while (std::string::npos != pos) {
    const std::string endPoint(str.substr(start, pos - start));
    const std::string::size_type split = endPoint.find_last_of(':');
    if (std::string::npos == split) {
      endPoints.push_back(std::pair<std::string, int>(endPoint, 0));
    } else {
      int port = 0;
      try {
        port = std::stoi(endPoint.substr(split + 1));
      } catch (...) {
        // NOP
      }
      endPoints.push_back(
          std::pair<std::string, int>(endPoint.substr(0, split), port));
    }
    start = pos + 1;
    pos = str.find_first_of(',', start);
  }
  return endPoints;
}
}  // namespace

/////////////////XML Parser Callback functions////////////////////////

extern "C" void startElementSAX2Function(void* ctx, const xmlChar* name,
                                         const xmlChar** atts) {
  CacheXmlParser* parser = (CacheXmlParser*)ctx;
  if (!parser) {
    Log::error("CacheXmlParser::startElementSAX2Function:Parser is NULL");
  }

  if ((!parser->isCacheXmlException()) &&
      (!parser->isIllegalStateException()) &&
      (!parser->isAnyOtherException())) {
    try {
      if (strcmp((char*)name, parser->CACHE) == 0) {
        parser->startCache(ctx, atts);
      } else if (strcmp((char*)name, parser->CLIENT_CACHE) == 0) {
        parser->startCache(ctx, atts);
      } else if (strcmp((char*)name, parser->PDX) == 0) {
        parser->startPdx(atts);
      } else if (strcmp((char*)name, parser->REGION) == 0) {
        parser->incNesting();
        parser->startRegion(atts, parser->isRootLevel());
      } else if (strcmp((char*)name, parser->ROOT_REGION) == 0) {
        parser->incNesting();
        parser->startRegion(atts, parser->isRootLevel());
      } else if (strcmp((char*)name, parser->REGION_ATTRIBUTES) == 0) {
        parser->startRegionAttributes(atts);
      } else if (strcmp((char*)name, parser->REGION_TIME_TO_LIVE) == 0) {
      } else if (strcmp((char*)name, parser->REGION_IDLE_TIME) == 0) {
      } else if (strcmp((char*)name, parser->ENTRY_TIME_TO_LIVE) == 0) {
      } else if (strcmp((char*)name, parser->ENTRY_IDLE_TIME) == 0) {
      } else if (strcmp((char*)name, parser->EXPIRATION_ATTRIBUTES) == 0) {
        parser->startExpirationAttributes(atts);
      } else if (strcmp((char*)name, parser->CACHE_LOADER) == 0) {
        parser->startCacheLoader(atts);
      } else if (strcmp((char*)name, parser->CACHE_WRITER) == 0) {
        parser->startCacheWriter(atts);
      } else if (strcmp((char*)name, parser->CACHE_LISTENER) == 0) {
        parser->startCacheListener(atts);
      } else if (strcmp((char*)name, parser->PARTITION_RESOLVER) == 0) {
        parser->startPartitionResolver(atts);
      } else if (strcmp((char*)name, parser->PERSISTENCE_MANAGER) == 0) {
        parser->startPersistenceManager(atts);
      } else if (strcmp((char*)name, parser->PROPERTIES) == 0) {
      } else if (strcmp((char*)name, parser->PROPERTY) == 0) {
        parser->startPersistenceProperties(atts);
      } else if (strcmp((char*)name, parser->POOL) == 0) {
        parser->startPool(atts);
      } else if (strcmp((char*)name, parser->LOCATOR) == 0) {
        parser->startLocator(atts);
      } else if (strcmp((char*)name, parser->SERVER) == 0) {
        parser->startServer(atts);
      } else {
        std::string temp((char*)name);
        std::string s = "XML:Unknown XML element \"" + temp + "\"";
        throw CacheXmlException(s.c_str());
      }
    } catch (const CacheXmlException& e) {
      parser->setCacheXmlException();
      std::string s = e.getMessage();
      parser->setError(s);
    } catch (const IllegalStateException& ex) {
      parser->setIllegalStateException();
      std::string s = ex.getMessage();
      parser->setError(s);
    } catch (const Exception& ex) {
      parser->setAnyOtherException();
      std::string s = ex.getMessage();
      parser->setError(s);
    }
  }  // flag
}

extern "C" void endElementSAX2Function(void* ctx, const xmlChar* name) {
  CacheXmlParser* parser = (CacheXmlParser*)ctx;
  if (!parser) {
    Log::error(
        "Error occured while xml parsing: "
        "CacheXmlParser:startElementSAX2Function:Parser is NULL");
    return;
  }

  if ((!parser->isCacheXmlException()) &&
      (!parser->isIllegalStateException()) &&
      (!parser->isAnyOtherException())) {
    try {
      if (strcmp((char*)name, parser->CACHE) == 0) {
        parser->endCache();
      } else if (strcmp((char*)name, parser->CLIENT_CACHE) == 0) {
        parser->endCache();
      } else if (strcmp((char*)name, parser->PDX) == 0) {
        parser->endPdx();
      } else if (strcmp((char*)name, parser->REGION) == 0) {
        parser->endRegion(parser->isRootLevel());
        parser->decNesting();
      } else if (strcmp((char*)name, parser->ROOT_REGION) == 0) {
        parser->endRegion(parser->isRootLevel());
        parser->decNesting();
      } else if (strcmp((char*)name, parser->REGION_ATTRIBUTES) == 0) {
        parser->endRegionAttributes();
      } else if (strcmp((char*)name, parser->REGION_TIME_TO_LIVE) == 0) {
        parser->endRegionTimeToLive();
      } else if (strcmp((char*)name, parser->REGION_IDLE_TIME) == 0) {
        parser->endRegionIdleTime();
      } else if (strcmp((char*)name, parser->ENTRY_TIME_TO_LIVE) == 0) {
        parser->endEntryTimeToLive();
      } else if (strcmp((char*)name, parser->ENTRY_IDLE_TIME) == 0) {
        parser->endEntryIdleTime();
      } else if (strcmp((char*)name, parser->EXPIRATION_ATTRIBUTES) == 0) {
      } else if (strcmp((char*)name, parser->CACHE_LOADER) == 0) {
      } else if (strcmp((char*)name, parser->CACHE_WRITER) == 0) {
      } else if (strcmp((char*)name, parser->CACHE_LISTENER) == 0) {
      } else if (strcmp((char*)name, parser->PARTITION_RESOLVER) == 0) {
      } else if (strcmp((char*)name, parser->PERSISTENCE_MANAGER) == 0) {
        parser->endPersistenceManager();
      } else if (strcmp((char*)name, parser->PROPERTIES) == 0) {
      } else if (strcmp((char*)name, parser->PROPERTY) == 0) {
      } else if (strcmp((char*)name, parser->POOL) == 0) {
        parser->endPool();
      } else if (strcmp((char*)name, parser->LOCATOR) == 0) {
        // parser->endLocator();
      } else if (strcmp((char*)name, parser->SERVER) == 0) {
        // parser->endServer();
      } else {
        std::string temp((char*)name);
        std::string s = "XML:Unknown XML element \"" + temp + "\"";
        throw CacheXmlException(s.c_str());
      }
    } catch (CacheXmlException& e) {
      parser->setCacheXmlException();
      std::string s = e.getMessage();
      parser->setError(s);
    } catch (IllegalStateException& ex) {
      parser->setIllegalStateException();
      std::string s = ex.getMessage();
      parser->setError(s);
    } catch (Exception& ex) {
      parser->setAnyOtherException();
      std::string s = ex.getMessage();
      parser->setError(s);
    }
  }  // flag
}

/**
 * warningDebug:
 * @ctxt:  An XML parser context
 * @msg:  the message to display/transmit
 * @...:  extra parameters for the message display
 *
 * Display and format a warning messages, gives file, line, position and
 * extra parameters.
 */
extern "C" void warningDebug(void* ctx, const char* msg, ...) {
  char logmsg[2048];
  va_list args;
  va_start(args, msg);
  vsprintf(logmsg, msg, args);
  va_end(args);
  LOGWARN("SAX.warning during XML declarative client initialization: %s",
          logmsg);
}

/**
 * fatalErrorDebug:
 * @ctxt:  An XML parser context
 * @msg:  the message to display/transmit
 * @...:  extra parameters for the message display
 *
 * Display and format a fatalError messages, gives file, line, position and
 * extra parameters.
 */
extern "C" void fatalErrorDebug(void* ctx, const char* msg, ...) {
  char buf[1024];
  va_list args;

  va_start(args, msg);
  CacheXmlParser* parser = (CacheXmlParser*)ctx;
  vsprintf(buf, msg, args);
  std::string stringMsg(buf);
  parser->setParserMessage(parser->getParserMessage() + stringMsg);
  va_end(args);
}

/////////////End of XML Parser Cackllback functions///////////////

///////////////static variables of the class////////////////////////

LibraryCacheLoaderFn CacheXmlParser::managedCacheLoaderFn = NULL;
LibraryCacheListenerFn CacheXmlParser::managedCacheListenerFn = NULL;
LibraryPartitionResolverFn CacheXmlParser::managedPartitionResolverFn = NULL;
LibraryCacheWriterFn CacheXmlParser::managedCacheWriterFn = NULL;
LibraryPersistenceManagerFn CacheXmlParser::managedPersistenceManagerFn = NULL;

//////////////////////////////////////////////////////////////////

CacheXmlParser::CacheXmlParser()
    : m_cacheCreation(NULL),
      m_nestedRegions(0),
      m_config(NULLPTR),
      m_parserMessage(""),
      m_flagCacheXmlException(false),
      m_flagIllegalStateException(false),
      m_flagAnyOtherException(false),
      m_flagExpirationAttribute(false),
      m_poolFactory(NULL) {
  static xmlSAXHandler saxHandler = {
      NULL,                     /* internalSubset */
      NULL,                     /* isStandalone */
      NULL,                     /* hasInternalSubset */
      NULL,                     /* hasExternalSubset */
      NULL,                     /* resolveEntity */
      NULL,                     /* getEntity */
      NULL,                     /* entityDecl */
      NULL,                     /* notationDecl */
      NULL,                     /* attributeDecl */
      NULL,                     /* elementDecl */
      NULL,                     /* unparsedEntityDecl */
      NULL,                     /* setDocumentLocator */
      NULL,                     /* startDocument */
      NULL,                     /* endDocument */
      startElementSAX2Function, /* startElement */
      endElementSAX2Function,   /* endElement */
      NULL,                     /* reference */
      NULL,                     /* characters */
      NULL,                     /* ignorableWhitespace */
      NULL,                     /* processingInstruction */
      NULL,                     // commentDebug, /* comment */
      warningDebug,             /* xmlParserWarning */
      fatalErrorDebug,          /* xmlParserError */
      NULL,                     /* xmlParserError */
      NULL,                     /* getParameterEntity */
      NULL,                     /* cdataBlock; */
      NULL,                     /* externalSubset; */
      XML_SAX2_MAGIC,
      NULL,
      NULL, /* startElementNs */
      NULL, /* endElementNs */
      NULL  /* xmlStructuredErrorFunc */
  };

  m_saxHandler = saxHandler;

  namedRegions = CacheImpl::getRegionShortcut();
}

void CacheXmlParser::parseFile(const char* filename) {
  int res = 0;

  res = xmlSAXUserParseFile(&this->m_saxHandler, this, filename);

  if (res == -1) {
    std::string temp(filename);
    std::string tempString = "Xml file " + temp + " not found\n";
    throw CacheXmlException(tempString.c_str());
  }
  handleParserErrors(res);
}

void CacheXmlParser::parseMemory(const char* buffer, int size) {
  int res = 0;
  res = xmlSAXUserParseMemory(&this->m_saxHandler, this, buffer, size);

  if (res == -1) {
    throw CacheXmlException("Unable to read buffer.");
  }
  handleParserErrors(res);
}

void CacheXmlParser::handleParserErrors(int res) {
  if (res != 0)  // xml file is not well-formed
  {
    char buf[256];
    ACE_OS::snprintf(buf, 256, "Error code returned by xml parser is : %d ",
                     res);
    Log::error(buf);

    std::string temp =
        "Xml file is not well formed. Error _stack: \n" + this->m_parserMessage;
    throw CacheXmlException(temp.c_str());
  }
  std::string temp = this->getError();
  if (temp == "") {
    Log::info("Xml file parsed successfully");
  } else  // well formed, but not according to our specs(dtd errors thrown
          // manually)
  {
    if (this->m_flagCacheXmlException) {
      throw CacheXmlException(temp.c_str());
    } else if (this->m_flagIllegalStateException) {
      throw IllegalStateException(temp.c_str());
    } else if (this->m_flagAnyOtherException) {
      throw UnknownException(temp.c_str());
    }
    this->setError("");
  }
}

//////////////////////  Static Methods  //////////////////////

/**
 * Parses XML data and from it creates an instance of
 * <code>CacheXmlParser</code> that can be used to create
 * the {@link Cache}, etc.
 *
 * @param  cacheXml
 *         The xml file
 *
 * @throws CacheXmlException
 *         Something went wrong while parsing the XML
 * @throws OutOfMemoryException
 * @throws CacheXmlException
 *         If xml file is not well-formed or
 *         Something went wrong while parsing the XML
 * @throws IllegalStateException
 *         If xml file is well-flrmed but not valid
 * @throws UnknownException otherwise
 */
CacheXmlParser* CacheXmlParser::parse(const char* cacheXml) {
  CacheXmlParser* handler;
  GF_NEW(handler, CacheXmlParser());
  // use RAII to delete the handler object in case of exceptions
  DeleteObject<CacheXmlParser> delHandler(handler);

  {
    handler->parseFile(cacheXml);
    delHandler.noDelete();
    return handler;
  }
}

void CacheXmlParser::setAttributes(Cache* cache) {}

/**
 * Creates cache artifacts ({@link Cache}s, etc.) based upon the XML
 * parsed by this parser.
 *
 * @param  cache
 *         The cachewhcih is to be populated
 * @throws OutOfMemoryException if the memory allocation failed
 * @throws NotConnectedException if the cache is not connected
 * @throws InvalidArgumentException if the attributePtr is NULL.
 * or if RegionAttributes is null or if regionName is null,
 * the empty   string, or contains a '/'
 * @throws RegionExistsException
 * @throws CacheClosedException if the cache is closed
 *         at the time of region creation
 * @throws UnknownException otherwise
 *
 */
void CacheXmlParser::create(Cache* cache) {
  // use DeleteObject class to delete m_cacheCreation in case of exceptions
  DeleteObject<CacheXmlCreation> delCacheCreation(m_cacheCreation);

  if (cache == NULL) {
    std::string s = "XML:No cache specified for performing configuration";
    throw IllegalArgumentException(s.c_str());
  }
  if (!m_cacheCreation) {
    throw CacheXmlException("XML: Element <cache> was not provided in the xml");
  }
  m_cacheCreation->create(cache);
  delCacheCreation.noDelete();
  Log::info("Declarative configuration of cache completed successfully");
}

void CacheXmlParser::startCache(void* ctx, const xmlChar** attrs) {
  int attrsCount = 0;
  if (attrs != NULL) {
    char* attrName;
    char* attrValue;
    while ((attrName = (char*)attrs[attrsCount++]) != NULL) {
      attrValue = (char*)attrs[attrsCount++];
      if (attrValue == NULL) {
        std::string exStr = "XML: No value provided for attribute: ";
        exStr += attrName;
        throw CacheXmlException(exStr.c_str());
      }
      if (strcmp(attrName, ENDPOINTS) == 0) {
        if (m_poolFactory) {
          std::vector<std::pair<std::string, int>> endPoints(
              parseEndPoints(attrValue));
          std::vector<std::pair<std::string, int>>::iterator endPoint;
          for (endPoint = endPoints.begin(); endPoint != endPoints.end();
               endPoint++) {
            m_poolFactory->addServer(endPoint->first.c_str(), endPoint->second);
          }
        }
      } else if (strcmp(attrName, REDUNDANCY_LEVEL) == 0) {
        m_poolFactory->setSubscriptionRedundancy(atoi(attrValue));
      }
    }
  }
  GF_NEW(m_cacheCreation, CacheXmlCreation());
}

void CacheXmlParser::startPdx(const xmlChar** atts) {
  if (!atts) {
    return;
  }

  int attrsCount = 0;

  while (atts[attrsCount] != NULL) {
    const char* name = reinterpret_cast<const char*>(atts[attrsCount]);
    ++attrsCount;
    const char* value = reinterpret_cast<const char*>(atts[attrsCount]);
    if (strcmp(name, IGNORE_UNREAD_FIELDS) == 0) {
      bool ignorePdxUnreadfields = false;
      if (strcmp("true", value) == 0 || strcmp("TRUE", value) == 0) {
        ignorePdxUnreadfields = true;
      } else if (strcmp("false", value) == 0 || strcmp("FALSE", value) == 0) {
        ignorePdxUnreadfields = false;
      } else {
        std::string temp(value);
        std::string s =
            "XML: " + temp +
            " is not a valid value for the attribute <ignore-unread-fields>";
        throw CacheXmlException(s.c_str());
      }
      m_cacheCreation->setPdxIgnoreUnreadField(ignorePdxUnreadfields);
    } else if (strcmp(name, READ_SERIALIZED) == 0) {
      bool readSerialized = false;
      if (strcmp("true", value) == 0 || strcmp("TRUE", value) == 0) {
        readSerialized = true;
      } else if (strcmp("false", value) == 0 || strcmp("FALSE", value) == 0) {
        readSerialized = false;
      } else {
        std::string temp(value);
        std::string s =
            "XML: " + temp +
            " is not a valid value for the attribute <ignore-unread-fields>";
        throw CacheXmlException(s.c_str());
      }
      m_cacheCreation->setPdxReadSerialized(readSerialized);
    } else {
      throw CacheXmlException("XML:Unrecognized pdx attribute");
    }
    ++attrsCount;
  }
}

void CacheXmlParser::endPdx() {}

void CacheXmlParser::startLocator(const xmlChar** atts) {
  int attrsCount = 0;
  if (!atts) {
    /* adongre
     * CID 28741: Parse warning (PW.EXPR_HAS_NO_EFFECT)expression has no effect
     */
    std::string s =
        "XML:No attributes provided for <locator>. "
        "A locator requires a host and port";
    throw CacheXmlException(s.c_str());
  }

  m_poolFactory = reinterpret_cast<PoolFactory*>(_stack.top());
  const char* host = NULL;
  const char* port = NULL;

  while (atts[attrsCount] != NULL) {
    const char* name = reinterpret_cast<const char*>(atts[attrsCount]);
    ++attrsCount;
    const char* value = reinterpret_cast<const char*>(atts[attrsCount]);
    if (strcmp(name, HOST) == 0) {
      host = value;
    } else if (strcmp(name, PORT) == 0) {
      port = value;
    } else {
      throw CacheXmlException("XML:Unrecognized locator attribute");
    }
    ++attrsCount;
  }

  if (attrsCount < 4) {
    throw CacheXmlException(
        "XML:Not enough attributes provided for a <locator> - host and port "
        "required");
  }

  m_poolFactory->addLocator(host, atoi(port));
}

void CacheXmlParser::startServer(const xmlChar** atts) {
  int attrsCount = 0;
  if (!atts) {
    /* adongre
     * CID 28742: Parse warning (PW.EXPR_HAS_NO_EFFECT)expression has no effect
     */
    std::string s =
        "XML:No attributes provided for <server>. A server requires a host and "
        "port";
    throw CacheXmlException(s.c_str());
  }

  PoolFactory* factory = reinterpret_cast<PoolFactory*>(_stack.top());
  const char* host = NULL;
  const char* port = NULL;

  while (atts[attrsCount] != NULL) {
    const char* name = reinterpret_cast<const char*>(atts[attrsCount]);
    ++attrsCount;
    const char* value = reinterpret_cast<const char*>(atts[attrsCount]);
    if (strcmp(name, HOST) == 0) {
      host = value;
    } else if (strcmp(name, PORT) == 0) {
      port = value;
    } else {
      throw CacheXmlException("XML:Unrecognized server attribute");
    }
    ++attrsCount;
  }

  if (attrsCount < 4) {
    throw CacheXmlException(
        "XML:Not enough attributes provided for a <server> - host and port "
        "required");
  }

  factory->addServer(host, atoi(port));
}

void CacheXmlParser::startPool(const xmlChar** atts) {
  int attrsCount = 0;
  if (!atts) {
    std::string s =
        "XML:No attributes provided for <pool>. "
        "A pool cannot be created without a name";
    throw CacheXmlException(s.c_str());
  }

  PoolFactoryPtr factory = PoolManager::createFactory();
  const char* poolName = NULL;

  while (atts[attrsCount] != NULL) {
    const char* name = reinterpret_cast<const char*>(atts[attrsCount]);
    ++attrsCount;
    const char* value = reinterpret_cast<const char*>(atts[attrsCount]);
    if (strcmp(name, NAME) == 0) {
      poolName = value;
    } else {
      setPoolInfo(factory.ptr(), name, value);
    }
    ++attrsCount;
  }

  if (attrsCount < 2) {
    std::string s =
        "XML:No attributes provided for a <pool> - at least the name is "
        "required";
    throw CacheXmlException(s.c_str());
  }

  PoolXmlCreation* poolxml = new PoolXmlCreation(poolName, factory);

  _stack.push(poolxml);
  _stack.push(factory.ptr());
}

void CacheXmlParser::endPool() {
  _stack.pop();  // remove factory
  PoolXmlCreation* poolxml = reinterpret_cast<PoolXmlCreation*>(_stack.top());
  _stack.pop();  // remove pool
  m_cacheCreation->addPool(poolxml);
}

void CacheXmlParser::setPoolInfo(PoolFactory* factory, const char* name,
                                 const char* value) {
  if (strcmp(name, FREE_CONNECTION_TIMEOUT) == 0) {
    factory->setFreeConnectionTimeout(atoi(value));
  } else if (strcmp(name, MULTIUSER_SECURE_MODE) == 0) {
    if (ACE_OS::strcasecmp(value, "true") == 0) {
      factory->setMultiuserAuthentication(true);
    } else {
      factory->setMultiuserAuthentication(false);
    }
  } else if (strcmp(name, IDLE_TIMEOUT) == 0) {
    factory->setIdleTimeout(atoi(value));
  } else if (strcmp(name, LOAD_CONDITIONING_INTERVAL) == 0) {
    factory->setLoadConditioningInterval(atoi(value));
  } else if (strcmp(name, MAX_CONNECTIONS) == 0) {
    factory->setMaxConnections(atoi(value));
  } else if (strcmp(name, MIN_CONNECTIONS) == 0) {
    factory->setMinConnections(atoi(value));
  } else if (strcmp(name, PING_INTERVAL) == 0) {
    factory->setPingInterval(atoi(value));
  } else if (strcmp(name, UPDATE_LOCATOR_LIST_INTERVAL) == 0) {
    factory->setUpdateLocatorListInterval(atoi(value));
  } else if (strcmp(name, READ_TIMEOUT) == 0) {
    factory->setReadTimeout(atoi(value));
  } else if (strcmp(name, RETRY_ATTEMPTS) == 0) {
    factory->setRetryAttempts(atoi(value));
  } else if (strcmp(name, SERVER_GROUP) == 0) {
    factory->setServerGroup(value);
  } else if (strcmp(name, SOCKET_BUFFER_SIZE) == 0) {
    factory->setSocketBufferSize(atoi(value));
  } else if (strcmp(name, STATISTIC_INTERVAL) == 0) {
    factory->setStatisticInterval(atoi(value));
  } else if (strcmp(name, SUBSCRIPTION_ACK_INTERVAL) == 0) {
    factory->setSubscriptionAckInterval(atoi(value));
  } else if (strcmp(name, SUBSCRIPTION_ENABLED) == 0) {
    if (ACE_OS::strcasecmp(value, "true") == 0) {
      factory->setSubscriptionEnabled(true);
    } else {
      factory->setSubscriptionEnabled(false);
    }
  } else if (strcmp(name, SUBSCRIPTION_MTT) == 0) {
    factory->setSubscriptionMessageTrackingTimeout(atoi(value));
  } else if (strcmp(name, SUBSCRIPTION_REDUNDANCY) == 0) {
    factory->setSubscriptionRedundancy(atoi(value));
  } else if (strcmp(name, THREAD_LOCAL_CONNECTIONS) == 0) {
    if (ACE_OS::strcasecmp(value, "true") == 0) {
      factory->setThreadLocalConnections(true);
    } else {
      factory->setThreadLocalConnections(false);
    }
  } else if (strcmp(name, PR_SINGLE_HOP_ENABLED) == 0) {
    if (ACE_OS::strcasecmp(value, "true") == 0) {
      factory->setPRSingleHopEnabled(true);
    } else {
      factory->setPRSingleHopEnabled(false);
    }
  } else {
    std::string s = "XML:Unrecognized pool attribute ";
    s += name;
    throw CacheXmlException(s.c_str());
  }
}

/**
 * When a <code>region</code> element is first encountered, we
 * create a {@link RegionCreation} and push it on the _stack.
 * An {@link AttributesFactory }is also created and puhed on _stack.
 */
void CacheXmlParser::startRegion(const xmlChar** atts, bool isRoot) {
  int attrsCount = 0;
  if (!atts) {
    std::string s =
        "XML:No attributes provided for <region>. "
        "A region cannot be created without a name";
    throw CacheXmlException(s.c_str());
  }
  while (atts[attrsCount] != NULL) ++attrsCount;
  if (attrsCount < 2 || attrsCount > 4) {
    std::string s =
        "XML:Incorrect number of attributes provided for a <region>";
    throw CacheXmlException(s.c_str());
  }

  char* regionName = NULL;
  char* refid = NULL;

  for (int i = 0; (atts[i] != NULL); i++) {
    if (atts[i] != NULL) {
      char* name = (char*)atts[i];
      i++;
      if (atts[i] != NULL) {
        char* value = (char*)atts[i];

        if (strcmp(name, "name") == 0) {
          regionName = value;
        } else if (strcmp(name, "refid") == 0) {
          refid = value;
        } else {
          std::string temp(name);
          std::string s =
              "XML:<region> does not contain the attribute :" + temp + "";
          throw CacheXmlException(s.c_str());
        }
      }
    }
  }

  if (regionName == NULL || strcmp(regionName, "") == 0)  // empty string
  {
    std::string s =
        "XML:The attribute name of <region> should be specified and cannot be "
        "empty ";
    throw CacheXmlException(s.c_str());
  }

  RegionXmlCreation* region = new RegionXmlCreation(regionName, isRoot);
  if (!region) {
    throw UnknownException("CacheXmlParser::startRegion:Out of memeory");
  }

  _stack.push(region);

  AttributesFactory* attrsFactory = NULL;
  if (refid == NULL) {
    attrsFactory = new AttributesFactory();
  } else {
    std::string refidStr(refid);

    if (namedRegions.find(refidStr) != namedRegions.end()) {
      attrsFactory = new AttributesFactory(namedRegions[refidStr]);
    } else {
      std::string s =
          "XML:referenced named attribute '" + refidStr + "' does not exist.";
      throw CacheXmlException(s.c_str());
    }
  }

  region->setAttributes(attrsFactory->createRegionAttributes());
  delete attrsFactory;
}

void CacheXmlParser::startSubregion(const xmlChar** atts) {
  startRegion(atts, false);
}

void CacheXmlParser::startRootRegion(const xmlChar** atts) {
  startRegion(atts, true);
}

void CacheXmlParser::startRegionAttributes(const xmlChar** atts) {
  bool isDistributed = false;
  bool isTCR = false;
  AttributesFactory* attrsFactory = NULL;
  if (atts) {
    int attrsCount = 0;
    while (atts[attrsCount] != NULL) ++attrsCount;

    if (attrsCount > 24)  // Remember to change this when the number changes
    {
      std::string s =
          "XML:Number of attributes provided for <region-attributes> are more";
      throw CacheXmlException(s.c_str());
    }

    char* refid = NULL;

    for (int i = 0; (atts[i] != NULL); i++) {
      i++;

      if (atts[i] != NULL) {
        char* value = (char*)atts[i];
        if (strcmp(value, "") == 0) {
          std::string s =
              "XML:In the <region-attributes> element no "
              "attribute can be set to empty string. It should either have a "
              "value or the attribute should be removed. In the latter case "
              "the default value will be set";
          throw CacheXmlException(s.c_str());
        }

        if (strcmp((char*)atts[i - 1], ID) == 0) {
          RegionXmlCreation* region =
              reinterpret_cast<RegionXmlCreation*>(_stack.top());
          region->setAttrId(std::string((char*)atts[i]));
        } else if (strcmp((char*)atts[i - 1], REFID) == 0) {
          refid = (char*)atts[i];
        }
      }
    }

    if (refid == NULL) {
      RegionXmlCreation* region =
          reinterpret_cast<RegionXmlCreation*>(_stack.top());
      attrsFactory = new AttributesFactory(region->getAttributes());
    } else {
      std::string refidStr(refid);

      if (namedRegions.find(refidStr) != namedRegions.end()) {
        attrsFactory = new AttributesFactory(namedRegions[refidStr]);
      } else {
        std::string s =
            "XML:referenced named attribute '" + refidStr + "' does not exist.";
        throw CacheXmlException(s.c_str());
      }
    }

    if (!attrsFactory) {
      throw UnknownException(
          "CacheXmlParser::startRegionAttributes:Out of memeory");
    }

    _stack.push(attrsFactory);

    for (int i = 0; (atts[i] != NULL); i++) {
      if (strcmp(ID, (char*)atts[i]) == 0 ||
          strcmp(REFID, (char*)atts[i]) == 0) {
        i++;
        continue;
      } else if (strcmp(CLIENT_NOTIFICATION_ENABLED, (char*)atts[i]) == 0) {
        i++;
        char* client_notification_enabled = (char*)atts[i];
        if (strcmp("false", client_notification_enabled) == 0 ||
            strcmp("FALSE", client_notification_enabled) == 0) {
          if (m_poolFactory) {
            m_poolFactory->setSubscriptionEnabled(false);
          }
        } else if (strcmp("true", client_notification_enabled) == 0 ||
                   strcmp("TRUE", client_notification_enabled) == 0) {
          if (m_poolFactory) {
            m_poolFactory->setSubscriptionEnabled(true);
          }
        } else {
          char* name = (char*)atts[i];
          std::string temp(name);
          std::string s =
              "XML: " + temp +
              " is not a valid value for the attribute <client-notification>\n";
          throw CacheXmlException(s.c_str());
        }
      } else if (strcmp(INITIAL_CAPACITY, (char*)atts[i]) == 0) {
        i++;
        char* initialCapacity = (char*)atts[i];
        attrsFactory->setInitialCapacity(atoi(initialCapacity));
      } else if (strcmp(CONCURRENCY_LEVEL, (char*)atts[i]) == 0) {
        i++;
        char* concurrencyLevel = (char*)atts[i];
        attrsFactory->setConcurrencyLevel(atoi(concurrencyLevel));
      } else if (strcmp(LOAD_FACTOR, (char*)atts[i]) == 0) {
        i++;
        char* loadFactor = (char*)atts[i];
        attrsFactory->setLoadFactor(
            static_cast<float>(atof(loadFactor)));  // check whether this works
      } else if (strcmp(CACHING_ENABLED, (char*)atts[i]) == 0) {
        bool flag = false;
        i++;
        char* cachingEnabled = (char*)atts[i];
        if (strcmp("true", cachingEnabled) == 0 ||
            strcmp("TRUE", cachingEnabled) == 0) {
          flag = true;
        } else if (strcmp("false", cachingEnabled) == 0 ||
                   strcmp("FALSE", cachingEnabled) == 0) {
          flag = false;
        } else {
          char* name = (char*)atts[i];
          std::string temp(name);
          std::string s =
              "XML: " + temp +
              " is not a valid value for the attribute <caching-enabled>";
          throw CacheXmlException(s.c_str());
        }
        attrsFactory->setCachingEnabled(flag);  // check whether this works
      } else if (strcmp(LRU_ENTRIES_LIMIT, (char*)atts[i]) == 0) {
        i++;
        char* lruentriesLimit = (char*)atts[i];
        int lruentriesLimitInt = atoi(lruentriesLimit);
        uint32_t temp = static_cast<uint32_t>(lruentriesLimitInt);
        attrsFactory->setLruEntriesLimit(temp);
      } else if (strcmp(DISK_POLICY, (char*)atts[i]) == 0) {
        i++;
        char* diskPolicy = (char*)atts[i];
        if (strcmp(OVERFLOWS, diskPolicy) == 0) {
          attrsFactory->setDiskPolicy(gemfire::DiskPolicyType::OVERFLOWS);
        } else if (strcmp(PERSIST, diskPolicy) == 0) {
          throw IllegalStateException(" persistence feature is not supported");
        } else if (strcmp(NONE, diskPolicy) == 0) {
          attrsFactory->setDiskPolicy(gemfire::DiskPolicyType::NONE);
        } else {
          char* name = (char*)atts[i];
          std::string temp(name);
          std::string s =
              "XML: " + temp +
              " is not a valid value for the attribute <disk-policy>";
          throw CacheXmlException(s.c_str());
        }
      } else if (strcmp(ENDPOINTS, (char*)atts[i]) == 0) {
        i++;
        if (m_poolFactory) {
          std::vector<std::pair<std::string, int>> endPoints(
              parseEndPoints((char*)atts[i]));
          std::vector<std::pair<std::string, int>>::iterator endPoint;
          for (endPoint = endPoints.begin(); endPoint != endPoints.end();
               endPoint++) {
            m_poolFactory->addServer(endPoint->first.c_str(), endPoint->second);
          }
        }
        isTCR = true;
      } else if (strcmp(POOL_NAME, (char*)atts[i]) == 0) {
        i++;
        char* poolName = (char*)atts[i];
        attrsFactory->setPoolName(poolName);
        isTCR = true;
      } else if (strcmp(CLONING_ENABLED, (char*)atts[i]) == 0) {
        i++;
        bool flag = false;
        char* isClonable = (char*)atts[i];

        if (strcmp("true", isClonable) == 0 ||
            strcmp("TRUE", isClonable) == 0) {
          flag = true;
        } else if (strcmp("false", isClonable) == 0 ||
                   strcmp("FALSE", isClonable) == 0) {
          flag = false;
        } else {
          char* name = (char*)atts[i];
          std::string temp(name);
          std::string s =
              "XML: " + temp +
              " is not a valid value for the attribute <cloning-enabled>";
          throw CacheXmlException(s.c_str());
        }

        attrsFactory->setCloningEnabled(flag);
        isTCR = true;
      } else if (strcmp(CONCURRENCY_CHECKS_ENABLED, (char*)atts[i]) == 0) {
        bool flag = false;
        i++;
        char* concurrencyChecksEnabled = (char*)atts[i];
        if (strcmp("true", concurrencyChecksEnabled) == 0 ||
            strcmp("TRUE", concurrencyChecksEnabled) == 0) {
          flag = true;
        } else if (strcmp("false", concurrencyChecksEnabled) == 0 ||
                   strcmp("FALSE", concurrencyChecksEnabled) == 0) {
          flag = false;
        } else {
          char* name = (char*)atts[i];
          std::string temp(name);
          std::string s = "XML: " + temp +
                          " is not a valid value for the attribute "
                          "<concurrency-checks-enabled>";
          throw CacheXmlException(s.c_str());
        }
        attrsFactory->setConcurrencyChecksEnabled(flag);
      }
    }  // for loop
  }    // atts is NULL
  else {
    RegionXmlCreation* region =
        reinterpret_cast<RegionXmlCreation*>(_stack.top());
    attrsFactory = new AttributesFactory(region->getAttributes());
    _stack.push(attrsFactory);
  }

  if (isDistributed && isTCR) {
    /* we don't allow DR+TCR at current stage according to sudhir */
    std::string s = "XML:endpoints cannot be defined for distributed region.\n";
    throw CacheXmlException(s.c_str());
  }
}

void CacheXmlParser::endRegionAttributes() {
  AttributesFactory* attrsFactory =
      reinterpret_cast<AttributesFactory*>(_stack.top());
  _stack.pop();
  if (!attrsFactory) {
    throw UnknownException(
        "CacheXmlParser::endRegion:AttributesFactory is null");
  }

  RegionAttributesPtr regionAttributesPtr =
      attrsFactory->createRegionAttributes();

  RegionXmlCreation* regionPtr =
      reinterpret_cast<RegionXmlCreation*>(_stack.top());
  if (!regionPtr) {
    throw UnknownException("CacheXmlParser::endRegion:Region is null");
  }

  std::string id = regionPtr->getAttrId();
  if (id != "") {
    namedRegions[id] = regionAttributesPtr;
  }

  regionPtr->setAttributes(regionAttributesPtr);
}

/**
 * When a <code>expiration-attributes</code> element is first
 * encountered, we create an {@link ExpirationAttibutes} object from
 * the element's attributes and push it on the _stack.
 */
void CacheXmlParser::startExpirationAttributes(const xmlChar** atts) {
  if (!atts) {
    std::string s = "XML:No attributes provided for <expiration-attributes> ";
    throw CacheXmlException(s.c_str());
  }
  m_flagExpirationAttribute = true;
  int attrsCount = 0;
  while (atts[attrsCount] != NULL) ++attrsCount;
  if (attrsCount > 4) {
    std::string s =
        "XML:Incorrect number of attributes provided for "
        "<expirartion-attributes>";
    throw CacheXmlException(s.c_str());
  }
  char* timeOut = NULL;
  int timeOutInt = 0;
  ExpirationAction::Action expire = ExpirationAction::INVALID_ACTION;
  for (int i = 0; (atts[i] != NULL); i++) {
    if (strcmp(TIMEOUT, (char*)atts[i]) == 0) {
      i++;
      timeOut = (char*)atts[i];
      if (strcmp(timeOut, "") == 0) {
        std::string s =
            "XML:Value for attribute <timeout> needs to be specified";
        throw CacheXmlException(s.c_str());
      }
      timeOutInt = atoi(timeOut);
    } else if (strcmp(ACTION, (char*)atts[i]) == 0) {
      i++;
      char* action = (char*)atts[i];
      if (strcmp(action, "") == 0)

      {
        std::string s =
            "XML:The attribute <action> of <expiration-attributes> cannot be "
            "set to empty string. It should either have a value or the "
            "attribute should be removed. In the latter case the default value "
            "will be set";
        throw CacheXmlException(s.c_str());
      } else if (strcmp(INVALIDATE, action) == 0) {
        expire = ExpirationAction::INVALIDATE;
      } else if (strcmp(DESTROY, action) == 0) {
        expire = ExpirationAction::DESTROY;
      } else if (strcmp(LOCAL_INVALIDATE, action) == 0) {
        expire = ExpirationAction::LOCAL_INVALIDATE;
      } else if (strcmp(LOCAL_DESTROY, action) == 0) {
        expire = ExpirationAction::LOCAL_DESTROY;
      } else {
        char* name = (char*)atts[i];
        std::string temp(name);
        std::string s =
            "XML: " + temp + " is not a valid value for the attribute <action>";
        throw CacheXmlException(s.c_str());
      }
    } else {
      char* name = (char*)atts[i];
      std::string temp(name);
      std::string s =
          "XML:Incorrect attribute name specified in "
          "<expiration-attributes>: " +
          temp;
      throw CacheXmlException(s.c_str());
    }
  }
  if (timeOut == NULL || strcmp(timeOut, "") == 0) {
    std::string s =
        "XML:The attribute <timeout> not specified in <expiration-attributes>.";
    throw CacheXmlException(s.c_str());
  }

  ExpirationAttributes* expireAttr =
      new ExpirationAttributes(timeOutInt, expire);
  if (!expireAttr) {
    throw UnknownException(
        "CacheXmlParser::startExpirationAttributes:Out of memeory");
  }
  expireAttr->setAction(expire);

  _stack.push(expireAttr);
}
void CacheXmlParser::startPersistenceManager(const xmlChar** atts) {
  int attrsCount = 0;
  if (!atts) {
    std::string s = "XML:No attributes provided for <persistence-manager>";
    throw CacheXmlException(s.c_str());
  }
  while (atts[attrsCount] != NULL) ++attrsCount;
  if (attrsCount > 4) {
    std::string s =
        "XML:Incorrect number of attributes provided for <persistence-manager>";
    throw CacheXmlException(s.c_str());
  }
  char* libraryName = NULL;
  char* libraryFunctionName = NULL;
  for (int i = 0; (atts[i] != NULL); i++) {
    if (strcmp(LIBRARY_NAME, (char*)atts[i]) == 0) {
      i++;
      size_t len = strlen((char*)atts[i]) + 1;
      libraryName = new char[len];
      /* adongre
       * CID 28824: Dereference before null check (REVERSE_INULL)
       */
      if (libraryName == NULL) {
        std::string s = "Memory allocation fails";
        throw CacheXmlException(s.c_str());
      }
      ACE_OS::strncpy(libraryName, (char*)atts[i], len);

      if (libraryName == NULL) {
        std::string s =
            "XML:The attribute <library-name> of <persistence-manager> cannot "
            "be set to an empty string. It should either have a value or the "
            "attribute should be removed. In the latter case the default value "
            "will be set";
        throw CacheXmlException(s.c_str());
      }
    } else if (strcmp(LIBRARY_FUNCTION_NAME, (char*)atts[i]) == 0) {
      i++;
      size_t len = strlen((char*)atts[i]) + 1;
      libraryFunctionName = new char[len];
      /* adongre
       * CID 28823: Dereference before null check (REVERSE_INULL)
       */
      if (libraryFunctionName == NULL) {
        std::string s = "Memory allocation fails";
        throw CacheXmlException(s.c_str());
      }

      ACE_OS::strncpy(libraryFunctionName, (char*)atts[i], len);
      if (libraryFunctionName == NULL) {
        std::string s =
            "XML:Value for the <library-function-name> needs to be provided";
        ;
        throw CacheXmlException(s.c_str());
      }
    } else {
      char* name = (char*)atts[i];
      std::string temp(name);
      std::string s =
          "XML:Incorrect attribute name specified in <persistence-manager>: " +
          temp;
      throw CacheXmlException(s.c_str());
    }
  }
  if (libraryFunctionName == NULL) {
    std::string s =
        "XML:Library function name not specified in the <persistence-manager>";
    throw CacheXmlException(s.c_str());
  }

  try {
    if (managedPersistenceManagerFn != NULL &&
        strchr(libraryFunctionName, '.') != NULL) {
      // this is a managed library
      (*managedPersistenceManagerFn)(libraryName, libraryFunctionName);
    } else {
      getFactoryFunc(libraryName, libraryFunctionName);
    }
  } catch (IllegalArgumentException& ex) {
    throw CacheXmlException(ex.getMessage());
  }

  _stack.push(libraryName);
  _stack.push(libraryFunctionName);
}

void CacheXmlParser::startPersistenceProperties(const xmlChar** atts) {
  if (!atts) {
    std::string s = "XML:No attributes provided for <property> ";
    throw CacheXmlException(s.c_str());
  }
  int attrsCount = 0;
  while (atts[attrsCount] != NULL) ++attrsCount;
  if (attrsCount > 4) {
    std::string s =
        "XML:Incorrect number of attributes provided for <property>";
    throw CacheXmlException(s.c_str());
  } else {
    if (m_config == NULLPTR) {
      m_config = Properties::create();
    }
  }
  char* propName = NULL;
  char* propValue = NULL;
  for (int i = 0; (atts[i] != NULL); i++) {
    if (strcmp("name", (char*)atts[i]) == 0) {
      i++;
      propName = (char*)atts[i];
      if (propName == NULL || strcmp(propName, "") == 0) {
        std::string s =
            "XML:Value for attribute <name> needs to be specified in the "
            "<property>";
        throw CacheXmlException(s.c_str());
      }
    } else if (strcmp("value", (char*)atts[i]) == 0) {
      i++;
      propValue = (char*)atts[i];
      if (propValue == NULL || strcmp(propValue, "") == 0) {
        std::string s =
            "XML:Value for attribute <value> needs to be "
            "specified in the <property>";
        throw CacheXmlException(s.c_str());
      }
    } else {
      char* name = (char*)atts[i];
      std::string temp(name);
      std::string s =
          "XML:Incorrect attribute name specified in <property>: " + temp;
      throw CacheXmlException(s.c_str());
    }
  }
  if (propName == NULL || strcmp(propName, "") == 0) {
    std::string s =
        "XML:attribute <name> needs to be specified in the <property>";
    throw CacheXmlException(s.c_str());
  }
  if (propValue == NULL || strcmp(propValue, "") == 0) {
    std::string s =
        "XML:attribute <value> needs to be  specified in the <property>";
    throw CacheXmlException(s.c_str());
  }
  m_config->insert(propName, propValue);
}

void CacheXmlParser::startCacheLoader(const xmlChar** atts) {
  char* libraryName = NULL;
  char* libraryFunctionName = NULL;
  int attrsCount = 0;
  if (!atts) {
    std::string s = "XML:No attributes provided for <cache-loader>";
    throw CacheXmlException(s.c_str());
  }
  while (atts[attrsCount] != NULL) ++attrsCount;
  if (attrsCount > 4) {
    std::string s =
        "XML:Incorrect number of attributes provided for <cache-loader>";
    throw CacheXmlException(s.c_str());
  }

  for (int i = 0; (atts[i] != NULL); i++) {
    if (strcmp(LIBRARY_NAME, (char*)atts[i]) == 0) {
      i++;
      libraryName = (char*)atts[i];
      if (libraryName == NULL || strcmp(libraryName, "") == 0) {
        std::string s =
            "XML:The attribute <library-name> of <cache-loader> cannot be set "
            "to an empty string. It should either have a value or the "
            "attribute should be removed. In the latter case the default value "
            "will be set";
        throw CacheXmlException(s.c_str());
      }
    } else if (strcmp(LIBRARY_FUNCTION_NAME, (char*)atts[i]) == 0) {
      i++;
      libraryFunctionName = (char*)atts[i];
      if (libraryFunctionName == NULL || strcmp(libraryFunctionName, "") == 0) {
        std::string s =
            "XML:Value for the <library-function-name> needs to be provided";
        throw CacheXmlException(s.c_str());
      }
    } else {
      char* name = (char*)atts[i];
      std::string temp(name);
      std::string s =
          "XML:Incorrect attribute name specified in <cache-loader> : " + temp;
      throw CacheXmlException(s.c_str());
    }
  }
  if (libraryFunctionName == NULL || strcmp(libraryFunctionName, "") == 0) {
    std::string s =
        "XML:<library-function-name> not specified in <cache-loader> ";
    throw CacheXmlException(s.c_str());
  }

  try {
    if (managedCacheLoaderFn != NULL &&
        strchr(libraryFunctionName, '.') != NULL) {
      // this is a managed library
      (*managedCacheLoaderFn)(libraryName, libraryFunctionName);
    } else {
      getFactoryFunc(libraryName, libraryFunctionName);
    }
  } catch (IllegalArgumentException& ex) {
    throw CacheXmlException(ex.getMessage());
  }

  AttributesFactory* attrsFactory =
      reinterpret_cast<AttributesFactory*>(_stack.top());
  attrsFactory->setCacheLoader(libraryName, libraryFunctionName);
}

void CacheXmlParser::startCacheListener(const xmlChar** atts) {
  char* libraryName = NULL;
  char* libraryFunctionName = NULL;
  int attrsCount = 0;
  if (!atts) {
    std::string s = "XML:No attributes provided for <cache-listener> ";
    throw CacheXmlException(s.c_str());
  }
  while (atts[attrsCount] != NULL) ++attrsCount;
  if (attrsCount > 4) {
    std::string s =
        "XML:Incorrect number of attributes provided for <cache-listener>";
    throw CacheXmlException(s.c_str());
  }

  for (int i = 0; (atts[i] != NULL); i++) {
    if (strcmp(LIBRARY_NAME, (char*)atts[i]) == 0) {
      i++;
      libraryName = (char*)atts[i];
      if (libraryName == NULL || strcmp(libraryName, "") == 0) {
        std::string s =
            "XML:The attribute <library-name> of the <cache-listener> tag "
            "cannot be set to an empty string. It should either have a value "
            "or the attribute should be removed. In the latter case the "
            "default value will be set";
        throw CacheXmlException(s.c_str());
      }
    } else if (strcmp(LIBRARY_FUNCTION_NAME, (char*)atts[i]) == 0) {
      i++;
      libraryFunctionName = (char*)atts[i];
      if (libraryFunctionName == NULL || strcmp(libraryFunctionName, "") == 0) {
        std::string s =
            "XML:Value for <library-function-name> needs to be provided";
        throw CacheXmlException(s.c_str());
      }
    } else {
      char* name = (char*)atts[i];
      std::string temp(name);
      std::string s =
          "XML:Incorrect attribute name specified in <cache-listener> : " +
          temp;
      throw CacheXmlException(s.c_str());
    }
  }
  if (libraryFunctionName == NULL || strcmp(libraryFunctionName, "") == 0) {
    std::string s =
        "XML:Library function name not specified in <cache-listener> ";
    throw CacheXmlException(s.c_str());
  }

  try {
    if (managedCacheListenerFn != NULL &&
        strchr(libraryFunctionName, '.') != NULL) {
      // this is a managed library
      (*managedCacheListenerFn)(libraryName, libraryFunctionName);
    } else {
      getFactoryFunc(libraryName, libraryFunctionName);
    }
  } catch (IllegalArgumentException& ex) {
    throw CacheXmlException(ex.getMessage());
  }

  AttributesFactory* attrsFactory =
      reinterpret_cast<AttributesFactory*>(_stack.top());
  attrsFactory->setCacheListener(libraryName, libraryFunctionName);
}

void CacheXmlParser::startPartitionResolver(const xmlChar** atts) {
  char* libraryName = NULL;
  char* libraryFunctionName = NULL;
  int attrsCount = 0;
  if (!atts) {
    std::string s = "XML:No attributes provided for <partition-resolver> ";
    throw CacheXmlException(s.c_str());
  }
  while (atts[attrsCount] != NULL) ++attrsCount;
  if (attrsCount > 4) {
    std::string s =
        "XML:Incorrect number of attributes provided for <partition-resolver>";
    throw CacheXmlException(s.c_str());
  }

  for (int i = 0; (atts[i] != NULL); i++) {
    if (strcmp(LIBRARY_NAME, (char*)atts[i]) == 0) {
      i++;
      libraryName = (char*)atts[i];
      if (libraryName == NULL || strcmp(libraryName, "") == 0) {
        std::string s =
            "XML:The attribute <library-name> of the <partition-resolver> tag "
            "cannot be set to an empty string. It should either have a value "
            "or the attribute should be removed. In the latter case the "
            "default value will be set";
        throw CacheXmlException(s.c_str());
      }
    } else if (strcmp(LIBRARY_FUNCTION_NAME, (char*)atts[i]) == 0) {
      i++;
      libraryFunctionName = (char*)atts[i];
      if (libraryFunctionName == NULL || strcmp(libraryFunctionName, "") == 0) {
        std::string s =
            "XML:Value for <library-function-name> needs to be provided";
        throw CacheXmlException(s.c_str());
      }
    } else {
      char* name = (char*)atts[i];
      std::string temp(name);
      std::string s =
          "XML:Incorrect attribute name specified in <partition-resolver> : " +
          temp;
      throw CacheXmlException(s.c_str());
    }
  }
  if (libraryFunctionName == NULL || strcmp(libraryFunctionName, "") == 0) {
    std::string s =
        "XML:Library function name not specified in <partition-resolver> ";
    throw CacheXmlException(s.c_str());
  }

  try {
    if (managedPartitionResolverFn != NULL &&
        strchr(libraryFunctionName, '.') != NULL) {
      // this is a managed library
      (*managedPartitionResolverFn)(libraryName, libraryFunctionName);
    } else {
      getFactoryFunc(libraryName, libraryFunctionName);
    }
  } catch (IllegalArgumentException& ex) {
    throw CacheXmlException(ex.getMessage());
  }

  AttributesFactory* attrsFactory =
      reinterpret_cast<AttributesFactory*>(_stack.top());
  attrsFactory->setPartitionResolver(libraryName, libraryFunctionName);
}

void CacheXmlParser::startCacheWriter(const xmlChar** atts) {
  char* libraryName = NULL;
  char* libraryFunctionName = NULL;
  int attrsCount = 0;
  if (!atts) {
    std::string s = "XML:No attributes provided for <cache-writer>";
    throw CacheXmlException(s.c_str());
  }
  while (atts[attrsCount] != NULL) ++attrsCount;
  if (attrsCount > 4) {
    std::string s =
        "XML:Incorrect number of attributes provided for <cache-writer>";
    throw CacheXmlException(s.c_str());
  }

  for (int i = 0; (atts[i] != NULL); i++) {
    if (strcmp(LIBRARY_NAME, (char*)atts[i]) == 0) {
      i++;
      libraryName = (char*)atts[i];
      if (libraryName == NULL || strcmp(libraryName, "") == 0) {
        std::string s =
            "XML:The attribute <library-name> of <cache-writer> cannot be set "
            "to an empty string. It should either have a value or the "
            "attribute should be removed. In the latter case the default value "
            "will be set";
        throw CacheXmlException(s.c_str());
      }
    } else if (strcmp(LIBRARY_FUNCTION_NAME, (char*)atts[i]) == 0) {
      i++;
      libraryFunctionName = (char*)atts[i];
      if (libraryFunctionName == NULL || strcmp(libraryFunctionName, "") == 0) {
        std::string s =
            "XML:Value for the <library-function-name> needs to be provided";
        throw CacheXmlException(s.c_str());
      }
    } else {
      char* name = (char*)atts[i];
      std::string temp(name);
      std::string s =
          "XML:Incorrect attribute name specified in <cache-writer>: " + temp;
      throw CacheXmlException(s.c_str());
    }
  }
  if (strcmp(libraryFunctionName, "") == 0) {
    std::string s =
        "XML:Library function name not specified in the <cache-writer>";
    throw CacheXmlException(s.c_str());
  }

  try {
    if (managedCacheWriterFn != NULL &&
        strchr(libraryFunctionName, '.') != NULL) {
      // this is a managed library
      (*managedCacheWriterFn)(libraryName, libraryFunctionName);
    } else {
      getFactoryFunc(libraryName, libraryFunctionName);
    }
  } catch (IllegalArgumentException& ex) {
    throw CacheXmlException(ex.getMessage());
  }

  AttributesFactory* attrsFactory =
      reinterpret_cast<AttributesFactory*>(_stack.top());
  attrsFactory->setCacheWriter(libraryName, libraryFunctionName);
}

/**
 * After popping the current <code>RegionXmlCreation</code> off the
 * _stack, if the element on top of the _stack is a
 * <code>RegionXmlCreation</code>, then it is the parent region.
 */
void CacheXmlParser::endRegion(bool isRoot) {
  RegionXmlCreation* regionPtr =
      reinterpret_cast<RegionXmlCreation*>(_stack.top());
  _stack.pop();
  if (isRoot) {
    if (!_stack.empty()) {
      std::string s = "Xml file has incorrectly nested region tags";
      throw CacheXmlException(s.c_str());
    }
    if (!m_cacheCreation) {
      throw CacheXmlException(
          "XML: Element <cache> was not provided in the xml");
    }

    m_cacheCreation->addRootRegion(regionPtr);
  } else {
    if (_stack.empty()) {
      std::string s = "Xml file has incorrectly nested region tags";
      throw CacheXmlException(s.c_str());
    }
    RegionXmlCreation* parent =
        reinterpret_cast<RegionXmlCreation*>(_stack.top());
    parent->addSubregion(regionPtr);
  }
}

void CacheXmlParser::endSubregion() { endRegion(false); }

void CacheXmlParser::endRootRegion() { endRegion(true); }

/**
 * When a <code>cache</code> element is finished
 */
void CacheXmlParser::endCache() {}

/**
 * When a <code>region-time-to-live</code> element is finished, the
 * {@link ExpirationAttributes} are on top of the _stack followed by
 * the {@link AttributesFactory} to which the expiration
 * attributes are assigned.
 */
void CacheXmlParser::endRegionTimeToLive() {
  if (!m_flagExpirationAttribute) {
    std::string s =
        "XML: <region-time-to-live> cannot be without a "
        "<expiration-attributes>";
    throw CacheXmlException(s.c_str());
  }

  ExpirationAttributes* expireAttr =
      reinterpret_cast<ExpirationAttributes*>(_stack.top());
  _stack.pop();

  AttributesFactory* attrsFactory =
      reinterpret_cast<AttributesFactory*>(_stack.top());
  attrsFactory->setRegionTimeToLive(expireAttr->getAction(),
                                    expireAttr->getTimeout());
  m_flagExpirationAttribute = false;
}

/**
 * When a <code>region-idle-time</code> element is finished, the
 * {@link ExpirationAttributes} are on top of the _stack followed by
 * the {@link AttributesFactory} to which the expiration
 * attributes are assigned.
 */
void CacheXmlParser::endRegionIdleTime() {
  if (!m_flagExpirationAttribute) {
    std::string s =
        "XML: <region-idle-time> cannot be without <expiration-attributes>";
    throw CacheXmlException(s.c_str());
  }
  ExpirationAttributes* expireAttr =
      reinterpret_cast<ExpirationAttributes*>(_stack.top());
  _stack.pop();
  AttributesFactory* attrsFactory =
      reinterpret_cast<AttributesFactory*>(_stack.top());

  attrsFactory->setRegionIdleTimeout(expireAttr->getAction(),
                                     expireAttr->getTimeout());
  m_flagExpirationAttribute = false;
}

/**
 * When a <code>entry-time-to-live</code> element is finished, the
 * {@link ExpirationAttributes} are on top of the _stack followed by
 * the {@link AttributesFactory} to which the expiration
 * attributes are assigned.
 */
void CacheXmlParser::endEntryTimeToLive() {
  if (!m_flagExpirationAttribute) {
    std::string s =
        "XML: <entry-time-to-live> cannot be without "
        "<expiration-attributes>";
    throw CacheXmlException(s.c_str());
  }
  ExpirationAttributes* expireAttr =
      reinterpret_cast<ExpirationAttributes*>(_stack.top());
  _stack.pop();
  AttributesFactory* attrsFactory =
      reinterpret_cast<AttributesFactory*>(_stack.top());

  attrsFactory->setEntryTimeToLive(expireAttr->getAction(),
                                   expireAttr->getTimeout());
  m_flagExpirationAttribute = false;
}

/**
 * When a <code>entry-idle-time</code> element is finished, the
 * {@link ExpirationAttributes} are on top of the _stack followed by
 * the {@link AttributesFactory} to which the expiration
 * attributes are assigned.
 */
void CacheXmlParser::endEntryIdleTime() {
  if (!m_flagExpirationAttribute) {
    std::string s =
        "XML: <entry-idle-time> cannot be without <expiration-attributes>";
    throw CacheXmlException(s.c_str());
  }
  ExpirationAttributes* expireAttr =
      reinterpret_cast<ExpirationAttributes*>(_stack.top());
  _stack.pop();
  AttributesFactory* attrsFactory =
      reinterpret_cast<AttributesFactory*>(_stack.top());
  attrsFactory->setEntryIdleTimeout(expireAttr->getAction(),
                                    expireAttr->getTimeout());
  m_flagExpirationAttribute = false;
}

/**
 * When persistence-manager attributes is finished, it will set the attribute
 * factory.
 */
void CacheXmlParser::endPersistenceManager() {
  char* libraryFunctionName = reinterpret_cast<char*>(_stack.top());
  _stack.pop();
  char* libraryName = reinterpret_cast<char*>(_stack.top());
  _stack.pop();
  AttributesFactory* attrsFactory =
      reinterpret_cast<AttributesFactory*>(_stack.top());
  if (m_config != NULLPTR) {
    attrsFactory->setPersistenceManager(libraryName, libraryFunctionName,
                                        m_config);
    m_config = NULLPTR;
  } else {
    attrsFactory->setPersistenceManager(libraryName, libraryFunctionName);
  }
  // Free memory allocated in startPersistenceManager, already checked for NULL
  free(libraryName);
  free(libraryFunctionName);
}

CacheXmlParser::~CacheXmlParser() { GF_SAFE_DELETE(m_cacheCreation); }

void CacheXmlParser::setError(const std::string& err) { m_error = err; }

const std::string& CacheXmlParser::getError() const { return m_error; }
