/*=========================================================================
* Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
* All Rights Reserved.
*=========================================================================
*/

#include "fwklib/FwkObjects.hpp"
#include "fwklib/FwkLog.hpp"
#include "fwklib/FwkException.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/GsRandom.hpp"

#include <xercesc/dom/DOMErrorHandler.hpp>
#include <xercesc/util/XMLString.hpp>
#include <xercesc/util/PlatformUtils.hpp>
#include <xercesc/parsers/AbstractDOMParser.hpp>
#include <xercesc/dom/DOMImplementation.hpp>
#include <xercesc/dom/DOMImplementationLS.hpp>
#include <xercesc/dom/DOMImplementationRegistry.hpp>
#include <xercesc/dom/DOMLSParser.hpp>
#include <xercesc/dom/DOMException.hpp>
#include <xercesc/dom/DOMDocument.hpp>
#include <xercesc/dom/DOMNodeList.hpp>
#include <xercesc/dom/DOMError.hpp>
#include <xercesc/dom/DOMLocator.hpp>
#include <xercesc/dom/DOMNamedNodeMap.hpp>
#include <xercesc/dom/DOMAttr.hpp>

#include <ctype.h>

using namespace gemfire;
using namespace gemfire::testframework;

const std::string FwkClientSet::m_defaultGroup = "DEFAULT";

XERCES_CPP_NAMESPACE_USE

bool allSpace(std::string& str) {
  bool space = true;
  if (str.empty()) return space;
  const char* chk = str.c_str();
  while (space && (*chk != '\0')) {
    if (!isspace(*chk)) space = false;
    chk++;
  }
  return space;
}

int32_t traverseChildElements(const DOMNode* node) {
  if (node == NULL) return 0;
  const DOMNode* child;
  int32_t count = 0;
  switch (node->getNodeType()) {
    case DOMNode::ELEMENT_NODE: {
      count++;
      std::string name = XMLChToStr(node->getNodeName());
      FWKINFO("--- Element: " << name);

      if (node->hasAttributes()) {
        DOMNamedNodeMap* attributes = node->getAttributes();
        int32_t size = static_cast<int32_t>(attributes->getLength());
        for (int32_t i = 0; i < size; ++i) {
          DOMAttr* attributeNode = (DOMAttr*)attributes->item(i);
          std::string attName = XMLChToStr(attributeNode->getName());
          std::string attValue = XMLChToStr(attributeNode->getValue());
          FWKINFO("------ " << name << " Attribute: " << attName
                            << " has value: " << attValue);
        }
      }
      FWKINFO("------ Children of " << name);
      for (child = node->getFirstChild(); child != NULL;
           child = child->getNextSibling()) {
        count += traverseChildElements(child);
      }
      FWKINFO("------ End of " << name);
    } break;
    case DOMNode::TEXT_NODE: {
      //      DOMText * tnode = dynamic_cast< DOMText * > ( ( DOMNode * )node );
      DOMText* tnode = (DOMText*)node;
      std::string tname = XMLChToStr(tnode->getNodeName());
      std::string text = XMLChToStr(tnode->getNodeValue());
      std::string ftext = XMLChToStr(tnode->getNodeValue());
      if (!tnode->isIgnorableWhitespace() && !allSpace(text))
        FWKINFO("--- " << tname << " Has text content ::>" << text
                       << "<:: " << text.length());
    } break;
    case DOMNode::ATTRIBUTE_NODE:
      FWKINFO("Node type: ATTRIBUTE_NODE");
      break;
    case DOMNode::CDATA_SECTION_NODE:
      FWKINFO("Node type: CDATA_SECTION_NODE");
      break;
    case DOMNode::ENTITY_REFERENCE_NODE:
      FWKINFO("Node type: ENTITY_REFERENCE_NODE");
      break;
    case DOMNode::ENTITY_NODE:
      FWKINFO("Node type: ENTITY_NODE");
      break;
    case DOMNode::PROCESSING_INSTRUCTION_NODE:
      FWKINFO("Node type: PROCESSING_INSTRUCTION_NODE");
      break;
    case DOMNode::COMMENT_NODE:
      FWKINFO("Node type: COMMENT_NODE");
      break;
    case DOMNode::DOCUMENT_NODE:
      FWKINFO("Node type: DOCUMENT_NODE");
      break;
    case DOMNode::DOCUMENT_TYPE_NODE:
      FWKINFO("Node type: DOCUMENT_TYPE_NODE");
      break;
    case DOMNode::DOCUMENT_FRAGMENT_NODE:
      FWKINFO("Node type: DOCUMENT_FRAGMENT_NODE");
      break;
    case DOMNode::NOTATION_NODE:
      FWKINFO("Node type: NOTATION_NODE");
      break;
    default:
      FWKINFO("Node type: " << node->getNodeType());
      break;
  }

  return count;
}

void TestDriver::fromXmlNode(const DOMNode* node) {
  //  FWKINFO( "Instantiate TestDriver" );
  //  int32_t count = traverseChildElements( node );
  //  FWKINFO( "Total of " << count << " elements." );
  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      std::string name = XMLChToStr(child->getNodeName());
      if (name == LOCALFILE_TAG) {
        addLocalFile(new LocalFile(child));
      } else if (name == DATA_TAG) {
        addData(new FwkData(child));
      } else if (name == DATASET_TAG) {
        addDataSet(new FwkDataSet(child));
      } else if (name == CLIENTSET_TAG) {
        addClientSet(new FwkClientSet(child));
      } else if (name == TEST_TAG) {
        addTest(new FwkTest(child));
      } else if (name == HOSTGROUP_TAG) {
        DOMNamedNodeMap* map = child->getAttributes();
        if (map != NULL) {
          DOMNode* tag = map->getNamedItem(StrToXMLCh("tag"));
          if (tag != NULL) {
            addHostGroup(XMLChToStr(tag->getNodeValue()));
          }
        }
      }
    }
    child = child->getNextSibling();
  }
}

FwkTest::FwkTest(const DOMNode* node) : m_waitTime(0), m_timesToRun(1) {
  //  FWKINFO( "Instantiate FwkTest" );
  //  traverseChildElements( node );
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("name"));
    if (nameNode != NULL) {
      setName(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("waitTime"));
    if (nameNode != NULL) {
      setWaitTime(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("description"));
    if (nameNode != NULL) {
      setDescription(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("timesToRun"));
    if (nameNode != NULL) {
      setTimesToRun(XMLChToStr(nameNode->getNodeValue()));
    }
  }

  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      addTask(new FwkTask(child));
    }
    child = child->getNextSibling();
  }
}

void PersistManager::addProperty(const DOMNode* node) {
  //  FWKINFO( "Instantiate addProperty" );
  //  traverseChildElements( node );
  DOMNamedNodeMap* map = node->getAttributes();
  std::string name;
  std::string value;
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("name"));
    if (nameNode != NULL) {
      name = XMLChToStr(nameNode->getNodeValue());
    }

    nameNode = map->getNamedItem(StrToXMLCh("value"));
    if (nameNode != NULL) {
      value = XMLChToStr(nameNode->getNodeValue());
    }
    m_properties->insert(name.c_str(), value.c_str());
  }
}

void PersistManager::addProperties(const DOMNode* node) {
  //  FWKINFO( "Instantiate addProperties" );
  //  traverseChildElements( node );
  m_properties = Properties::create();

  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      std::string name = XMLChToStr(child->getNodeName());
      if (name == PROPERTY_TAG) {
        addProperty(child);
      }
    }
    child = child->getNextSibling();
  }
}

PersistManager::PersistManager(const DOMNode* node) {
  //  FWKINFO( "Instantiate PersistManager" );
  //  traverseChildElements( node );
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("library"));
    if (nameNode != NULL) {
      setLibraryName(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("function"));
    if (nameNode != NULL) {
      setLibraryFunctionName(XMLChToStr(nameNode->getNodeValue()));
    }
  }

  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      std::string name = XMLChToStr(child->getNodeName());
      if (name == PROPERTIES_TAG) {
        addProperties(child);
      }
    }
    child = child->getNextSibling();
  }
}

FwkTask::FwkTask(const DOMNode* node)
    : m_waitTime(0),
      m_timesToRun(0),
      m_threadCount(1),
      m_parallel(false),
      m_timesRan(0),
      m_continue(false),
      m_clientSet(NULL),
      m_dataSet(NULL),
      m_parent(NULL) {
  //  FWKINFO( "Instantiate FwkTask" );
  //  traverseChildElements( node );
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("name"));
    if (nameNode != NULL) {
      setName(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("action"));
    if (nameNode != NULL) {
      setAction(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("class"));
    if (nameNode != NULL) {
      setClass(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("container"));
    if (nameNode != NULL) {
      setContainer(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("waitTime"));
    if (nameNode != NULL) {
      setWaitTime(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("timesToRun"));
    if (nameNode != NULL) {
      setTimesToRun(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("threadCount"));
    if (nameNode != NULL) {
      setThreadCount(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("parallel"));
    if (nameNode != NULL) {
      setParallel(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("continueOnError"));
    if (nameNode != NULL) {
      setContinueOnError(XMLChToStr(nameNode->getNodeValue()));
    }
  }

  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      std::string name = XMLChToStr(child->getNodeName());
      if (name == CLIENTSET_TAG) {
        addClientSet(new FwkClientSet(child));
      } else if (name == DATA_TAG) {
        addData(new FwkData(child));
      }
    }
    child = child->getNextSibling();
  }
}

LocalFile::LocalFile(const DOMNode* node) : m_append(false) {
  //  FWKINFO( "Instantiate LocalFile" );
  //  traverseChildElements( node );
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("name"));
    if (nameNode != NULL) {
      setName(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("append"));
    if (nameNode != NULL) {
      setAppend(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("description"));
    if (nameNode != NULL) {
      setDescription(XMLChToStr(nameNode->getNodeValue()));
    }
  }

  bool done = false;
  DOMNode* child = node->getFirstChild();
  while ((child != NULL) && !done) {
    if (child->getNodeType() == DOMNode::TEXT_NODE) {
      //      DOMText * tnode = dynamic_cast< DOMText * >( child );
      DOMText* tnode = (DOMText*)child;
      std::string text = XMLChToStr(tnode->getNodeValue());
      if (!tnode->isIgnorableWhitespace() && !allSpace(text)) {
        setContent(text);
        done = true;
      }
    }
    child = child->getNextSibling();
  }
}

FwkClientSet::FwkClientSet(const DOMNode* node)
    : m_exclude(false),
      m_count(1),
      m_begin(1),
      m_hostGroup(m_defaultGroup),
      m_remaining(false) {
  //  FWKINFO( "Instantiate FwkClientSet" );
  //  traverseChildElements( node );
  std::string value;
  DOMNode* attNode;
  std::string name;
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    attNode = map->getNamedItem(StrToXMLCh("name"));
    if (attNode != NULL) {
      value = XMLChToStr(attNode->getNodeValue());
    }
  }
  if (value.empty()) {
    name = GsRandom::getAlphanumericString(20);
  } else {
    name = value;
  }
  setName(name);

  if (map != NULL) {
    attNode = map->getNamedItem(StrToXMLCh("exclude"));
    if (attNode != NULL) {
      setExclude(XMLChToStr(attNode->getNodeValue()));
    }
    attNode = map->getNamedItem(StrToXMLCh("count"));
    if (attNode != NULL) {
      setCount(XMLChToStr(attNode->getNodeValue()));
    }
    attNode = map->getNamedItem(StrToXMLCh("begin"));
    if (attNode != NULL) {
      setBegin(XMLChToStr(attNode->getNodeValue()));
    }
    attNode = map->getNamedItem(StrToXMLCh("hostGroup"));
    if (attNode != NULL) {
      setHostGroup(XMLChToStr(attNode->getNodeValue()));
    }
    attNode = map->getNamedItem(StrToXMLCh("remaining"));
    if (attNode != NULL) {
      setRemaining(XMLChToStr(attNode->getNodeValue()));
    }
  }

  int32_t childCnt = 0;
  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      addClient(new FwkClient(child));
      childCnt++;
    }
    child = child->getNextSibling();
  }

  if (childCnt == 0) {
    int32_t count = getCount();  // defaults to one
    int32_t begin = getBegin();  // defaults to one
    for (int32_t i = begin; i < count + begin; i++) {
      std::ostringstream cnam;
      cnam << name << "_" << i;
      addClient(new FwkClient(cnam.str()));
    }
  }
}

FwkClient::FwkClient(const DOMNode* node)
    : m_program(NULL), m_arguments(NULL), m_remaining(false) {
  //  FWKINFO( "Instantiate FwkClient" );
  //  traverseChildElements( node );
  std::string value;
  DOMNode* attNode;
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    attNode = map->getNamedItem(StrToXMLCh("name"));
    if (attNode != NULL) {
      value = XMLChToStr(attNode->getNodeValue());
    }
  }
  if (value.empty()) {
    setName(GsRandom::getAlphanumericString(20));
  } else {
    setName(value);
  }

  if (map != NULL) {
    attNode = map->getNamedItem(StrToXMLCh("program"));
    if (attNode != NULL) {
      setProgram(XMLChToStr(attNode->getNodeValue()));
    }
    attNode = map->getNamedItem(StrToXMLCh("arguments"));
    if (attNode != NULL) {
      setArguments(XMLChToStr(attNode->getNodeValue()));
    }
  }
}

FwkRegion::FwkRegion(const DOMNode* node) : m_attributes(NULL) {
  //  FWKINFO( "Instantiate FwkRegion" );
  //  traverseChildElements( node );
  std::string name;
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("name"));
    if (nameNode != NULL) {
      setName(XMLChToStr(nameNode->getNodeValue()));
    }
  }

  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      setAttributes(new Attributes(child));
    }
    child = child->getNextSibling();
  }
}

Attributes::Attributes(const DOMNode* node)
    : m_isLocal(false), m_withPool(false) {
  //  FWKINFO( "Instantiate Attributes" );
  //  traverseChildElements( node );
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("caching-enabled"));
    if (nameNode != NULL) {
      setCachingEnabled(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("load-factor"));
    if (nameNode != NULL) {
      setLoadFactor(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("concurrency-level"));
    if (nameNode != NULL) {
      setConcurrencyLevel(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("lru-entries-limit"));
    if (nameNode != NULL) {
      setLruEntriesLimit(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("initial-capacity"));
    if (nameNode != NULL) {
      setInitialCapacity(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("disk-policy"));
    if (nameNode != NULL) {
      setDiskPolicy(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("pool-name"));
    if (nameNode != NULL) {
      setPoolName(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("cloning-enabled"));
    if (nameNode != NULL) {
      setCloningEnabled(XMLChToStr(nameNode->getNodeValue()));
    }
    nameNode = map->getNamedItem(StrToXMLCh("concurrency-checks-enabled"));
    if (nameNode != NULL) {
      setConcurrencyCheckEnabled(XMLChToStr(nameNode->getNodeValue()));
    }
  }

  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      std::string name = XMLChToStr(child->getNodeName());
      if (name == REGIONTIMETOLIVE_TAG) {
        setRegionTimeToLive(getExpiryAttributes(child));
      } else if (name == REGIONIDLETIME_TAG) {
        setRegionIdleTime(getExpiryAttributes(child));
      } else if (name == ENTRYTIMETOLIVE_TAG) {
        setEntryTimeToLive(getExpiryAttributes(child));
      } else if (name == ENTRYIDLETIME_TAG) {
        setEntryIdleTime(getExpiryAttributes(child));
      } else if (name == CACHELOADER_TAG) {
        setCacheLoader(new ActionPair(child));
      } else if (name == CACHELISTENER_TAG) {
        setCacheListener(new ActionPair(child));
      } else if (name == CACHEWRITER_TAG) {
        setCacheWriter(new ActionPair(child));
      } else if (name == PERSISTENCEMANAGER_TAG) {
        setPersistenceManager(new PersistManager(child));
      }
    }
    child = child->getNextSibling();
  }
}

FwkPool::FwkPool(const DOMNode* node) : m_locators(false), m_servers(false) {
  // Init Factory
  m_poolFactory = PoolManager::createFactory();
  // Set Attrs to Pool
  setAttributesToFactory(node);

  /*DOMNode * child = node->getFirstChild();
  while ( child != NULL ) {
    if ( child->getNodeType() == DOMNode::ELEMENT_NODE ) {
    }
    child = child->getNextSibling();
  }*/
}

void FwkPool::setAttributesToFactory(const DOMNode* node) {
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("name"));
    if (nameNode != NULL) {
      setName(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("free-connection-timeout"));
    if (nameNode != NULL) {
      setFreeConnectionTimeout(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("idle-timeout"));
    if (nameNode != NULL) {
      setIdleTimeout(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("load-conditioning-interval"));
    if (nameNode != NULL) {
      setLoadConditioningInterval(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("max-connections"));
    if (nameNode != NULL) {
      setMaxConnections(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("min-connections"));
    if (nameNode != NULL) {
      setMinConnections(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("ping-interval"));
    if (nameNode != NULL) {
      setPingInterval(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("read-timeout"));
    if (nameNode != NULL) {
      setReadTimeout(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("retry-attempts"));
    if (nameNode != NULL) {
      setRetryAttempts(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("server-group"));
    if (nameNode != NULL) {
      setServerGroup(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("socket-buffer-size"));
    if (nameNode != NULL) {
      setSocketBufferSize(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("subscription-ack-interval"));
    if (nameNode != NULL) {
      setSubscriptionAckInterval(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("subscription-enabled"));
    if (nameNode != NULL) {
      setSubscriptionEnabled(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode =
        map->getNamedItem(StrToXMLCh("subscription-message-tracking-timeout"));
    if (nameNode != NULL) {
      setSubscriptionMessageTrackingTimeout(
          XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("subscription-redundancy"));
    if (nameNode != NULL) {
      setSubscriptionRedundancy(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("thread-local-connections"));
    if (nameNode != NULL) {
      setThreadLocalConnections(XMLChToStr(nameNode->getNodeValue()));
    }
    nameNode = map->getNamedItem(StrToXMLCh("pr-single-hop-enabled"));
    if (nameNode != NULL) {
      setPRSingleHopEnabled(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("locators"));
    if (nameNode != NULL) {
      setLocatorsFlag(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("servers"));
    if (nameNode != NULL) {
      setServersFlag(XMLChToStr(nameNode->getNodeValue()));
    }
  }
}

ActionPair::ActionPair(const DOMNode* node) {
  //  FWKINFO( "Instantiate ActionPair" );
  //  traverseChildElements( node );
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("library"));
    if (nameNode != NULL) {
      setLibraryName(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("function"));
    if (nameNode != NULL) {
      setLibraryFunctionName(XMLChToStr(nameNode->getNodeValue()));
    }
  }
}

ExpiryAttributes* Attributes::getExpiryAttributes(const DOMNode* node) {
  //  FWKINFO( "Instantiate getExpiryAttributes" );
  //  traverseChildElements( node );
  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      return new ExpiryAttributes(child);
    }
    child = child->getNextSibling();
  }
  return NULL;
}

ExpiryAttributes::ExpiryAttributes(const DOMNode* node) : m_timeout(0) {
  //  FWKINFO( "Instantiate ExpiryAttributes" );
  //  traverseChildElements( node );
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("timeout"));
    if (nameNode != NULL) {
      setTimeout(XMLChToStr(nameNode->getNodeValue()));
    }

    nameNode = map->getNamedItem(StrToXMLCh("action"));
    if (nameNode != NULL) {
      setAction(XMLChToStr(nameNode->getNodeValue()));
    }
  }
}

FwkDataSet::FwkDataSet(const DOMNode* node) {
  //  FWKINFO( "Instantiate FwkDataSet" );
  //  traverseChildElements( node );
  std::string name;
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("name"));
    if (nameNode != NULL) {
      name = XMLChToStr(nameNode->getNodeValue());
    }
  }
  if (name.empty()) {
    setName(GsRandom::getAlphanumericString(20));
  } else {
    setName(name);
  }

  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      add(new FwkData(child));
    }
    child = child->getNextSibling();
  }
}

FwkData::FwkData(const DOMNode* node)
    : m_dataList(NULL),
      m_dataOneof(NULL),
      m_dataRange(NULL),
      m_snippet(NULL),
      m_dataType(DATA_TYPE_NULL) {
  //  FWKINFO( "Instantiate FwkData" );
  //  traverseChildElements( node );
  std::string name;
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* nameNode = map->getNamedItem(StrToXMLCh("name"));
    if (nameNode != NULL) {
      name = XMLChToStr(nameNode->getNodeValue());
    }
  }
  if (name.empty()) {
    setName(GsRandom::getAlphanumericString(20));
  } else {
    setName(name);
  }

  DOMNode* child = node->getFirstChild();
  while ((child != NULL) && (m_dataType == DATA_TYPE_NULL)) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      name = XMLChToStr(child->getNodeName());
      if (name == LIST_TAG) {
        setList(new DataList(child));
      } else if (name == ONEOF_TAG) {
        setOneof(new DataOneof(child));
      } else if (name == RANGE_TAG) {
        setRange(new DataRange(child));
      } else if (name == SNIPPET_TAG) {
        setSnippet(new DataSnippet(child));
      }
    } else if (child->getNodeType() == DOMNode::TEXT_NODE) {
      //      DOMText * tnode = dynamic_cast< DOMText * >( child );
      DOMText* tnode = (DOMText*)child;
      std::string text = XMLChToStr(tnode->getNodeValue());
      if (!tnode->isIgnorableWhitespace() && !allSpace(text)) {
        setContent(text);
      }
    }
    child = child->getNextSibling();
  }
}

DataRange::DataRange(const DOMNode* node) {
  //  FWKINFO( "Instantiate DataRange" );
  //  traverseChildElements( node );
  DOMNamedNodeMap* map = node->getAttributes();
  if (map != NULL) {
    DOMNode* attNode = map->getNamedItem(StrToXMLCh("low"));
    if (attNode != NULL) {
      setLow(XMLChToStr(attNode->getNodeValue()));
    }
    attNode = map->getNamedItem(StrToXMLCh("high"));
    if (attNode != NULL) {
      setHigh(XMLChToStr(attNode->getNodeValue()));
    }
  }
}

DataSnippet::DataSnippet(const DOMNode* node) : m_region(NULL), m_pool(NULL) {
  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      std::string name = XMLChToStr(child->getNodeName());
      if (name == REGION_TAG) {
        setRegion(new FwkRegion(child));
      } else {
        setPool(new FwkPool(child));
      }
    }
    child = child->getNextSibling();
  }
}

DataList::DataList(const DOMNode* node) {
  //  FWKINFO( "Instantiate DataList" );
  //  traverseChildElements( node );
  // for all children, process ITEM_TAG elements
  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      std::string tag = XMLChToStr(child->getNodeName());
      if (tag == ITEM_TAG) {  // now find all TEXT_NODE children of item
        DOMNode* tchild = child->getFirstChild();
        while (tchild != NULL) {
          if (tchild->getNodeType() == DOMNode::TEXT_NODE) {
            //            DOMText * tnode = dynamic_cast< DOMText * >( tchild );
            DOMText* tnode = (DOMText*)tchild;
            std::string text = XMLChToStr(tnode->getNodeValue());
            if (!tnode->isIgnorableWhitespace() && !allSpace(text)) {
              addValue(text);
            }
          }
          tchild = tchild->getNextSibling();
        }
      }
    }
    child = child->getNextSibling();
  }
}

DataOneof::DataOneof(const DOMNode* node) {
  //  FWKINFO( "Instantiate DataOneof" );
  //  traverseChildElements( node );
  // for all children, process ITEM_TAG elements
  DOMNode* child = node->getFirstChild();
  while (child != NULL) {
    if (child->getNodeType() == DOMNode::ELEMENT_NODE) {
      std::string tag = XMLChToStr(child->getNodeName());
      if (tag == ITEM_TAG) {  // now find all TEXT_NODE children of item
        DOMNode* tchild = child->getFirstChild();
        while (tchild != NULL) {
          if (tchild->getNodeType() == DOMNode::TEXT_NODE) {
            //            DOMText * tnode = dynamic_cast< DOMText * >( tchild );
            DOMText* tnode = (DOMText*)tchild;
            std::string text = XMLChToStr(tnode->getNodeValue());
            if (!tnode->isIgnorableWhitespace() && !allSpace(text)) {
              addValue(text);
            }
          }
          tchild = tchild->getNextSibling();
        }
      }
    }
    child = child->getNextSibling();
  }
}

bool FwkDomErrorHandler::handleError(const DOMError& domError) {
  m_hadErrors = true;
  std::string sev = "Fatal Error";
  if (domError.getSeverity() == DOMError::DOM_SEVERITY_WARNING) {
    sev = "Warning";
  } else {
    if (domError.getSeverity() == DOMError::DOM_SEVERITY_ERROR) {
      sev = "Error";
    }
  }

  FWKSEVERE(sev << " in file " << XMLChToStr(domError.getLocation()->getURI())
                << ", line: " << domError.getLocation()->getLineNumber()
                << ", col: " << domError.getLocation()->getColumnNumber()
                << "  " << XMLChToStr(domError.getMessage()));

  return true;
}

TestDriver::TestDriver(const char* xmlfile) {
  // Initialize the XML4C system
  try {
    XMLPlatformUtils::Initialize();
  } catch (const XMLException& ex) {
    FWKEXCEPTION("TestDriver::fromXmlFile: During init: caught XML exception: "
                 << ex.getMessage());
  }

  // Instantiate the DOM pDomBuilder.
  static const XMLCh ls[] = {chLatin_L, chLatin_S, chNull};
  DOMImplementation* domImpl =
      DOMImplementationRegistry::getDOMImplementation(ls);

  if (domImpl == NULL) {
    FWKEXCEPTION("TestDriver::fromXmlFile() Failed to instantiate domImpl.");
  }

  DOMLSParser* builder =
      ((DOMImplementationLS*)domImpl)
          ->createLSParser(DOMImplementationLS::MODE_SYNCHRONOUS, 0);

  if (builder == NULL) {
    FWKEXCEPTION(
        "TestDriver::fromXmlFile() Failed to instantiate dom builder.");
  }

  builder->getDomConfig()->setParameter(XMLUni::fgDOMNamespaces, true);
  builder->getDomConfig()->setParameter(XMLUni::fgDOMDatatypeNormalization,
                                        true);
  builder->getDomConfig()->setParameter(XMLUni::fgXercesSchema, true);
  builder->getDomConfig()->setParameter(XMLUni::fgXercesSchemaFullChecking,
                                        true);
  //  builder->getDomConfig()->setParameter( XMLUni::fgDOMValidateIfSchema, true
  //  );
  builder->getDomConfig()->setParameter(XMLUni::fgDOMValidate, true);
  builder->getDomConfig()->setParameter(XMLUni::fgDOMElementContentWhitespace,
                                        false);

  // And create our error handler and install it
  FwkDomErrorHandler errorHandler;
  builder->getDomConfig()->setParameter(XMLUni::fgDOMErrorHandler,
                                        &errorHandler);

  XERCES_CPP_NAMESPACE_QUALIFIER DOMDocument* doc = 0;

  // Does the cache file exist?
  if (ACE_OS::access(xmlfile, F_OK) == -1) {
    FWKEXCEPTION("TestDriver::fromXmlFile() Failed to find file: " << xmlfile);
  }

  try {
    builder->resetDocumentPool();
    doc = builder->parseURI(xmlfile);
  } catch (const XMLException& ex) {
    FWKEXCEPTION("TestDriver::fromXmlFile() Failed to parse file: "
                 << xmlfile << " Caught XML exception: " << ex.getMessage());
  } catch (const DOMException& ex) {
    XMLCh errText[2048];

    if (DOMImplementation::loadDOMExceptionMsg(ex.code, errText, 2047)) {
      FWKSEVERE("TestDriver::fromXmlFile() DOM exception during parse: "
                << XMLChToStr(errText));
    }
    FWKEXCEPTION("TestDriver::fromXmlFile() Failed to parse file: " << xmlfile);
  } catch (...) {
    FWKEXCEPTION("TestDriver::fromXmlFile() Failed to parse file: "
                 << xmlfile << " Caught unknown exception.");
  }

  DOMNode* node = (DOMNode*)doc->getDocumentElement();
  if (node != NULL) {
    fromXmlNode(node);
  }

  builder->release();
  XMLPlatformUtils::Terminate();
}

void FwkTask::setKey(int32_t cnt) {
  if (!m_id.empty()) return;
  std::ostringstream ostr;
  ostr << m_parent->getKey() << "::" << getName() << "::" << cnt;
  m_id = ostr.str();
}

/** brief Get FwkDataSet pointer */
const FwkDataSet* FwkTest::getDataSet(const char* name) const {
  return m_parent->getDataSet(name);
}

/** brief Get FwkData pointer */
const FwkData* FwkTest::getData(const char* name) const {
  return m_parent->getData(name);
}

/** brief Get FwkDataSet pointer */
const FwkDataSet* FwkTask::getDataSet(const char* name) const {
  return m_parent->getDataSet(name);
}

/** brief Get FwkData pointer */
const FwkData* FwkTask::getData(const char* name) const {
  const FwkData* data = NULL;

  if (m_dataSet) {
    data = m_dataSet->find(name);
  }

  if ((data == NULL) && m_parent) {  // ask our parent
    data = m_parent->getData(name);
  }

  return data;
}
