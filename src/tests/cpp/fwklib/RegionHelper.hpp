/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#ifndef __REGION_HELPER_HPP__
#define __REGION_HELPER_HPP__

#include <gfcpp/GemfireCppCache.hpp>
#include <AtomicInc.hpp>
#include "fwklib/FrameworkTest.hpp"
#include "fwklib/FwkObjects.hpp"
#include "fwklib/FwkStrCvt.hpp"
#include "fwklib/FwkLog.hpp"
#include <stdlib.h>

#include <string>
#include <map>

namespace gemfire {
namespace testframework {

// ----------------------------------------------------------------------------

/** @class RegionHelper
  * @brief Class used to define a valid combination of attributes and
  * specifications for a region. Since some combinations are illegal,
  * this gives a test an easier way to consider all valid combinations.
  */
class RegionHelper {
  const FwkRegion* m_region;
  std::string m_spec;

 public:
  /** Fill in this instance of RegionHelper based on the spec named by sname.
   */
  RegionHelper(const FrameworkTest* test) : m_region(NULL) {
    m_spec = test->getStringValue("regionSpec");
    if (m_spec.empty()) {
      FWKEXCEPTION("Failed to find regionSpec definition.");
    }
    m_region = test->getSnippet(m_spec);
    if (m_region == NULL) {
      FWKEXCEPTION("Failed to find region definition.");
    }
  }

  std::string regionAttributesToString() {
    RegionAttributesPtr atts = m_region->getAttributesPtr();
    return regionAttributesToString(atts);
  }

  std::string regionName() { return m_region->getName(); }

  const std::string specName() { return m_spec; }

  std::string regionTag() {
    RegionAttributesPtr atts = m_region->getAttributesPtr();
    return regionTag(atts);
  }

  static std::string regionTag(RegionAttributesPtr attr) {
    std::string sString;

    sString += attr->getCachingEnabled() ? "Caching" : "NoCache";
    sString += (attr->getCacheListener() == NULLPTR) ? "Nlstnr" : "Lstnr";
    return sString;
  }

  /** @brief Given RegionAttributes, return a string logging its configuration.
    *  @param attr Return a string describing this region.
    *  @retval A String representing aRegion.
    */
  static std::string regionAttributesToString(RegionAttributesPtr& attr) {
    std::string sString;

    sString += "\ncaching: ";
    sString += attr->getCachingEnabled() ? "Enabled" : "Disabled";
    sString += "\nendpoints: ";
    sString += FwkStrCvt(attr->getEndpoints()).toString();
    sString += "\nclientNotification: ";
    sString += attr->getClientNotificationEnabled() ? "Enabled" : "Disabled";
    sString += "\ninitialCapacity: ";
    sString += FwkStrCvt(attr->getInitialCapacity()).toString();
    sString += "\nloadFactor: ";
    sString += FwkStrCvt(attr->getLoadFactor()).toString();
    sString += "\nconcurrencyLevel: ";
    sString += FwkStrCvt(attr->getConcurrencyLevel()).toString();
    sString += "\nlruEntriesLimit: ";
    sString += FwkStrCvt(attr->getLruEntriesLimit()).toString();
    sString += "\nlruEvictionAction: ";
    sString += ExpirationAction::fromOrdinal(attr->getLruEvictionAction());
    sString += "\nentryTimeToLive: ";
    sString += FwkStrCvt(attr->getEntryTimeToLive()).toString();
    sString += "\nentryTimeToLiveAction: ";
    sString += ExpirationAction::fromOrdinal(attr->getEntryTimeToLiveAction());
    sString += "\nentryIdleTimeout: ";
    sString += FwkStrCvt(attr->getEntryIdleTimeout()).toString();
    sString += "\nentryIdleTimeoutAction: ";
    sString += ExpirationAction::fromOrdinal(attr->getEntryIdleTimeoutAction());
    sString += "\nregionTimeToLive: ";
    sString += FwkStrCvt(attr->getRegionTimeToLive()).toString();
    sString += "\nregionTimeToLiveAction: ";
    sString += ExpirationAction::fromOrdinal(attr->getRegionTimeToLiveAction());
    sString += "\nregionIdleTimeout: ";
    sString += FwkStrCvt(attr->getRegionIdleTimeout()).toString();
    sString += "\nregionIdleTimeoutAction: ";
    sString +=
        ExpirationAction::fromOrdinal(attr->getRegionIdleTimeoutAction());
    sString += "\npoolName: ";
    sString += FwkStrCvt(attr->getPoolName()).toString();
    sString += "\nCacheLoader: ";
    sString += (attr->getCacheLoaderLibrary() != NULL &&
                attr->getCacheLoaderFactory() != NULL)
                   ? "Enabled"
                   : "Disabled";
    sString += "\nCacheWriter: ";
    sString += (attr->getCacheWriterLibrary() != NULL &&
                attr->getCacheWriterFactory() != NULL)
                   ? "Enabled"
                   : "Disabled";
    sString += "\nCacheListener: ";
    sString += (attr->getCacheListenerLibrary() != NULL &&
                attr->getCacheListenerFactory() != NULL)
                   ? "Enabled"
                   : "Disabled";
    sString += "\nConcurrencyChecksEnabled: ";
    sString += attr->getConcurrencyChecksEnabled() ? "Enabled" : "Disabled";
    sString += "\n";

    return sString;
  }
  void setRegionAttributes(RegionFactoryPtr& regionFac) {
    RegionAttributesPtr atts = m_region->getAttributesPtr();
    regionFac->setCachingEnabled(atts->getCachingEnabled());
    if (atts->getCacheListenerLibrary() != NULL &&
        atts->getCacheListenerFactory() != NULL) {
      regionFac->setCacheListener(atts->getCacheListenerLibrary(),
                                  atts->getCacheListenerFactory());
    }
    if (atts->getCacheLoaderLibrary() != NULL &&
        atts->getCacheLoaderFactory() != NULL) {
      regionFac->setCacheLoader(atts->getCacheLoaderLibrary(),
                                atts->getCacheLoaderFactory());
    }
    if (atts->getCacheWriterLibrary() != NULL &&
        atts->getCacheWriterFactory() != NULL) {
      regionFac->setCacheWriter(atts->getCacheWriterLibrary(),
                                atts->getCacheWriterFactory());
    }
    if (atts->getEntryIdleTimeout() != 0) {
      regionFac->setEntryIdleTimeout(atts->getEntryIdleTimeoutAction(),
                                     atts->getEntryIdleTimeout());
    }
    if (atts->getEntryTimeToLive() != 0) {
      regionFac->setEntryTimeToLive(atts->getEntryTimeToLiveAction(),
                                    atts->getEntryTimeToLive());
    }
    if (atts->getRegionIdleTimeout() != 0) {
      regionFac->setRegionIdleTimeout(atts->getRegionIdleTimeoutAction(),
                                      atts->getRegionIdleTimeout());
    }
    if (atts->getRegionTimeToLive() != 0) {
      regionFac->setRegionTimeToLive(atts->getRegionTimeToLiveAction(),
                                     atts->getRegionTimeToLive());
    }
    if (atts->getPartitionResolverLibrary() != NULL &&
        atts->getPartitionResolverFactory() != NULL) {
      regionFac->setPartitionResolver(atts->getPartitionResolverLibrary(),
                                      atts->getPartitionResolverFactory());
    }
    if (atts->getPersistenceLibrary() != NULL &&
        atts->getPersistenceFactory() != NULL) {
      regionFac->setPersistenceManager(atts->getPersistenceLibrary(),
                                       atts->getPersistenceFactory(),
                                       atts->getPersistenceProperties());
    }
    regionFac->setInitialCapacity(atts->getInitialCapacity());
    regionFac->setLoadFactor(atts->getLoadFactor());
    regionFac->setConcurrencyLevel(atts->getConcurrencyLevel());
    regionFac->setLruEntriesLimit(atts->getLruEntriesLimit());
    regionFac->setDiskPolicy(atts->getDiskPolicy());
    regionFac->setCloningEnabled(atts->getCloningEnabled());
    regionFac->setPoolName(atts->getPoolName());
    regionFac->setConcurrencyChecksEnabled(atts->getConcurrencyChecksEnabled());
  }

  RegionPtr createRootRegion(CachePtr& cachePtr) {
    RegionPtr region;
    std::string regionName = m_region->getName();
    if (regionName.empty()) {
      FWKEXCEPTION("Region name not specified.");
    }
    return createRootRegion(cachePtr, regionName);
  }

  RegionPtr createRootRegion(CachePtr& cachePtr, std::string regionName) {
    RegionPtr region;
    RegionFactoryPtr regionFac;
    if (regionName.empty()) {
      regionName = m_region->getName();
      FWKINFO("region name is " << regionName);
      if (regionName.empty()) {
        FWKEXCEPTION("Region name not specified.");
      }
    }
    regionFac = cachePtr->createRegionFactory(CACHING_PROXY);
    setRegionAttributes(regionFac);
    RegionAttributesPtr atts = m_region->getAttributesPtr();
    /*   if(atts->getCachingEnabled())
       {
         regionFac=cachePtr->createRegionFactory(CACHING_PROXY);
         regionFac->setCachingEnabled(true);
         FWKINFO("Setting CachingEnabled=true");
       }
       else
       {
         regionFac=cachePtr->createRegionFactory(PROXY);
         regionFac->setCachingEnabled(false);
         FWKINFO("Setting CachingEnabled=false");
       }*/
    // UNUSED bool hasEndpoints = ( NULL != atts->getEndpoints() ) ? true :
    // false;
    bool withPool = (NULL != atts->getPoolName()) ? true : false;
    std::string poolName;
    if (withPool)
      poolName = atts->getPoolName();
    else
      poolName = "";

    region = regionFac->create(regionName.c_str());
    FWKINFO("Region created with name = " << regionName + " and pool name= "
                                          << poolName);
    FWKINFO(" Region Created with following attributes :"
            << regionAttributesToString());
    return region;
  }
};

}  // namespace testframework
}  // namespace gemfire

// ----------------------------------------------------------------------------

#endif  // __REGION_HELPER_HPP__
