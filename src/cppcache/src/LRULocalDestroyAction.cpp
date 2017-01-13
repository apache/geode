#include "LRULocalDestroyAction.hpp"
#include "LRUEntriesMap.hpp"
#include "CacheImpl.hpp"

using namespace gemfire;

bool LRULocalDestroyAction::evict(const MapEntryImplPtr& mePtr) {
  CacheableKeyPtr keyPtr;
  mePtr->getKeyI(keyPtr);
  VersionTagPtr versionTag;
  //  we should invoke the destroyNoThrow with appropriate
  // flags to correctly invoke listeners
  LOGDEBUG("LRULocalDestroy: evicting entry with key [%s]",
           Utils::getCacheableKeyString(keyPtr)->asChar());
  GfErrType err = m_regionPtr->destroyNoThrow(
      keyPtr, NULLPTR, -1, CacheEventFlags::EVICTION | CacheEventFlags::LOCAL,
      versionTag);
  return (err == GF_NOERR);
}
