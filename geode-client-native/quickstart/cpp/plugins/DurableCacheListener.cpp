#include "DurableCacheListener.hpp"

void DurableCacheListener::afterRegionLive(const RegionEvent& event)
{
    LOGINFO("DurableCacheListener: Got an afterRegionLive event.");
}
void DurableCacheListener::afterCreate(const EntryEvent& event)
{
  CacheableStringPtr key = dynCast<CacheableStringPtr>(event.getKey());
  LOGINFO("DurableCacheListener: Got an afterCreate event for key: %s ", key->toString());
}

void DurableCacheListener::afterUpdate(const EntryEvent& event)
{
  CacheableStringPtr key = dynCast<CacheableStringPtr>(event.getKey());
  LOGINFO("DurableCacheListener: Got an afterUpdate event for key: %s ", key->toString());
}
