#include "SimpleCacheListener.hpp"

void SimpleCacheListener::afterCreate(const EntryEvent& event)
{
  LOGINFO("SimpleCacheListener: Got an afterCreate event.");
}

void SimpleCacheListener::afterUpdate(const EntryEvent& event)
{
  LOGINFO("SimpleCacheListener: Got an afterUpdate event.");
}

void SimpleCacheListener::afterInvalidate(const EntryEvent& event)
{
  LOGINFO("SimpleCacheListener: Got an afterInvalidate event.");
}

void SimpleCacheListener::afterDestroy(const EntryEvent& event) 
{
  LOGINFO("SimpleCacheListener: Got an afterDestroy event.");
}

void SimpleCacheListener::afterRegionInvalidate(const RegionEvent& event)
{
  LOGINFO("SimpleCacheListener: Got an afterRegionInvalidate event.");
}

void SimpleCacheListener::afterRegionDestroy(const RegionEvent& event)
{
  LOGINFO("SimpleCacheListener: Got an afterRegionDestroy event.");
}

void SimpleCacheListener::close(const RegionPtr& region)
{
  LOGINFO("SimpleCacheListener: Got a close event.");
}
