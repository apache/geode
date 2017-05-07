#include "SimpleCacheWriter.hpp"

bool SimpleCacheWriter::beforeUpdate(const EntryEvent& event)
{
  LOGINFO("SimpleCacheWriter: Got a beforeUpdate event.");
  return true;
}

bool SimpleCacheWriter::beforeCreate(const EntryEvent& event)
{
  LOGINFO("SimpleCacheWriter: Got a beforeCreate event.");
  return true;
}

void SimpleCacheWriter::beforeInvalidate(const EntryEvent& event)
{
  LOGINFO("SimpleCacheWriter: Got a beforeInvalidate event.");
}

bool SimpleCacheWriter::beforeDestroy(const EntryEvent& event) 
{
  LOGINFO("SimpleCacheWriter: Got a beforeDestroy event.");
  return true;
}

void SimpleCacheWriter::beforeRegionInvalidate(const RegionEvent& event)
{
  LOGINFO("SimpleCacheWriter: Got a beforeRegionInvalidate event.");
}

bool SimpleCacheWriter::beforeRegionDestroy(const RegionEvent& event)
{
  LOGINFO("SimpleCacheWriter: Got a beforeRegionDestroy event.");
  return true;
}

void SimpleCacheWriter::close(const RegionPtr& region)
{
  LOGINFO("SimpleCacheWriter: Got a close event.");
}
