#include "SimpleCacheLoader.hpp"

CacheablePtr SimpleCacheLoader::load(
  const RegionPtr& region, 
  const CacheableKeyPtr& key, 
  const UserDataPtr& aCallbackArgument)
{
  LOGINFO("SimpleCacheLoader: Got a load event.");
  
  CacheablePtr value = CacheableString::create("LoaderValue");
  
  return value; 
}

void SimpleCacheLoader::close( const RegionPtr& region )
{
  LOGINFO("SimpleCacheLoader: Got a close event.");
}

// ----------------------------------------------------------------------------
