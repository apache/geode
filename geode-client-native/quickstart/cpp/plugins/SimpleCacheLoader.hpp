/*
 * SimpleCacheLoader QuickStart Example.
 *
 * This is a simple implementation of a Cache Loader
 * It merely prints the events captured from the GemFire Native Client.
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/CacheLoader.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

// The SimpleCacheLoader class.
class SimpleCacheLoader : public CacheLoader
{
public:
  virtual CacheablePtr load(
    const RegionPtr& region,
    const CacheableKeyPtr& key,
    const UserDataPtr& aCallbackArgument);
  
  virtual void close( const RegionPtr& region );
};
