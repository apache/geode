/*
 * SimpleCacheListener QuickStart Example.
 *
 * This is a simple implementation of a Cache Listener
 * It merely prints the events captured from the GemFire Native Client.
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/CacheListener.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

// The SimpleCacheListener class.
class SimpleCacheListener : public CacheListener
{
public:
  // The Cache Listener callbacks.
  virtual void afterCreate( const EntryEvent& event );
  virtual void afterUpdate( const EntryEvent& event );
  virtual void afterInvalidate( const EntryEvent& event );
  virtual void afterDestroy( const EntryEvent& event );
  virtual void afterRegionInvalidate( const RegionEvent& event );
  virtual void afterRegionDestroy( const RegionEvent& event );
  virtual void close(const RegionPtr& region);
};
