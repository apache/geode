/*
 * SimpleCacheWriter QuickStart Example.
 *
 * This is a simple implementation of a Cache Writer
 * It merely prints the events captured from the GemFire Native Client.
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/CacheWriter.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

// The SimpleCacheWriter class.
class SimpleCacheWriter : public CacheWriter
{
public:
  // The Cache Writer callbacks.
  virtual bool beforeUpdate( const EntryEvent& event );
  virtual bool beforeCreate( const EntryEvent& event );
  virtual void beforeInvalidate( const EntryEvent& event );
  virtual bool beforeDestroy( const EntryEvent& event );
  virtual void beforeRegionInvalidate( const RegionEvent& event );
  virtual bool beforeRegionDestroy( const RegionEvent& event );
  virtual void close(const RegionPtr& region);
};
