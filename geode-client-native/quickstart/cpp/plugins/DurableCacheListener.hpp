/*
 * DurableClient QuickStart Example.
 *
 * This is to use newly added listener callback "afterRegionLive"
 * It merely prints the events captured from the GemFire Native Client.
 *
 */

// Include the GemFire library.
#include <gfcpp/GemfireCppCache.hpp>
#include <gfcpp/CacheListener.hpp>

// Use the "gemfire" namespace.
using namespace gemfire;

// The SimpleCacheListener class.
class DurableCacheListener : public CacheListener
{
public:
  // The regionLiveCallback. This get called once all the queued events are recieved by client on reconnect.
  virtual void afterRegionLive( const RegionEvent& event );
  virtual void afterCreate( const EntryEvent& event );
  virtual void afterUpdate( const EntryEvent& event );
};
