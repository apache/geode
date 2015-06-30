package com.gemstone.gemfire.internal.cache.control;

import java.util.Set;

import com.gemstone.gemfire.internal.cache.control.ResourceAdvisor.ResourceManagerProfile;

/**
 * Implemented by classes that the ResourceManager creates in order to monitor a
 * specific type of resource (heap memory, off-heap memory, disk, etc.).
 * 
 * @author David Hoots
 * @since 9.0
 */
interface ResourceMonitor {

  /**
   * Ask the monitor to notify the given listeners of the given event.
   * 
   * @param listeners
   *          Set of listeners of notify.
   * @param event
   *          Event to send to the listeners.
   */
  public void notifyListeners(final Set<ResourceListener> listeners, final ResourceEvent event);

  /**
   * Ask the monitor to stop monitoring.
   */
  public void stopMonitoring();
  
  /**
   * Populate the fields in the profile that are appropriate for this monitor.
   * 
   * @param profile
   *          The profile to populate.
   */
  public void fillInProfile(final ResourceManagerProfile profile);
}
