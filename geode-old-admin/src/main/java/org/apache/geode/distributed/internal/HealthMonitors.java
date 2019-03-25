package org.apache.geode.distributed.internal;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.admin.GemFireHealthConfig;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;

public class HealthMonitors {
  final ConcurrentMap<InternalDistributedMember, HealthMonitor> hmMap = new ConcurrentHashMap<>();
  private final InternalDistributedSystem system;

  public HealthMonitors(InternalDistributedSystem system) {
    this.system = system;
  }

  /**
   * Returns the health monitor for this distribution manager.
   *
   * @param owner the agent that owns the created monitor
   * @param cfg the configuration to use when creating the monitor
   * @since GemFire 3.5
   */
  public void createHealthMonitor(InternalDistributedMember owner, GemFireHealthConfig cfg) {
    if (system.getDistributionManager().shutdownInProgress()) {
      return;
    }
    {
      final HealthMonitor hm = getHealthMonitor(owner);
      if (hm != null) {
        hm.stop();
        this.hmMap.remove(owner);
      }
    }
    {
      HealthMonitorImpl newHm = new HealthMonitorImpl(owner, cfg, system.getDistributionManager());
      newHm.start();
      this.hmMap.put(owner, newHm);
    }
  }

  /**
   * Remove a monitor that was previously created.
   *
   * @param owner the agent that owns the monitor to remove
   */
  public void removeHealthMonitor(InternalDistributedMember owner, int theId) {
    final HealthMonitor hm = getHealthMonitor(owner);
    if (hm != null && hm.getId() == theId) {
      hm.stop();
      this.hmMap.remove(owner);
    }
  }

  public void removeAllHealthMonitors() {
    Iterator it = this.hmMap.values().iterator();
    while (it.hasNext()) {
      HealthMonitor hm = (HealthMonitor) it.next();
      hm.stop();
      it.remove();
    }
  }

  public HealthMonitor getHealthMonitor(InternalDistributedMember owner) {
    return hmMap.get(owner);
  }
}
