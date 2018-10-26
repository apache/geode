/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.distributed.internal.locks;

import java.util.HashMap;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.log4j.LogMarker;

/**
 * Keeps track of all the information kept by the elder.
 *
 * @since GemFire 4.0
 */
public class ElderState {
  private static final Logger logger = LogService.getLogger();

  /**
   * Maps service name keys to GrantorInfo values.
   */
  private final HashMap<String, GrantorInfo> nameToInfo;
  private final DistributionManager dm;

  /**
   * Constructs the EdlerState for the given dm. Note that this constructor does not complete until
   * elder recovery is complete.
   */
  public ElderState(DistributionManager dm) {
    Assert.assertTrue(dm != null);
    this.dm = dm;
    this.nameToInfo = new HashMap<>();
    try {
      this.dm.getStats().incElders(1);
      ElderInitProcessor.init(this.dm, this.nameToInfo);
    } catch (NullPointerException | InternalGemFireError e) {
      try {
        checkForProblem(dm);
      } finally {
        throw e;
      }
    } finally {
      if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
        StringBuilder sb = new StringBuilder("ElderState initialized with:");
        for (String key : this.nameToInfo.keySet()) {
          sb.append("\n\t").append(key).append(": ").append(this.nameToInfo.get(key));
        }
        logger.trace(LogMarker.DLS_VERBOSE, sb.toString());
      }
    }
  }

  private void checkForProblem(DistributionManager checkDM) {
    if (checkDM.getSystem() == null) {
      logger.warn(LogMarker.DLS_MARKER, "ElderState problem: system={}", checkDM.getSystem());
      return;
    }
    if (checkDM.getSystem().getDistributionManager() == null) {
      logger.warn(LogMarker.DLS_MARKER,
          "ElderState problem: system DistributionManager={}",
          checkDM.getSystem().getDistributionManager());
    }
    if (checkDM != checkDM.getSystem().getDistributionManager()) {
      logger.warn(LogMarker.DLS_MARKER,
          "ElderState problem: dm={}, but system DistributionManager={}",
          new Object[] {checkDM, checkDM.getSystem().getDistributionManager()});
    }
  }

  /**
   * Atomically determine who is the current grantor of the given service. If no current grantor
   * exists then the caller is made the grantor.
   *
   * @param serviceName the name of the lock service we want the grantor of
   * @param requestor the id of the member who is making this request
   * @return the current grantor of <code>serviceName</code> and recoveryNeeded will be true if
   *         requestor has become the grantor and needs to recover lock info.
   */
  public GrantorInfo getGrantor(String serviceName, InternalDistributedMember requestor,
      int dlsSerialNumberRequestor) {
    synchronized (this) {
      GrantorInfo gi = (GrantorInfo) this.nameToInfo.get(serviceName);
      if (gi != null) {
        waitWhileInitiatingTransfer(gi);
        InternalDistributedMember currentGrantor = gi.getId();
        // Note that elder recovery may put GrantorInfo instances in
        // the map whose id is null and whose needRecovery is true
        if (currentGrantor != null
            && this.dm.getDistributionManagerIds().contains(currentGrantor)) {
          return gi;
        } else {
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "Elder setting grantor for {} to {} because {} ",
                serviceName, requestor, (currentGrantor != null ? "current grantor crashed"
                    : "of unclean grantor shutdown"));
          }
          // current grantor crashed; make new member grantor and force recovery
          long myVersion = gi.getVersionId() + 1;
          this.nameToInfo.put(serviceName,
              new GrantorInfo(requestor, myVersion, dlsSerialNumberRequestor, false));
          return new GrantorInfo(requestor, myVersion, dlsSerialNumberRequestor, true);
        }
      } else {
        if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
          logger.trace(LogMarker.DLS_VERBOSE,
              "Elder setting grantor for {} to {} because of clean grantor shutdown", serviceName,
              requestor);
        }
        gi = new GrantorInfo(requestor, 1, dlsSerialNumberRequestor, false);
        this.nameToInfo.put(serviceName, gi);
        return gi;
      }
    }
  }

  /**
   * Atomically determine who is the current grantor of the given service.
   *
   * @param serviceName the name of the lock service we want the grantor of
   * @return the current grantor of <code>serviceName</code> and recoveryNeeded will be true if
   *         requestor has become the grantor and needs to recover lock info.
   */
  public GrantorInfo peekGrantor(String serviceName) {
    synchronized (this) {
      GrantorInfo gi = (GrantorInfo) this.nameToInfo.get(serviceName);
      if (gi != null) {
        waitWhileInitiatingTransfer(gi);
        InternalDistributedMember currentGrantor = gi.getId();
        // Note that elder recovery may put GrantorInfo instances in
        // the map whose id is null and whose needRecovery is true
        if (currentGrantor != null
            && this.dm.getDistributionManagerIds().contains(currentGrantor)) {
          return gi;
        } else {
          return new GrantorInfo(null, 0, 0, true);
        }
      } else {
        return new GrantorInfo(null, 0, 0, false);
      }
    }
  }

  /**
   * Atomically sets the current grantor of the given service to <code>newGrantor</code>.
   *
   * @param serviceName the name of the lock service we want the grantor of
   * @param newGrantor the id of the member who is making this request
   * @param oldTurk if non-null then only do the become if the current grantor is the oldTurk
   * @return the previous grantor, which may be null, of <code>serviceName</code> and recoveryNeeded
   *         will be true if new grantor needs to recover lock info
   */
  public GrantorInfo becomeGrantor(String serviceName, InternalDistributedMember newGrantor,
      int newGrantorSerialNumber, InternalDistributedMember oldTurk) {
    GrantorInfo newInfo = null;
    InternalDistributedMember previousGrantor = null;
    long newGrantorVersion = -1;
    try {
      synchronized (this) {
        GrantorInfo gi = (GrantorInfo) this.nameToInfo.get(serviceName);
        while (gi != null && gi.isInitiatingTransfer()) {
          waitWhileInitiatingTransfer(gi);
          gi = (GrantorInfo) this.nameToInfo.get(serviceName);
        }
        if (gi != null) {
          previousGrantor = gi.getId();
          // Note that elder recovery may put GrantorInfo instances in
          // the map whose id is null and whose needRecovery is true

          // if previousGrantor still exists...
          if (previousGrantor != null
              && this.dm.getDistributionManagerIds().contains(previousGrantor)) {

            // if newGrantor is not previousGrantor...
            if (!newGrantor.equals(previousGrantor)) {

              // problem: specified oldTurk is not previousGrantor...
              if (oldTurk != null && !oldTurk.equals(previousGrantor)) {
                if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
                  logger.trace(LogMarker.DLS_VERBOSE,
                      "Elder did not become grantor for {} to {} because oldT was {} and the current grantor is {}",
                      serviceName, newGrantor, oldTurk, previousGrantor);
                }
              }

              // no oldTurk or oldTurk matches previousGrantor... transfer might occur
              else {
                // install new grantor
                if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
                  logger.trace(LogMarker.DLS_VERBOSE, "Elder forced to set grantor for {} to {}",
                      serviceName, newGrantor);
                }
                long myVersion = gi.getVersionId() + 1;
                newGrantorVersion = myVersion;
                newInfo = new GrantorInfo(newGrantor, myVersion, newGrantorSerialNumber, false);
                this.nameToInfo.put(serviceName, newInfo);

                if (gi.getId() != null && (oldTurk == null || gi.getId().equals(oldTurk))
                    && !gi.getId().equals(newGrantor)) {
                  beginInitiatingTransfer(newInfo);
                }

              }
            }
            // return previous grantor
            return new GrantorInfo(gi.getId(), gi.getVersionId(), gi.getSerialNumber(), true);
          }

          // no previousGrantor in existence...
          else {
            long myVersion = gi.getVersionId() + 1;

            // problem: oldTurk was specified but there is no previousGrantor...
            if (oldTurk != null) {
              if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
                logger.trace(LogMarker.DLS_VERBOSE,
                    "Elder did not become grantor for {} to {} because oldT was {} and the current grantor {} had crashed",
                    serviceName, newGrantor, oldTurk, previousGrantor);
              }
            }

            // no oldTurk was specified...
            else {
              if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
                logger.trace(LogMarker.DLS_VERBOSE,
                    "Elder forced to set grantor for {} to {} and noticed previous grantor had crashed",
                    serviceName, newGrantor);
              }
              // current grantor crashed; make new member grantor and force recovery
              this.nameToInfo.put(serviceName,
                  new GrantorInfo(newGrantor, myVersion, newGrantorSerialNumber, false));
            }

            return new GrantorInfo(null, myVersion - 1, gi.getSerialNumber(), true);
          }
        }

        // GrantorInfo was null...
        else {
          // problem: no oldTurk was specified
          if (oldTurk != null) {
            if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
              logger.trace(LogMarker.DLS_VERBOSE,
                  "Elder did not become grantor for {} to {} because oldT was {} and elder had no current grantor",
                  serviceName, newGrantor, oldTurk);
            }
          }

          // no oldTurk was specified
          else {
            if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
              logger.trace(LogMarker.DLS_VERBOSE,
                  "Elder forced to set grantor for {} to {} because of clean grantor shutdown",
                  serviceName, newGrantor);
            }
            // no current grantor; last one shutdown cleanly
            gi = new GrantorInfo(newGrantor, 1, newGrantorSerialNumber, false);
            this.nameToInfo.put(serviceName, gi);
          }
          return new GrantorInfo(null, 0, 0, false);
        }
      }
    } finally {
      if (isInitiatingTransfer(newInfo)) {
        Assert.assertTrue(newGrantorVersion > -1);
        DeposeGrantorProcessor.send(serviceName, previousGrantor, newGrantor, newGrantorVersion,
            newGrantorSerialNumber, dm);
        finishInitiatingTransfer(newInfo);
      }
    }
  }

  /**
   * Atomically clears the current grantor of the given service if the current grantor is
   * <code>oldGrantor</code>. The next grantor for this service will not need to recover unless
   * <code>locksHeld</code> is true.
   *
   * @param locksHeld true if old grantor had held locks
   */
  public void clearGrantor(long grantorVersion, String serviceName, int dlsSerialNumber,
      InternalDistributedMember oldGrantor, boolean locksHeld) {
    synchronized (this) {
      if (grantorVersion == -1) {
        // not possible to clear grantor of non-initialized grantorVersion
        return;
      }

      GrantorInfo currentGI = (GrantorInfo) this.nameToInfo.get(serviceName);
      if (currentGI == null) {
        // added null check because becomeGrantor may not have talked to elder before destroy dls
        return;
      }
      if (currentGI.getVersionId() != grantorVersion
          || currentGI.getSerialNumber() != dlsSerialNumber) {
        // not possible to clear mismatched grantorVersion
        return;
      }

      GrantorInfo gi;
      if (locksHeld) {
        gi = (GrantorInfo) this.nameToInfo.put(serviceName,
            new GrantorInfo(null, currentGI.getVersionId(), 0, true));
      } else {
        gi = (GrantorInfo) this.nameToInfo.remove(serviceName);
      }
      if (gi != null) {
        InternalDistributedMember currentGrantor = gi.getId();
        if (!oldGrantor.equals(currentGrantor)) { // fix for 32603
          this.nameToInfo.put(serviceName, gi);
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE,
                "Elder not making {} grantor shutdown for {} by {} because the current grantor is {}",
                (locksHeld ? "unclean" : "clean"), serviceName, oldGrantor, currentGrantor);
          }
        } else {
          if (logger.isTraceEnabled(LogMarker.DLS_VERBOSE)) {
            logger.trace(LogMarker.DLS_VERBOSE, "Elder making {} grantor shutdown for {} by {}",
                (locksHeld ? "unclean" : "clean"), serviceName, oldGrantor);
          }
        }
      }
    }
  }

  private boolean isInitiatingTransfer(GrantorInfo gi) {
    if (gi == null)
      return false;
    synchronized (this) {
      return gi.isInitiatingTransfer();
    }
  }

  private void beginInitiatingTransfer(GrantorInfo gi) {
    synchronized (this) {
      gi.setInitiatingTransfer(true);
    }
  }

  private void finishInitiatingTransfer(GrantorInfo gi) {
    synchronized (this) {
      gi.setInitiatingTransfer(false);
      notifyAll();
    }
  }

  private void waitWhileInitiatingTransfer(GrantorInfo gi) {
    synchronized (this) {
      boolean interrupted = false;
      try {
        while (gi.isInitiatingTransfer()) {
          try {
            wait();
          } catch (InterruptedException e) {
            interrupted = true;
            dm.getCancelCriterion().checkCancelInProgress(e);
          }
        }
      } finally {
        if (interrupted)
          Thread.currentThread().interrupt();
      }
    }
  }

  /** Testing method to force grantor recovery state for named service */
  public void forceGrantorRecovery(String serviceName) {
    synchronized (this) {
      GrantorInfo gi = (GrantorInfo) this.nameToInfo.get(serviceName);
      if (gi.isInitiatingTransfer()) {
        throw new IllegalStateException(
            "Cannot force grantor recovery for grantor that is transferring");
      }
      this.nameToInfo.put(serviceName,
          new GrantorInfo(gi.getId(), gi.getVersionId(), gi.getSerialNumber(), true));
    }
  }

}
