package org.apache.geode.distributed.internal.membership;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.assignableTo;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.type;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchIgnore;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;

import org.apache.geode.CancelCriterion;
import org.apache.geode.DataSerializer;
import org.apache.geode.GemFireException;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.DurableClientAttributes;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.Role;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.FlowControlParams;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.LocatorStats;
import org.apache.geode.distributed.internal.OverflowQueueWithDMStats;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.SizeableRunnable;
import org.apache.geode.distributed.internal.direct.DirectChannel;
import org.apache.geode.distributed.internal.direct.DirectChannelListener;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.distributed.internal.tcpserver.TcpHandler;
import org.apache.geode.distributed.internal.tcpserver.TcpServer;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.ConnectionWatcher;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.OSProcess;
import org.apache.geode.internal.SystemTimer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;
import org.apache.geode.internal.VersionedObjectInput;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.internal.admin.remote.RemoteTransportConfig;
import org.apache.geode.internal.alerting.AlertingAction;
import org.apache.geode.internal.cache.CacheConfig;
import org.apache.geode.internal.cache.CacheServerImpl;
import org.apache.geode.internal.cache.DirectReplyMessage;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.xmlcache.CacheServerCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.internal.concurrent.ConcurrentHashSet;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.logging.LoggingExecutors;
import org.apache.geode.internal.logging.LoggingThread;
import org.apache.geode.internal.logging.log4j.AlertAppender;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.internal.shared.StringPrintWriter;
import org.apache.geode.internal.tcp.ConnectExceptions;
import org.apache.geode.internal.util.Breadcrumbs;
import org.apache.geode.internal.util.JavaWorkarounds;

@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "org.apache.geode.distributed.internal.membership..")
public class MembershipDependenciesJUnitTest {

  /*
   * This test verifies that the membership component (which is currently made up of classes
   * inside the geode-core module, but which may someday reside in a separate module)
   * depends only on packages within itself (within the membership component) or packages
   * outside apache.geode.
   *
   * For purposes of this test, classes in the membership...adapter package are not considered.
   * They will eventually become part of geode-core.
   *
   * While this rule is ignored, comment-out the ignore annotation to run it periodically to get
   * the current count of deviations.
   */
  // TODO: remove ignore once membershipDoesntDependOnCoreProvisional matches this rule exactly
  @ArchIgnore
  @ArchTest
  public static final ArchRule membershipDoesntDependOnCore = classes()
      .that()
      .resideInAPackage("org.apache.geode.distributed.internal.membership..")
      .and()
      .resideOutsideOfPackage("org.apache.geode.distributed.internal.membership.adapter..")
      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.distributed.internal.membership..")
              .or(not(resideInAPackage("org.apache.geode.."))));

  /*
   * This test is a work-in-progress. It starts from the membershipDoesntDependOnCore rule
   * and adds deviations. Each deviation has a comment like TODO:...
   * Those deviations comprise a to do list for the membership team as it modularizes
   * the membership component--severing its dependency on the geode-core component.
   */
  @ArchTest
  public static final ArchRule membershipDoesntDependOnCoreProvisional = classes()
      .that()
      .resideInAPackage("org.apache.geode.distributed.internal.membership..")
      .and()
      .resideOutsideOfPackage("org.apache.geode.distributed.internal.membership.adapter..")
      .and()

      // TODO: InternalDistributedMember needs to move out of the package
      .areNotAssignableFrom(InternalDistributedMember.class)


      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.distributed.internal.membership..")

              .or(not(resideInAPackage("org.apache.geode..")))

              // TODO: Create a new stats interface for membership
              .or(assignableTo(DMStats.class))
              .or(type(LocatorStats.class))
              .or(type(OverflowQueueWithDMStats.class))

              // TODO: Figure out what to do with exceptions
              .or(assignableTo(GemFireException.class))
              .or(type(InternalGemFireError.class))
              .or(type(ConnectExceptions.class))

              // TODO: Serialization needs to become its own module
              .or(type(DataSerializer.class))
              .or(type(DataSerializableFixedID.class))
              .or(type(InternalDataSerializer.class))
              .or(type(Version.class))
              .or(type(Version[].class)) // ArchUnit needs the array type to be explicitly mentioned
              .or(type(VersionedObjectInput.class))
              .or(type(HeapDataOutputStream.class))
              .or(type(VersionedDataInputStream.class))

              // TODO: Figure out what to do with messaging
              // Membership messages don't need to extend DistributionMessage
              .or(assignableTo(DistributionMessage.class))
              .or(assignableTo(DirectReplyMessage.class))

              // TODO: Membership needs its own config object
              .or(type(DistributionConfig.class))
              .or(type(DistributionConfigImpl.class))
              .or(type(RemoteTransportConfig.class))
              .or(type(FlowControlParams.class))


              // TODO: Break dependency on geode logger
              .or(type(LogService.class))
              .or(assignableTo(LoggingThread.class))
              .or(type(LoggingExecutors.class))
              .or(type(LogMarker.class))
              .or(type(AlertAppender.class))

              // TODO
              .or(type(SystemFailure.class))

              // TODO: Break dependency on Role
              .or(assignableTo(Role.class))

              // TODO: Break dependency on SystemTimerTask
              .or(assignableTo(SystemTimer.SystemTimerTask.class))
              .or(type(SystemTimer.class))

              // TODO: Break dependency on SizeableRunnable
              .or(assignableTo(SizeableRunnable.class))

              // TODO: Break dependency on DirectChannelListener (and its getDM method!)
              .or(assignableTo(DirectChannelListener.class))

              // TODO: This and TCP conduit be moved inside membership.
              // Do we even need this class since it is just forwarding to TCPConduit?
              .or(type(DirectChannel.class))

              // TODO: Break dependency on geode DurableClientAttributes
              .or(type(DurableClientAttributes.class))

              // TODO
              .or(assignableTo(CancelCriterion.class))

              // TODO
              .or(assignableTo(ConnectionWatcher.class))

              // TODO:
              .or(type(SocketCreator.class))
              .or(type(SocketCreatorFactory.class))

              // TODO:
              .or(type(Assert.class))

              // TODO: break dependencies on locator-related classes
              .or(type(Locator.class))
              .or(type(TcpHandler.class))
              .or(type(TcpClient.class))
              .or(type(TcpServer.class))
              .or(type(ServerLocation.class))
              .or(type(InternalLocator.class))
              .or(type(DistributionLocatorId.class))
              .or(type(InternalConfigurationPersistenceService.class))

              // TODO: Mt. Olympus (G*D objects live here)
              .or(type(DistributionManager.class))
              .or(type(InternalDistributedSystem.class))
              .or(type(DistributedSystem.class))
              .or(type(GemFireCache.class))
              .or(type(GemFireCacheImpl.class))
              .or(type(InternalCache.class))
              .or(type(ClusterDistributionManager.class))

              // TODO: break dependency on internal.security
              .or(type(SecurableCommunicationChannel.class))

              // TODO:
              .or(type(DistributedMember.class))

              // TODO: This stuff is just used in GMSMembershipManager.saveCacheXmlForReconnect
              .or(type(CacheServerImpl.class))
              .or(type(StringPrintWriter.class))
              .or(type(CacheXmlGenerator.class))
              .or(type(CacheServerCreation.class))
              .or(type(CacheConfig.class))

              // TODO:
              .or(type(JavaWorkarounds.class))

              // TODO:
              .or(type(ConcurrentHashSet.class))

              // TODO:
              .or(type(OSProcess.class))

              // TODO:
              .or(type(ClassPathLoader.class))

              // TODO:
              .or(type(AlertingAction.class))

              // TODO:
              .or(type(Breadcrumbs.class))

  );

}
