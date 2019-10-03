package org.apache.geode.distributed.internal.tcpserver;

import static com.tngtech.archunit.base.DescribedPredicate.not;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.type;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;

import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.junit.ArchUnitRunner;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.runner.RunWith;

import org.apache.geode.CancelException;
import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.SystemFailure;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.distributed.internal.DistributionStats;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.PoolStatHelper;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolProcessor;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolService;
import org.apache.geode.internal.cache.client.protocol.ClientProtocolServiceLoader;
import org.apache.geode.internal.cache.tier.CommunicationMode;
import org.apache.geode.internal.logging.CoreLoggingExecutors;
import org.apache.geode.internal.net.SSLConfigurationFactory;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;


@RunWith(ArchUnitRunner.class)
@AnalyzeClasses(packages = "org.apache.geode.distributed.internal.tcpserver")
public class TcpServerDependenciesTest {

  @ArchTest
  public static final ArchRule membershipDoesntDependOnCoreProvisional = classes()
      .that()
      .resideInAPackage("org.apache.geode.distributed.internal.tcpserver..")

      .should()
      .onlyDependOnClassesThat(
          resideInAPackage("org.apache.geode.distributed.internal.tcpserver..")
              .or(resideInAPackage("org.apache.geode.internal.serialization.."))
              .or(resideInAPackage("org.apache.geode.logging.."))

              .or(not(resideInAPackage("org.apache.geode..")))
              .or(resideInAPackage("org.apache.geode.test.."))


              // TODO - serialization related classes
              .or(type(DataSerializer.class))
              .or(type(DataSerializable.class))

              // TODO - TCP socket related classes
              .or(type(CommunicationMode.class))
              .or(type(SocketCreator.class))
              .or(type(SSLConfigurationFactory.class))
              .or(type(SecurableCommunicationChannel.class))
              .or(type(SocketCreatorFactory.class))
              .or(type(SSLConfigurationFactory.class))

              // TODO - client protocol service
              .or(type(ClientProtocolServiceLoader.class))
              .or(type(ClientProtocolService.class))
              .or(type(ClientProtocolProcessor.class))



              // TODO - stats
              .or(type(DistributionStats.class))
              .or(type(PoolStatHelper.class))
              .or(type(CoreLoggingExecutors.class))

              // TODO - cancel excpetion
              .or(type(CancelException.class))


              // TODO - config
              .or(type(DistributionConfigImpl.class))
              .or(type(DistributionConfig.class))


              // TODO - god classes
              .or(type(DistributedSystem.class))
              .or(type(InternalConfigurationPersistenceService.class))
              .or(type(GemFireCache.class))
              .or(type(InternalLocator.class))
              .or(type(InternalCache.class))
              .or(type(InternalDistributedSystem.class))
              .or(type(SystemFailure.class))

              // TODO - version class? Version.java is in serialization, what is
              // GemFireVersion?
              .or(type(GemFireVersion.class))

  );


}
