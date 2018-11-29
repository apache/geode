package org.apache.geode.connectors.jdbc.internal.cli;

import java.util.List;
import java.util.Set;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.CacheElement;
import org.apache.geode.cache.configuration.JndiBindingsType;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.SingleGfshCommand;
import org.apache.geode.management.internal.cli.exceptions.EntityNotFoundException;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DestroyJndiBindingFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyDataSourceCommand extends SingleGfshCommand {
  static final String DESTROY_DATA_SOURCE = "destroy data-source";
  static final String DESTROY_DATA_SOURCE_HELP =
      "Destroy a data source that holds a jdbc configuration.";
  static final String DATA_SOURCE_NAME = "name";
  static final String DATA_SOURCE_NAME_HELP = "Name of the data source to be destroyed.";
  static final String IFEXISTS_HELP =
      "Skip the destroy operation when the specified data source does "
          + "not exist. Without this option, an error results from the specification "
          + "of a data source that does not exist.";

  @CliCommand(value = DESTROY_DATA_SOURCE, help = DESTROY_DATA_SOURCE_HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public ResultModel destroyDataSource(
      @CliOption(key = DATA_SOURCE_NAME, mandatory = true,
          help = DATA_SOURCE_NAME_HELP) String dataSourceName,
      @CliOption(key = CliStrings.IFEXISTS, help = IFEXISTS_HELP, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false") boolean ifExists) {

    InternalConfigurationPersistenceService service =
        (InternalConfigurationPersistenceService) getConfigurationPersistenceService();
    if (service != null) {
      List<JndiBindingsType.JndiBinding> bindings =
          service.getCacheConfig("cluster").getJndiBindings();
      JndiBindingsType.JndiBinding binding = CacheElement.findElement(bindings, dataSourceName);
      // fail fast when CC is running and if required binding not found assuming that
      // when CC is running then every configuration goes through CC
      if (binding == null) {
        throw new EntityNotFoundException(
            CliStrings.format("Data source named \"{0}\" does not exist.", dataSourceName),
            ifExists);
      }
    }

    Set<DistributedMember> targetMembers = findMembers(null, null);
    if (targetMembers.size() > 0) {
      List<CliFunctionResult> dataSourceDestroyResult =
          executeAndGetFunctionResult(new DestroyJndiBindingFunction(),
              new Object[] {dataSourceName, true}, targetMembers);

      if (!ifExists) {
        int resultsNotFound = 0;
        for (CliFunctionResult result : dataSourceDestroyResult) {
          if (result.getStatusMessage().contains("not found")) {
            resultsNotFound++;
          }
        }
        if (resultsNotFound == dataSourceDestroyResult.size()) {
          throw new EntityNotFoundException(
              CliStrings.format("Data source named \"{0}\" does not exist.", dataSourceName),
              ifExists);
        }
      }

      ResultModel result = ResultModel.createMemberStatusResult(dataSourceDestroyResult);
      result.setConfigObject(dataSourceName);

      return result;
    } else {
      if (service != null) {
        return ResultModel.createInfo("No members found.");
      } else {
        return ResultModel.createError("No members found and cluster configuration disabled.");
      }
    }
  }

  @Override
  public boolean updateConfigForGroup(String group, CacheConfig config, Object element) {
    CacheElement.removeElement(config.getJndiBindings(), (String) element);
    return true;
  }
}
