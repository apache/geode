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
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.management.internal.cli.result.ResultBuilder.buildResult;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.functions.DestroyJndiBindingFunction;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;

public class DestroyJndiBindingCommand implements GfshCommand {
  static final String CREATE_JNDIBINDING = "destroy jndi-binding";
  static final String CREATE_JNDIBINDING__HELP =
      "Destroy a jndi binding that holds the configuration for the XA datasource.";
  static final String JNDI_NAME = "name";
  static final String JNDI_NAME__HELP = "Name of the binding to be destroyed.";

  @CliCommand(value = CREATE_JNDIBINDING, help = CREATE_JNDIBINDING__HELP)
  @CliMetaData(relatedTopic = CliStrings.TOPIC_GEODE_REGION)
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public Result destroyJDNIBinding(
      @CliOption(key = JNDI_NAME, mandatory = true, help = JNDI_NAME__HELP) String jndiName)
      throws IOException, SAXException, ParserConfigurationException, TransformerException {

    Result result;
    boolean persisted = false;
    ClusterConfigurationService service = getSharedConfiguration();
    if (service != null) {
      Element existingBinding =
          service.getXmlElement("cluster", "jndi-binding", "jndi-name", jndiName);
      if (existingBinding == null) {
        return ResultBuilder.createUserErrorResult(
            CliStrings.format("Jndi binding with jndi-name \"{0}\" does not exist.", jndiName));
      }
      removeJndiBindingFromXml(jndiName);
      persisted = true;
    }

    Set<DistributedMember> targetMembers = findMembers(null, null);
    if (targetMembers.size() > 0) {
      List<CliFunctionResult> jndiCreationResult =
          executeAndGetFunctionResult(new DestroyJndiBindingFunction(), jndiName, targetMembers);
      return buildResult(jndiCreationResult);
    } else {
      if (persisted) {
        result = ResultBuilder.createInfoResult(CliStrings.format(
            "No members found. Jndi-binding \"{0}\" is removed from cluster configuration.",
            jndiName));
      } else {
        result = ResultBuilder.createInfoResult("No members found.");
      }
    }
    result.setCommandPersisted(persisted);
    return result;
  }

  void removeJndiBindingFromXml(String jndiName)
      throws TransformerException, IOException, SAXException, ParserConfigurationException {
    // cluster group config should always be present
    Configuration config = getSharedConfiguration().getConfiguration("cluster");

    Document document = XmlUtils.createDocumentFromXml(config.getCacheXmlContent());
    NodeList jndiBindings = document.getElementsByTagName("jndi-binding");

    boolean updatedXml = false;
    if (jndiBindings != null && jndiBindings.getLength() > 0) {
      for (int i = 0; i < jndiBindings.getLength(); i++) {
        Element eachBinding = (Element) jndiBindings.item(i);
        if (eachBinding.getAttribute("jndi-name").equals(jndiName)) {
          eachBinding.getParentNode().removeChild(eachBinding);
          updatedXml = true;
        }
      }
    }

    if (updatedXml) {
      String newXml = XmlUtils.prettyXml(document.getFirstChild());
      config.setCacheXmlContent(newXml);

      getSharedConfiguration().getConfigurationRegion().put("cluster", config);
    }
  }
}
