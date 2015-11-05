/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.web.controllers;

import java.io.IOException;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.lang.ObjectUtils;
import com.gemstone.gemfire.internal.lang.StringUtils;
import com.gemstone.gemfire.internal.util.IOUtils;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.web.domain.Link;
import com.gemstone.gemfire.management.internal.web.domain.LinkIndex;
import com.gemstone.gemfire.management.internal.web.domain.QueryParameterSource;
import com.gemstone.gemfire.management.internal.web.http.HttpMethod;

/**
 * The ShellCommandsController class implements GemFire REST API calls for Gfsh Shell Commands.
 * 
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.ShellCommands
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.RequestBody
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 8.0
 */
@Controller("shellController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class ShellCommandsController extends AbstractCommandsController {

  protected static final String MBEAN_ATTRIBUTE_LINK_RELATION = "mbean-attribute";
  protected static final String MBEAN_OPERATION_LINK_RELATION = "mbean-operation";
  protected static final String MBEAN_QUERY_LINK_RELATION = "mbean-query";
  protected static final String PING_LINK_RELATION = "ping";

  @RequestMapping(method = RequestMethod.POST, value = "/management/commands", params = "cmd")
  @ResponseBody
  public String command(@RequestParam("cmd") final String command) {
    return processCommand(decode(command));
  }

  // TODO research the use of Jolokia instead
  @RequestMapping(method = RequestMethod.GET, value = "/mbean/attribute")
  public ResponseEntity<?> getAttribute(@RequestParam("resourceName") final String resourceName,
                                        @RequestParam("attributeName") final String attributeName)
  {
    try {
      final Object attributeValue = getMBeanServer().getAttribute(ObjectName.getInstance(decode(resourceName)),
        decode(attributeName));

      return new ResponseEntity<byte[]>(IOUtils.serializeObject(attributeValue), HttpStatus.OK);
    }
    catch (AttributeNotFoundException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.BAD_REQUEST);
    }
    catch (InstanceNotFoundException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.NOT_FOUND);
    }
    catch (MalformedObjectNameException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.BAD_REQUEST);
    }
    catch (Exception e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  // TODO research the use of Jolokia instead
  @RequestMapping(method = RequestMethod.POST, value = "/mbean/operation")
  public ResponseEntity<?> invoke(@RequestParam("resourceName") final String resourceName,
                                  @RequestParam("operationName") final String operationName,
                                  @RequestParam(value = "signature", required = false) String[] signature,
                                  @RequestParam(value = "parameters", required = false) Object[] parameters)
  {
    signature = (signature != null ? signature : StringUtils.EMPTY_STRING_ARRAY);
    parameters = (parameters != null ? parameters : ObjectUtils.EMPTY_OBJECT_ARRAY);

    try {
      final Object result = getMBeanServer().invoke(ObjectName.getInstance(decode(resourceName)), decode(operationName),
        parameters, signature);

      return new ResponseEntity<byte[]>(IOUtils.serializeObject(result), HttpStatus.OK);
    }
    catch (InstanceNotFoundException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.NOT_FOUND);
    }
    catch (MalformedObjectNameException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.BAD_REQUEST);
    }
    catch (Exception e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @RequestMapping(method = RequestMethod.POST, value = "/mbean/query")
  public ResponseEntity<?> queryNames(@RequestBody final QueryParameterSource query) {
    try {
      final Set<ObjectName> objectNames = getMBeanServer().queryNames(query.getObjectName(), query.getQueryExpression());

      return new ResponseEntity<byte[]>(IOUtils.serializeObject(objectNames), HttpStatus.OK);
    }
    catch (IOException e) {
      return new ResponseEntity<String>(printStackTrace(e), HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Gets a link index for the web service endpoints and REST API calls in GemFire for management and monitoring
   * using GemFire shell (Gfsh).
   * 
   * @return a LinkIndex containing Links for all web service endpoints, REST API calls in GemFire.
   * @see com.gemstone.gemfire.management.internal.cli.i18n.CliStrings
   * @see AbstractCommandsController#toUri(String, String)
   * @see com.gemstone.gemfire.management.internal.web.domain.Link
   * @see com.gemstone.gemfire.management.internal.web.domain.LinkIndex
   * @see com.gemstone.gemfire.management.internal.web.http.HttpMethod
   */
  // TODO figure out a better way to maintain this link index, such as using an automated way to introspect
  // the Spring Web MVC Controller RequestMapping Annotations.
  @RequestMapping(method = RequestMethod.GET, value = "/index", produces = MediaType.APPLICATION_XML_VALUE)
  @ResponseBody
  public LinkIndex index(@RequestParam(value = "scheme", required = false, defaultValue = "http") final String scheme) {
    //logger.warning(String.format("Returning Link Index for Context Path (%1$s).",
    //  ServletUriComponentsBuilder.fromCurrentContextPath().build().toString()));
    return new LinkIndex()
      // Cluster Commands
      .add(new Link(CliStrings.STATUS_SHARED_CONFIG, toUri("/services/cluster-config",
          scheme)))
      // Member Commands
      .add(new Link(CliStrings.LIST_MEMBER, toUri("/members", scheme)))
      .add(new Link(CliStrings.DESCRIBE_MEMBER, toUri("/members/{name}", scheme)))
      // Region Commands
      .add(new Link(CliStrings.LIST_REGION, toUri("/regions", scheme)))
      .add(new Link(CliStrings.DESCRIBE_REGION, toUri("/regions/{name}", scheme)))
      .add(new Link(CliStrings.ALTER_REGION, toUri("/regions/{name}", scheme), HttpMethod.PUT))
      .add(new Link(CliStrings.CREATE_REGION, toUri("/regions", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.DESTROY_REGION, toUri("/regions/{name}", scheme), HttpMethod.DELETE))
      // Index Commands
      .add(new Link(CliStrings.LIST_INDEX, toUri("/indexes", scheme)))
      .add(new Link(CliStrings.CLEAR_DEFINED_INDEXES, toUri("/indexes?op=clear-defined",
          scheme), HttpMethod.DELETE))
      .add(new Link(CliStrings.CREATE_INDEX, toUri("/indexes", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.CREATE_DEFINED_INDEXES, toUri("/indexes?op=create-defined",
          scheme), HttpMethod.POST))
      .add(new Link(CliStrings.DEFINE_INDEX, toUri("/indexes?op=define", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.DESTROY_INDEX, toUri("/indexes", scheme), HttpMethod.DELETE))
      .add(new Link(CliStrings.DESTROY_INDEX, toUri("/indexes/{name}", scheme), HttpMethod.DELETE))
        // Data Commands
      .add(new Link(CliStrings.GET, toUri("/regions/{region}/data", scheme), HttpMethod.GET))
      .add(new Link(CliStrings.PUT, toUri("/regions/{region}/data", scheme), HttpMethod.PUT))
      .add(new Link(CliStrings.REMOVE, toUri("/regions/{region}/data", scheme), HttpMethod.DELETE))
      .add(new Link(CliStrings.EXPORT_DATA, toUri("/members/{member}/regions/{region}/data", scheme), HttpMethod.GET))
      .add(new Link(CliStrings.IMPORT_DATA, toUri("/members/{member}/regions/{region}/data", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.LOCATE_ENTRY, toUri("/regions/{region}/data/location", scheme), HttpMethod.GET))
      .add(new Link(CliStrings.QUERY, toUri("/regions/data/query", scheme), HttpMethod.GET))
      .add(new Link(CliStrings.REBALANCE, toUri("/regions/data?op=rebalance", scheme), HttpMethod.POST))
      // Function Commands
      .add(new Link(CliStrings.LIST_FUNCTION, toUri("/functions", scheme)))
      .add(new Link(CliStrings.DESTROY_FUNCTION, toUri("/functions/{id}", scheme), HttpMethod.DELETE))
      .add(new Link(CliStrings.EXECUTE_FUNCTION, toUri("/functions/{id}", scheme), HttpMethod.POST))
      // Client Commands
      .add(new Link(CliStrings.LIST_CLIENTS, toUri("/clients", scheme)))
      .add(new Link(CliStrings.DESCRIBE_CLIENT, toUri("/clients/{clientID}", scheme)))
      // Config Commands
      .add(new Link(CliStrings.ALTER_RUNTIME_CONFIG, toUri("/config", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.DESCRIBE_CONFIG, toUri("/members/{member}/config", scheme)))
      .add(new Link(CliStrings.EXPORT_CONFIG, toUri("/config", scheme)))
      .add(new Link(CliStrings.EXPORT_SHARED_CONFIG, toUri("/config/cluster", scheme)))
      .add(new Link(CliStrings.IMPORT_SHARED_CONFIG, toUri("/config/cluster", scheme), HttpMethod.POST))
      // Deploy Commands
      .add(new Link(CliStrings.LIST_DEPLOYED, toUri("/deployed", scheme)))
      .add(new Link(CliStrings.DEPLOY, toUri("/deployed", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.UNDEPLOY, toUri("/deployed", scheme), HttpMethod.DELETE))
      // Disk Store Commands
      .add(new Link(CliStrings.LIST_DISK_STORE, toUri("/diskstores", scheme)))
      .add(new Link(CliStrings.BACKUP_DISK_STORE, toUri("/diskstores?op=backup", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.COMPACT_DISK_STORE, toUri("/diskstores/{name}?op=compact",
          scheme), HttpMethod.POST))
      .add(new Link(CliStrings.CREATE_DISK_STORE, toUri("/diskstores", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.DESCRIBE_DISK_STORE, toUri("/diskstores/{name}", scheme)))
      .add(new Link(CliStrings.DESTROY_DISK_STORE, toUri("/diskstores/{name}", scheme), HttpMethod.DELETE))
      .add(new Link(CliStrings.REVOKE_MISSING_DISK_STORE, toUri("/diskstores/{id}?op=revoke", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.SHOW_MISSING_DISK_STORE, toUri("/diskstores/missing", scheme)))
      // Durable Client Commands
      .add(new Link(CliStrings.LIST_DURABLE_CQS, toUri("/durable-clients/{durable-client-id}/cqs", scheme)))
      .add(new Link(CliStrings.COUNT_DURABLE_CQ_EVENTS, toUri("/durable-clients/{durable-client-id}/cqs/events", scheme)))
      .add(new Link(CliStrings.COUNT_DURABLE_CQ_EVENTS, toUri("/durable-clients/{durable-client-id}/cqs/{durable-cq-name}/events", scheme)))
      .add(new Link(CliStrings.CLOSE_DURABLE_CLIENTS, toUri("/durable-clients/{durable-client-id}?op=close", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.CLOSE_DURABLE_CQS, toUri("/durable-clients/{durable-client-id}/cqs/{durable-cq-name}?op=close", scheme), HttpMethod.POST))
      // Launcher Lifecycle Commands
      .add(new Link(CliStrings.STATUS_LOCATOR, toUri("/members/{name}/locator", scheme)))
      .add(new Link(CliStrings.STATUS_SERVER, toUri("/members/{name}/server", scheme)))
      // Miscellaneous Commands
      .add(new Link(CliStrings.CHANGE_LOGLEVEL, toUri("/groups/{groups}/loglevel", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.CHANGE_LOGLEVEL, toUri("/members/{members}/loglevel", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.CHANGE_LOGLEVEL, toUri("/members/{members}/groups/{groups}/loglevel", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.EXPORT_LOGS, toUri("/logs", scheme)))
      .add(new Link(CliStrings.EXPORT_STACKTRACE, toUri("/stacktraces", scheme)))
      .add(new Link(CliStrings.GC, toUri("/gc", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.GC, toUri("/members/{member}/gc", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.NETSTAT, toUri("/netstat", scheme)))
      .add(new Link(CliStrings.SHOW_DEADLOCK, toUri("/deadlocks", scheme)))
      .add(new Link(CliStrings.SHOW_LOG, toUri("/members/{member}/log", scheme)))
      .add(new Link(CliStrings.SHOW_METRICS, toUri("/metrics", scheme)))
      .add(new Link(CliStrings.SHUTDOWN, toUri("/shutdown", scheme), HttpMethod.POST)) // verb!
      // Queue Commands
      .add(new Link(CliStrings.CREATE_ASYNC_EVENT_QUEUE, toUri("/async-event-queues", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.LIST_ASYNC_EVENT_QUEUES, toUri("/async-event-queues", scheme)))
      // PDX Commands
      .add(new Link(CliStrings.CONFIGURE_PDX, toUri("/pdx", scheme), HttpMethod.POST))
      //.add(new Link(CliStrings.PDX_DELETE_FIELD, toUri("/pdx/type/field"), HttpMethod.DELETE))
      .add(new Link(CliStrings.PDX_RENAME, toUri("/pdx/type", scheme), HttpMethod.POST))
      // Shell Commands
      .add(new Link(MBEAN_ATTRIBUTE_LINK_RELATION, toUri("/mbean/attribute", scheme)))
      .add(new Link(MBEAN_OPERATION_LINK_RELATION, toUri("/mbean/operation", scheme), HttpMethod.POST))
      .add(new Link(MBEAN_QUERY_LINK_RELATION, toUri("/mbean/query", scheme), HttpMethod.POST))
      .add(new Link(PING_LINK_RELATION, toUri("/ping", scheme), HttpMethod.GET))
      .add(new Link(CliStrings.VERSION, toUri("/version", scheme)))
      // WAN Gateway Commands
      .add(new Link(CliStrings.LIST_GATEWAY, toUri("/gateways", scheme)))
      .add(new Link(CliStrings.CREATE_GATEWAYRECEIVER, toUri("/gateways/receivers", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.CREATE_GATEWAYSENDER, toUri("/gateways/senders", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.LOAD_BALANCE_GATEWAYSENDER, toUri("/gateways/senders/{id}?op=load-balance", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.PAUSE_GATEWAYSENDER, toUri("/gateways/senders/{id}?op=pause", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.RESUME_GATEWAYSENDER, toUri("/gateways/senders/{id}?op=resume", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.START_GATEWAYRECEIVER, toUri("/gateways/receivers?op=start", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.START_GATEWAYSENDER, toUri("/gateways/senders?op=start", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.STATUS_GATEWAYRECEIVER, toUri("/gateways/receivers", scheme)))
      .add(new Link(CliStrings.STATUS_GATEWAYSENDER, toUri("/gateways/senders/{id}", scheme)))
      .add(new Link(CliStrings.STOP_GATEWAYRECEIVER, toUri("/gateways/receivers?op=stop", scheme), HttpMethod.POST))
      .add(new Link(CliStrings.STOP_GATEWAYSENDER, toUri("/gateways/senders/{id}?op=stop", scheme), HttpMethod.POST))
        ;
  }

  @RequestMapping(method = { RequestMethod.GET, RequestMethod.HEAD }, value = "/ping")
  public ResponseEntity<String> ping() {
    return new ResponseEntity<String>("<html><body><h1>Mischief Managed!</h1></body></html>", HttpStatus.OK);
  }

  @RequestMapping(method = RequestMethod.GET, value = "/version")
  @ResponseBody
  public String version() {
    return GemFireVersion.getProductName().concat("/").concat(GemFireVersion.getGemFireVersion());
  }

  @RequestMapping(method = RequestMethod.GET, value = "/version/full")
  @ResponseBody
  public String versionSimple() {
    return GemFireVersion.asString();
  }

}
