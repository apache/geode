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
package org.apache.geode.management.internal.cli;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.internal.cli.annotation.CliArgument;
import org.apache.geode.management.internal.cli.help.CliTopic;
import org.apache.geode.management.internal.cli.parser.*;
import org.apache.geode.management.internal.cli.parser.jopt.JoptOptionParser;
import org.apache.geode.management.internal.cli.util.ClasspathScanLoadHelper;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.annotation.CliAvailabilityIndicator;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.*;
import java.util.Map.Entry;

import static org.apache.geode.distributed.ConfigurationProperties.*;

/**
 * 
 * @since GemFire 7.0
 */
public class CommandManager {
  //1. Load Commands, availability indicators - Take from GfshParser
  //2. Load Converters - Take from GfshParser
  //3. Load Result Converters - Add  
  
  private static final Object INSTANCE_LOCK = new Object();
  private static CommandManager INSTANCE = null;
  public static final String USER_CMD_PACKAGES_PROPERTY = DistributionConfig.GEMFIRE_PREFIX + USER_COMMAND_PACKAGES;
  public static final String USER_CMD_PACKAGES_ENV_VARIABLE = "GEMFIRE_USER_COMMAND_PACKAGES";
  
  private Properties cacheProperties;
  
  private LogWrapper logWrapper;
  
  private CommandManager(final boolean loadDefaultCommands, final Properties cacheProperties) throws ClassNotFoundException, IOException {
    if (cacheProperties != null) {
      this.cacheProperties = cacheProperties;
    }
    
    logWrapper = LogWrapper.getInstance();
    if (loadDefaultCommands) {
      loadCommands();

      if (logWrapper.fineEnabled()) {
        logWrapper.fine("Commands Loaded: "+commands.keySet());
        logWrapper.fine("Command Availability Indicators Loaded: "+availabilityIndicators.keySet());
        logWrapper.fine("Converters Loaded: "+converters);
      }
    }
  }
  
  private void loadUserCommands() throws ClassNotFoundException, IOException {
    final Set<String> userCommandPackages = new HashSet<String>();
    
    // Find by packages specified by the system property
    if (System.getProperty(USER_CMD_PACKAGES_PROPERTY) != null) {
      StringTokenizer tokenizer = new StringTokenizer(System.getProperty(USER_CMD_PACKAGES_PROPERTY), ",");
      while (tokenizer.hasMoreTokens()) {
        userCommandPackages.add(tokenizer.nextToken());
      }
    }
    
    // Find by packages specified by the environment variable
    if (System.getenv().containsKey(USER_CMD_PACKAGES_ENV_VARIABLE)) {
      StringTokenizer tokenizer = new StringTokenizer(System.getenv().get(USER_CMD_PACKAGES_ENV_VARIABLE), ",");
      while (tokenizer.hasMoreTokens()) {
        userCommandPackages.add(tokenizer.nextToken());
      }
    }
    
    // Find by  packages specified in the distribution config
    if (this.cacheProperties != null) {
      String cacheUserCmdPackages = this.cacheProperties.getProperty(ConfigurationProperties.USER_COMMAND_PACKAGES);
      if (cacheUserCmdPackages != null && !cacheUserCmdPackages.isEmpty()) {
        StringTokenizer tokenizer = new StringTokenizer(cacheUserCmdPackages, ",");
        while (tokenizer.hasMoreTokens()) {
          userCommandPackages.add(tokenizer.nextToken());
        }
      }
    }
    
    // Load commands found in all of the packages
    for (String userCommandPackage : userCommandPackages) {
      try {
        Set<Class<?>> foundClasses = ClasspathScanLoadHelper.loadAndGet(userCommandPackage, CommandMarker.class, true);
        for (Class<?> klass : foundClasses) {
          try {
            add((CommandMarker) klass.newInstance());
          } catch (Exception e) {
            logWrapper.warning("Could not load User Commands from: " + klass+" due to "+e.getLocalizedMessage()); // continue
          }
        }
        raiseExceptionIfEmpty(foundClasses, "User Command");
      } catch (ClassNotFoundException e) {
        logWrapper.warning("Could not load User Commands due to "+ e.getLocalizedMessage());
        throw e;
      } catch (IOException e) {
        logWrapper.warning("Could not load User Commands due to "+ e.getLocalizedMessage());
        throw e;
      } catch (IllegalStateException e) {
        logWrapper.warning(e.getMessage(), e);
        throw e;
      }
    }
  }

  /**
   * Loads commands via {@link ServiceLoader} from {@link ClassPathLoader}. 
   * 
   * @since GemFire 8.1
   */
  private void loadPluginCommands() {
    final Iterator<CommandMarker> iterator  = ServiceLoader.load(CommandMarker.class, ClassPathLoader.getLatest().asClassLoader()).iterator();
    while (iterator.hasNext()) {
      try {
        final CommandMarker commandMarker = iterator.next();
        try {
          add(commandMarker);
        } catch (Exception e) {
          logWrapper.warning("Could not load Command from: " + commandMarker.getClass() + " due to " + e.getLocalizedMessage(), e); // continue
        }
      } catch (ServiceConfigurationError e) {
        logWrapper.severe("Could not load Command: " + e.getLocalizedMessage(), e); // continue
      }
    }
  }
  
  private void loadCommands() throws ClassNotFoundException, IOException {
    loadUserCommands();
    
    loadPluginCommands();
    
    //CommandMarkers
    Set<Class<?>> foundClasses = null;
    try {
      foundClasses = ClasspathScanLoadHelper.loadAndGet("org.apache.geode.management.internal.cli.commands", CommandMarker.class, true);
      for (Class<?> klass : foundClasses) {
        try {
          add((CommandMarker)klass.newInstance());
        } catch (Exception e) {
          logWrapper.warning("Could not load Command from: "+ klass +" due to "+e.getLocalizedMessage()); // continue
        }
      }
      raiseExceptionIfEmpty(foundClasses, "Commands");
    } catch (ClassNotFoundException e) {
      logWrapper.warning("Could not load Commands due to "+ e.getLocalizedMessage());
      throw e;
    } catch (IOException e) {
      logWrapper.warning("Could not load Commands due to "+ e.getLocalizedMessage());
      throw e;
    } catch (IllegalStateException e) {
      logWrapper.warning(e.getMessage(), e);
      throw e;
    }
    
    //Converters
    try {
      foundClasses = ClasspathScanLoadHelper.loadAndGet("org.apache.geode.management.internal.cli.converters", Converter.class, true);
      for (Class<?> klass : foundClasses) {
        try {
          add((Converter<?>)klass.newInstance());
        } catch (Exception e) {
          logWrapper.warning("Could not load Converter from: "+ klass + " due to "+e.getLocalizedMessage()); // continue
        }
      }
      raiseExceptionIfEmpty(foundClasses, "Converters");
    } catch (ClassNotFoundException e) {
      logWrapper.warning("Could not load Converters due to "+ e.getLocalizedMessage());
      throw e;
    } catch (IOException e) {
      logWrapper.warning("Could not load Converters due to "+ e.getLocalizedMessage());
      throw e;
    } catch (IllegalStateException e) {
      logWrapper.warning(e.getMessage(), e);
      throw e;
    }
    
    //Roo's Converters
    try {
      foundClasses = ClasspathScanLoadHelper.loadAndGet("org.springframework.shell.converters", Converter.class, true);
      for (Class<?> klass : foundClasses) {
        try {
          if (!SHL_CONVERTERS_TOSKIP.contains(klass.getName())) {
            add((Converter<?>)klass.newInstance());
          }
        } catch (Exception e) {
          logWrapper.warning("Could not load Converter from: "+klass+ " due to "+e.getLocalizedMessage()); // continue
        }
      }
      raiseExceptionIfEmpty(foundClasses, "Basic Converters");
    } catch (ClassNotFoundException e) {
      logWrapper.warning("Could not load Default Converters due to "+ e.getLocalizedMessage());//TODO - Abhishek: Should these converters be moved in GemFire?
      throw e;
    } catch (IOException e) {
      logWrapper.warning("Could not load Default Converters due to "+ e.getLocalizedMessage());//TODO - Abhishek: Should these converters be moved in GemFire?
      throw e;
    } catch (IllegalStateException e) {
      logWrapper.warning(e.getMessage(), e);
      throw e;
    }
  }

  private static void raiseExceptionIfEmpty(Set<Class<?>> foundClasses, String errorFor) throws IllegalStateException {
    if (foundClasses == null || foundClasses.isEmpty()) {
      throw new IllegalStateException("Required " + errorFor + " classes were not loaded. Check logs for errors.");
    }
  }

  public static CommandManager getInstance() throws ClassNotFoundException, IOException {
    return getInstance(true);
  }
  
  public static CommandManager getInstance(Properties cacheProperties) throws ClassNotFoundException, IOException {
    return getInstance(true, cacheProperties);
  }
  
  // For testing.
  public static void clearInstance() {
    synchronized (INSTANCE_LOCK) {
      INSTANCE = null;
    }
  }
  
  //This method exists for test code use only ...
  /*package*/static CommandManager getInstance(boolean loadDefaultCommands) 
      throws ClassNotFoundException, IOException {
    return getInstance(loadDefaultCommands, null);
  }
  
  private static CommandManager getInstance(boolean loadDefaultCommands, Properties cacheProperties) 
      throws ClassNotFoundException, IOException {
    synchronized (INSTANCE_LOCK) {
      if (INSTANCE == null) {
        INSTANCE = new CommandManager(loadDefaultCommands, cacheProperties);
      }
      return INSTANCE;
    }
  }
  
  public static CommandManager getExisting() {
//    if (INSTANCE == null) {
//      throw new IllegalStateException("CommandManager doesn't exist.");
//    }
    return INSTANCE;
  }

  /** Skip some of the Converters from Spring Shell for our customization  */
  private static List<String> SHL_CONVERTERS_TOSKIP = new ArrayList<String>();
  static {
    //Over-ridden by cggm.internal.cli.converters.BooleanConverter
    SHL_CONVERTERS_TOSKIP.add("org.springframework.shell.converters.BooleanConverter");
    //Over-ridden by cggm.internal.cli.converters.EnumConverter
    SHL_CONVERTERS_TOSKIP.add("org.springframework.shell.converters.EnumConverter");
  }

  /**
   * List of converters which should be populated first before any command can
   * be added
   */
  private final List<Converter<?>> converters = new ArrayList<Converter<?>>();

  /**
   * Map of command string and actual CommandTarget object
   * 
   * This map can also be implemented as a trie to support command abbreviation
   */
  private final Map<String, CommandTarget> commands = new TreeMap<String, CommandTarget>();

  /**
   * This method will store the all the availabilityIndicators
   */
  private final Map<String, AvailabilityTarget> availabilityIndicators = new HashMap<String, AvailabilityTarget>();

  /**
   */
  private final Map<String, CliTopic> topics = new TreeMap<String, CliTopic>();
  
  /**
   * Method to add new Converter
   * 
   * @param converter
   */
  public void add(Converter<?> converter) {
    converters.add(converter);
  }

  /**
   * Method to add new Commands to the parser
   * 
   * @param commandMarker
   */
  public void add(CommandMarker commandMarker) {
 // First we need to find out all the methods marked with
    // Command annotation
    Method[] methods = commandMarker.getClass().getMethods();
    for (Method method : methods) {
      if (method.getAnnotation(CliCommand.class) != null) {

        //
        // First Build the option parser
        //

        // Create the empty LinkedLists for storing the argument and
        // options
        LinkedList<Argument> arguments = new LinkedList<Argument>();
        LinkedList<Option> options = new LinkedList<Option>();
        // Also we need to create the OptionParser for each command
        GfshOptionParser optionParser = getOptionParser();
        // Now get all the parameters annotations of the method
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        // Also get the parameter Types
        Class<?>[] parameterTypes = method.getParameterTypes();

        int parameterNo = 0;

        for (int i = 0; i < parameterAnnotations.length; i++) {
          // Get all the annotations for this specific parameter
          Annotation[] annotations = parameterAnnotations[i];
          // Also get the parameter type for this parameter
          Class<?> parameterType = parameterTypes[i];

          boolean paramFound = false;
          String valueSeparator = CliMetaData.ANNOTATION_NULL_VALUE;
          for (Annotation annotation : annotations) {
            if (annotation instanceof CliArgument) {
              // Here we need to create the argument Object
              Argument argumentToAdd = createArgument((CliArgument) annotation, parameterType, parameterNo);
              arguments.add(argumentToAdd);
              parameterNo++;
            } else if (annotation instanceof CliOption) {
              Option createdOption = createOption((CliOption) annotation, parameterType, parameterNo);
              if (!CliMetaData.ANNOTATION_NULL_VALUE.equals(valueSeparator)) { // CliMetaData was found earlier
                createdOption.setValueSeparator(valueSeparator);

                // reset valueSeparator back to null
                valueSeparator = CliMetaData.ANNOTATION_NULL_VALUE;
              } else { // CliMetaData is yet to be found
                paramFound = true;
              }
              options.add(createdOption);
              parameterNo++;
            } else if (annotation instanceof CliMetaData) {
              valueSeparator = ((CliMetaData)annotation).valueSeparator();
              if (!CliMetaData.ANNOTATION_NULL_VALUE.equals(valueSeparator)) {
                if (paramFound) { //CliOption was detected earlier
                  Option lastAddedOption = options.getLast();
                  lastAddedOption.setValueSeparator(valueSeparator);
                  // reset valueSeparator back to null
                  valueSeparator = CliMetaData.ANNOTATION_NULL_VALUE;
                } // param not found yet, store valueSeparator value
              } else {
                // reset valueSeparator back to null
                valueSeparator = CliMetaData.ANNOTATION_NULL_VALUE;
              }
            }
          }
        }
        optionParser.setArguments(arguments);
        optionParser.setOptions(options);

        //
        // Now build the commandTarget
        //

        // First build the MethodTarget for the command Method
        GfshMethodTarget gfshMethodTarget = new GfshMethodTarget(method,
            commandMarker);

        // Fetch the value array from the cliCommand annotation
        CliCommand cliCommand = method.getAnnotation(CliCommand.class);
        String[] values = cliCommand.value();

        // First string will point to the command
        // rest of them will act as synonyms
        String commandName = null;
        String[] synonyms = null;
        if (values.length > 1) {
          synonyms = new String[values.length - 1];
        }

        commandName = values[0];

        for (int j = 1; j < values.length; j++) {
          synonyms[j - 1] = values[j];
        }

        // Create the commandTarget object
        CommandTarget commandTarget = new CommandTarget(commandName, synonyms,
            gfshMethodTarget, optionParser, null, cliCommand.help());

        // Now for each string in values put an entry in the commands
        // map
        for (String string : values) {
          if(commands.get(string)==null){
            commands.put(string, commandTarget);
          } else {
            //TODO Handle collision
            logWrapper.info("Multiple commands configured with the same name: "+string);
          }
        }

        if (CliUtil.isGfshVM()) {
          CliMetaData commandMetaData = method.getAnnotation(CliMetaData.class);
          if (commandMetaData != null) {
            String[] relatedTopics = commandMetaData.relatedTopic();
//            System.out.println("relatedTopic :: "+Arrays.toString(relatedTopics));
            for (String topicName : relatedTopics) {
              CliTopic topic = topics.get(topicName);
              if (topic == null) {
                topic = new CliTopic(topicName);
                topics.put(topicName, topic);
              }
              topic.addCommandTarget(commandTarget);
            }
          }
        }
        
      } else if (method.getAnnotation(CliAvailabilityIndicator.class) != null) {
        // Now add this availability Indicator to the list of
        // availability Indicators
        CliAvailabilityIndicator cliAvailabilityIndicator = method
            .getAnnotation(CliAvailabilityIndicator.class);

        // Create a AvailabilityTarget for this availability Indicator
        AvailabilityTarget availabilityIndicator = new AvailabilityTarget(
            commandMarker, method);

        String[] value = cliAvailabilityIndicator.value();
        for (String string : value) {
          availabilityIndicators.put(string, availabilityIndicator);
        }

      }
    }
    // Now we must update all the existing CommandTargets to add
    // this availability Indicator if it applies to them
    updateAvailabilityIndicators();
  }
  

/**
   * Will update all the references to availability Indicators for commands
   * 
   */
  public void updateAvailabilityIndicators() {
    for (String string : availabilityIndicators.keySet()) {
      CommandTarget commandTarget = commands.get(string);
      if (commandTarget != null) {
        commandTarget.setAvailabilityIndicator(availabilityIndicators
            .get(string));
      }
    }
  }

  /**
   * Creates a new {@link Option} instance
   * 
   * @param cliOption
   * @param parameterType
   * @param parameterNo
   * @return Option
   */
  public Option createOption(CliOption cliOption, Class<?> parameterType,
      int parameterNo) {
    Option option = new Option();

    // First set the Option identifiers
    List<String> synonyms = new ArrayList<String>();
    for (String string : cliOption.key()) {
      if (!option.setLongOption(string)) {
        synonyms.add(string);
      }
    }
    option.setSynonyms(synonyms);
    if (!(option.getAggregate().size() > 0)) {
      logWrapper.warning("Option should have a name");
    }
    // Set the option Help
    option.setHelp(cliOption.help());

    // Set whether the option is required or not
    option.setRequired(cliOption.mandatory());

    // Set the fields related to option value
    option.setSystemProvided(cliOption.systemProvided());
    option.setSpecifiedDefaultValue(cliOption.specifiedDefaultValue());
    option.setUnspecifiedDefaultValue(cliOption.unspecifiedDefaultValue());

    // Set the things which are useful for value conversion and
    // auto-completion
    option.setContext(cliOption.optionContext());
    // Find the matching Converter<?> for this option
    option.setConverter(getConverter(parameterType, option.getContext()));

    option.setDataType(parameterType);
    option.setParameterNo(parameterNo);
    return option;
  }

  /**
   * Creates a new {@link Argument} instance
   * 
   * @param cliArgument
   * @param parameterType
   * @param parameterNo
   * @return Argument
   */
  public Argument createArgument(CliArgument cliArgument,
      Class<?> parameterType, int parameterNo) {
    Argument argument = new Argument();
    argument.setArgumentName(cliArgument.name());
    argument.setContext(cliArgument.argumentContext());
    argument.setConverter(getConverter(parameterType, argument.getContext()));
    argument.setHelp(cliArgument.help());
    argument.setRequired(cliArgument.mandatory());
    argument.setDataType(parameterType);
    argument.setParameterNo(parameterNo);
    argument.setUnspecifiedDefaultValue(cliArgument.unspecifiedDefaultValue());
    argument.setSystemProvided(cliArgument.systemProvided());
    return argument;
  }

  /**
   * Looks for a matching {@link Converter}
   * 
   * @param parameterType
   * @param context
   * @return {@link Converter}
   */
  public Converter<?> getConverter(Class<?> parameterType, String context) {
    for (Converter<?> converter : converters) {
      if (converter.supports(parameterType, context)) {
        return converter;
      }
    }
    return null;
  }

  /**
   * For the time being this method returns a {@link JoptOptionParser} object
   * but in the future we can change which optionParser should be returned.
   * 
   * @return {@link GfshOptionParser}
   */
  private GfshOptionParser getOptionParser() {
    return new JoptOptionParser();
  }

  /**
   * @return the commands
   */
  public Map<String, CommandTarget> getCommands() {
    return Collections.unmodifiableMap(commands);
  }

  AvailabilityTarget getAvailabilityIndicator(Object key) {
    return availabilityIndicators.get(key);
  }

  public Set<String> getTopicNames() {
    Set<String> topicsNames = topics.keySet();
    return Collections.unmodifiableSet(topicsNames);
  }

  public List<CliTopic> getTopics() {
    List<CliTopic> topicsList = new ArrayList<CliTopic>(topics.values());
    return Collections.unmodifiableList(topicsList);
  }

  public CliTopic getTopic(String topicName) {
    CliTopic foundTopic = topics.get(topicName);

    if (foundTopic == null) {
      Set<Entry<String, CliTopic>> entries = topics.entrySet();
      
      for (Entry<String, CliTopic> entry : entries) {
        if (entry.getKey().equalsIgnoreCase(topicName)) {
          foundTopic = entry.getValue();
          break;
        }
      }
    }

    return foundTopic;
  }
}
