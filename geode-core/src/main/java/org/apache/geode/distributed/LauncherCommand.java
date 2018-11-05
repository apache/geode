// package org.apache.geode.distributed;
//
//
//
//
// public interface LauncherCommand {
// String name = Strings.EMPTY;
// List<String> options = Collections.emptyList();
//
// /**
// * Determines whether the specified name refers to a valid launcher command, as defined
// * by the enumerated type implementing this interface.
// *
// * @param name a String value indicating the potential name of a launcher command.
// * @return a boolean indicating whether the specified name for a launcher command is
// * valid.
// */
// public static boolean isCommand(final String name) {
// return (valueOfName(name) != null);
// }
//
// /**
// * Determines whether the given launcher command has been properly specified. The
// * command is deemed unspecified if the reference is null or the Command is UNSPECIFIED.
// *
// * @param command the launcher command.
// * @return a boolean value indicating whether the launcher command is unspecified.
// * @see LauncherCommand#UNSPECIFIED
// */
// static boolean isUnspecified(final LauncherCommand command) {
// return (command == null || command.isUnspecified());
// }
//
// /**
// * Looks up a Locator launcher command by name. The equality comparison on name is
// * case-insensitive.
// *
// * @param name a String value indicating the name of the Locator launcher command.
// * @return an enumerated type representing the command name or null if the no such command with
// * the specified name exists.
// */
// static LauncherCommand valueOfName(final String name) {
// for (LauncherCommand command : values()) {
// if (command.getName().equalsIgnoreCase(name)) {
// return command;
// }
// }
//
// return null;
// }
// /**
// * Gets the name of the Locator launcher command.
// *
// * @return a String value indicating the name of the Locator launcher command.
// */
// default String getName() {
// return this.name;
// }
//
// /**
// * Gets a set of valid options that can be used with the Locator launcher command when used from
// * the command-line.
// *
// * @return a Set of Strings indicating the names of the options available to the Locator
// * launcher command.
// */
// default List<String> getOptions() {
// return this.options;
// }
//
// /**
// * Determines whether this Locator launcher command has the specified command-line option.
// *
// * @param option a String indicating the name of the command-line option to this command.
// * @return a boolean value indicating whether this command has the specified named command-line
// * option.
// */
// default boolean hasOption(final String option) {
// return getOptions().contains(lowerCase(option));
// }
//
// /**
// * Convenience method for determining whether this is the UNSPECIFIED Locator launcher command.
// *
// * @return a boolean indicating if this command is UNSPECIFIED.
// */
// boolean isUnspecified();
//
// /**
// * Gets the String representation of this Locator launcher command.
// *
// * @return a String value representing this Locator launcher command.
// */
// @Override
// default String toString() {
// return getName();
// }
// }
