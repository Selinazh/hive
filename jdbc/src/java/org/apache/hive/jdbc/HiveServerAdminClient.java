package org.apache.hive.jdbc;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.service.cli.thrift.TActiveOperations;
import org.apache.hive.service.cli.thrift.TCLIService;
import org.apache.hive.service.cli.thrift.TOperationStatus;
import org.apache.hive.service.cli.thrift.TResourceStatus;
import org.apache.thrift.TException;

import java.sql.SQLException;
import java.util.List;
import java.util.Properties;

public class HiveServerAdminClient {

  private String jdbcURL = "";
  private boolean suppressHeaders = false;

  public HiveServerAdminClient(String jdbcURL, boolean suppressHeaders) {
    this.jdbcURL = jdbcURL;
    this.suppressHeaders = suppressHeaders;
  }

  private String getFormatForResourceUsage() {
    return suppressHeaders? "%s\t%s\t%s\t%s\n" : "%-30s\t%-15s\t%-15s\t%-15s\n";
  }

  private String getFormatForOperationInfo() {
    return suppressHeaders?  "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"
                           : "%-12s\t%-20s\t%-40s\t%-40s\t%-15s\t%-20s\t%-35s\t%-50s\n";
  }

  private void checkAndPrintResourceUsageHeader() {
    if (!suppressHeaders) {
      System.out.println("\nResource Summary:");
      System.out.println(  "=================\n");
      String formatString = getFormatForResourceUsage();
      System.out.printf(formatString, "ResourceName", "ResourceType", "UsedCapacity", "MaxCapacity");
      System.out.printf(formatString, "------------", "------------", "------------", "-----------");
    }
  }

  private void checkAndPrintOperationInfoHeader() {
    if (!suppressHeaders) {
      System.out.println("\nOperation Info:");
      System.out.println(  "===============\n");
      String formatString = getFormatForOperationInfo();
      System.out.printf(formatString, "User", "Host", "SessionId", "TezSessionId", "OpStatus", "OpType", "AppId", "Query");
      System.out.printf(formatString, "----", "----", "---------", "------------", "--------", "------", "-----", "-----");
    }
  }

  public void printResourceUsage() throws TException, SQLException {
    try (HiveConnection adminClient = new HiveConnection(jdbcURL, new Properties())) {
      TCLIService.Iface client = adminClient.getClient();
      List<TResourceStatus> resourceStatusList =
          client.GetResourceConsumptionList().getResourcesConsumed();
      checkAndPrintResourceUsageHeader();
      if (resourceStatusList.size() > 0) {
        for (TResourceStatus rs : resourceStatusList) {
          System.out.printf(
              getFormatForResourceUsage(),
                rs.getResourceName(),
                rs.getResourceType(),
                rs.getUsedCapacity(),
                rs.getMaxCapacity());
        }
      }
    }
  }

  public void printOperationInfo() throws TException, SQLException {

    try (HiveConnection adminClient = new HiveConnection(jdbcURL, new Properties())) {
      TCLIService.Iface client = adminClient.getClient();
      TActiveOperations activeOperations = client.GetActiveOperations();

      checkAndPrintOperationInfoHeader();
      if (activeOperations.isSetActiveOperations() &&
          !activeOperations.getActiveOperations().isEmpty()) {
        for (TOperationStatus opStatus : activeOperations.getActiveOperations()) {
          System.out.printf(
              getFormatForOperationInfo(),
                opStatus.getUser(),
                opStatus.getHost(),
                opStatus.getSessionID(),
                opStatus.getTezSessionID(),
                opStatus.getOperationStatus().name(),
                opStatus.getOperationType().name(),
                opStatus.getTrackingURL(),
                opStatus.getQuery()
          );
        }
      }
    }
    catch (Exception exception) {
      System.err.println("Error!");
      exception.printStackTrace();
    }
  }

  private static final Option JDBC_URL = new Option("j", "jdbc-url", true, "JDBC URL for HiveServer2");
  private static final Option RESOURCES = new Option("r", "resources", false, "Print resource-usage summary");
  private static final Option OPERATIONS = new Option("o", "operations", false, "Print list of active operations");
  private static final Option SUPPRESS_HEADERS = new Option("s", "suppress-headers", false, "Suppress headers and simplify formatting");
  private static final Option HELP = new Option("h", "help", false, "Print help/usage");

  private static Options getCliOptions() {
    Options cliOptions = new Options();
    cliOptions.addOption(JDBC_URL);
    cliOptions.addOption(RESOURCES);
    cliOptions.addOption(OPERATIONS);
    cliOptions.addOption(SUPPRESS_HEADERS);
    cliOptions.addOption(HELP);
    return cliOptions;
  }

  private static CommandLine parse(Options cliOptions, String[] args) {
    try {
      return new GnuParser().parse(cliOptions, args, false);
    }
    catch (ParseException parseFailure) {
      System.err.println("Error parsing options." + parseFailure.getMessage());
      printHelp(cliOptions);
      return null;
    }
  }

  private static void printHelp(Options cliOptions) {
    new HelpFormatter().printHelp("HiveServerAdminClient", cliOptions);
  }

  public static void main(String[] args) throws SQLException, TException {
    Options cliOptions = getCliOptions();
    CommandLine commandLine = parse(cliOptions, args);
    if (commandLine == null) {
      // Parse failure. Bail.
      return;
    }

    if (args.length == 0 || commandLine.hasOption(HELP.getLongOpt())) {
      printHelp(cliOptions);
      return;
    }

    String jdbcURL = commandLine.getOptionValue("jdbc-url");
    if (StringUtils.isBlank(jdbcURL)) {
      System.err.println("JDBC URL can't be blank!");
    }

    HiveServerAdminClient adminClient = new HiveServerAdminClient(jdbcURL,
                                                                  commandLine.hasOption(SUPPRESS_HEADERS.getLongOpt()));

    if (commandLine.hasOption(RESOURCES.getLongOpt())) {
      adminClient.printResourceUsage();
    }

    if (commandLine.hasOption(OPERATIONS.getLongOpt())) {
      adminClient.printOperationInfo();
    }
  }
}
