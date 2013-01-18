// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.KijiProduceJobBuilder;
import org.kiji.mapreduce.output.KijiTableMapReduceJobOutput;

/**
 * A program that runs the {@link com.wibidata.core.client.lib.examples.EmailDomainProducer}
 * over a Wibi table.
 *
 * <p>To run this job from the command line:</p>
 *
 * <pre>
 * $ java -cp `$WIBI_HOME/bin/wibi classpath` \
 * &gt;   com.wibidata.core.client.lib.examples.EmailDomainProduceJob \
 * &gt;   instance-name table-name
 * </pre>
 */
public class EmailDomainProduceJob extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(EmailDomainProduceJob.class);

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    if (2 != args.length) {
      throw new IllegalArgumentException(
          "Invalid number of arguments. Requires wibi-instance-name and table-name.");
    }

    // Read the arguments from the command-line.
    String instanceName = args[0];
    String wibiTableName = args[1];
    LOG.info("Configuring a produce job over table " + wibiTableName + ".");

    LOG.info("Loading HBase configuration...");
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    LOG.info("Opening a wibi connection...");
    KijiConfiguration wibiConf = new KijiConfiguration(getConf(), instanceName);
    Kiji wibi = Kiji.open(wibiConf);

    LOG.info("Opening wibi table " + wibiTableName + "...");
    KijiTable table = wibi.openTable(wibiTableName);

    LOG.info("Configuring a produce job...");
    KijiProduceJobBuilder jobBuilder = new KijiProduceJobBuilder()
        .withInputTable(table)
        .withProducer(EmailDomainProducer.class)
        .withOutput(new KijiTableMapReduceJobOutput(table));

    LOG.info("Building the produce job...");
    MapReduceJob job = jobBuilder.build();

    LOG.info("Running the job...");
    boolean isSuccessful = job.run();

    table.close();
    wibi.close();

    LOG.info(isSuccessful ? "Job succeeded." : "Job failed.");
    return isSuccessful ? 0 : 1;
  }

  /**
   * The program's entry point.
   *
   * <pre>
   * USAGE:
   *
   *     EmailDomainProduceJob &lt;wibi-instance&gt; &lt;wibi-table&gt;
   *
   * ARGUMENTS:
   *
   *     wibi-instance: Name of the wibi instance the table is in.
   *
   *     wibi-table: Name of the wibi table to produce over.
   * </pre>
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new EmailDomainProduceJob(), args));
  }
}
