// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.KijiGatherJobBuilder;
import org.kiji.mapreduce.output.TextMapReduceJobOutput;
import org.kiji.mapreduce.lib.reduce.IntSumReducer;

/**
 * A program that runs the {@link com.wibidata.core.client.lib.examples.EmailDomainCountGatherer}
 * over a Wibi table.
 *
 * <p>To run this job from the command line:</p>
 *
 * <pre>
 * $ java -cp `$WIBI_HOME/bin/wibi classpath` \
 * &gt;   com.wibidata.core.client.lib.examples.EmailDomainCountGatherJob \
 * &gt;   instance-name table-name output-path num-splits
 * </pre>
 */
public class EmailDomainCountGatherJob extends Configured implements Tool {
  /** A logger. */
  private static final Logger LOG = LoggerFactory.getLogger(EmailDomainCountGatherJob.class);

  /** {@inheritDoc} */
  @Override
  public int run(String[] args) throws Exception {
    if (4 != args.length) {
      throw new IllegalArgumentException("Invalid number of arguments. "
          + "Requires instance-name, table-name, output-path, and num-splits.");
    }

    // Read the arguments from the commmand-line.
    String instanceName = args[0];
    String wibiTableName = args[1];
    Path outputPath = new Path(args[2]);
    int numSplits = Integer.parseInt(args[3]);
    LOG.info("Configuring a gather job over table " + wibiTableName + ".");
    LOG.info("Writing output to " + outputPath + ".");
    LOG.info("Using " + numSplits + " reducers.");

    LOG.info("Loading HBase configuration...");
    setConf(HBaseConfiguration.addHbaseResources(getConf()));

    LOG.info("Opening a wibi connection...");
    KijiConfiguration wibiConf = new KijiConfiguration(getConf(), instanceName);
    Kiji wibi = Kiji.open(wibiConf);

    LOG.info("Opening wibi table " + wibiTableName + "...");
    KijiTable table = wibi.openTable(wibiTableName);

    LOG.info("Configuring a gather job...");
    KijiGatherJobBuilder jobBuilder = new KijiGatherJobBuilder()
        .withInputTable(table)
        .withGatherer(EmailDomainCountGatherer.class)
        .withCombiner(IntSumReducer.class)
        .withReducer(IntSumReducer.class)
        .withOutput(new TextMapReduceJobOutput(outputPath, numSplits));

    LOG.info("Building the gather job...");
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
   *     EmailDomainCountGatherJob &lt;wibi-instance&gt; &lt;wibi-table&gt; &lt;output-path&gt;
   *       &lt;num-splits&gt;
   *
   * ARGUMENTS:
   *
   *     wibi-instance: Name of the wibi instance the table is in.
   *
   *     wibi-table: Name of the wibi table gather over.
   *
   *     output-path: The path to the output files to generate.
   *
   *     num-splits: The number of output file shards to generate (determines number of reducers).
   * </pre>
   *
   * @param args The command-line arguments.
   * @throws Exception If there is an error.
   */
  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new EmailDomainCountGatherJob(), args));
  }
}
