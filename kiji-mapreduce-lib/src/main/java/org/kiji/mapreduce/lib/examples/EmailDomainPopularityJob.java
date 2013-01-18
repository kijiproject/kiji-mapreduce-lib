// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.examples;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.kiji.schema.Kiji;
import org.kiji.schema.KijiConfiguration;
import org.kiji.schema.KijiTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.MapReduceJob;
import org.kiji.mapreduce.KijiGatherJobBuilder;
import org.kiji.mapreduce.KijiTransformJobBuilder;
import org.kiji.mapreduce.input.SequenceFileMapReduceJobInput;
import org.kiji.mapreduce.output.AvroKeyValueMapReduceJobOutput;
import org.kiji.mapreduce.output.SequenceFileMapReduceJobOutput;
import org.kiji.mapreduce.lib.reduce.IntSumReducer;

/**
 * A program that generates an Avro file of email domains sorted by decreasing popularity.
 *
 * <p>This program runs two MapReduce jobs:</p>
 *
 * <ol>
 *   <li>The EmailDomainCountGatherer and IntSumReducer are used to generate a map from
 *       email domains to their popularity.</li>
 *   <li>The InvertCountMapper and TextListReducer are used to invert the output into a
 *       sorted map from popularity to list of email domains.</li>
 * </ol>
 *
 * <p>To run this job from the command line:</p>
 *
 * <pre>
 * $ java -cp `$WIBI_HOME/bin/wibi classpath` \
 * &gt;   com.wibidata.core.client.lib.examples.EmailDomainPopularityJob \
 * &gt;   instance-name table-name output-path num-splits
 * </pre>
 */
public class EmailDomainPopularityJob extends Configured implements Tool {
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

    LOG.info("Running the first job: Count email domain popularity...");
    Path emailDomainCountPath = new Path(outputPath, "email-domain-count");
    boolean isFirstJobSuccessful = countEmailDomainPopularity(
        table, emailDomainCountPath, numSplits);
    if (!isFirstJobSuccessful) {
      LOG.error("First job failed.");
      return 1;
    }

    LOG.info("Running the second job: Invert and sort...");
    Path sortedPopularityPath = new Path(outputPath, "sorted-popularity");
    boolean isSecondJobSuccessful = invertAndSortByPopularity(
        emailDomainCountPath, sortedPopularityPath, numSplits, wibiConf);
    if (!isSecondJobSuccessful) {
      LOG.error("Second job failed.");
      return 2;
    }

    table.close();
    wibi.close();

    return 0;
  }

  /**
   * Runs the email domain count gather job to generate a map from email domain to popularity.
   *
   * @param table The input Wibi table of users.
   * @param outputPath The output path for the map from email domains to their popularity.
   * @param numSplits The number of output file shards to write.
   * @return Whether the job was successful.
   * @throws Exception If there is an exception.
   */
  private boolean countEmailDomainPopularity(KijiTable table, Path outputPath, int numSplits)
      throws Exception {
    LOG.info("Configuring a gather job...");
    KijiGatherJobBuilder jobBuilder = new KijiGatherJobBuilder()
        .withInputTable(table)
        .withGatherer(EmailDomainCountGatherer.class)
        .withCombiner(IntSumReducer.class)
        .withReducer(IntSumReducer.class)
        .withOutput(new SequenceFileMapReduceJobOutput(outputPath, numSplits));

    LOG.info("Building the gather job...");
    MapReduceJob job = jobBuilder.build();

    LOG.info("Running the gather job...");
    return job.run();
  }

  /**
   * Runs the job to invert the email domain popularity map and sort by popularity.
   *
   * @param inputPath The map from email domains to their popularity.
   * @param outputPath The output path for the sorted map of popularity to email domains.
   * @param numSplits The number of output file shards to write.
   * @param wibiConf the Wibi configuration object to be used.
   * @return Whether the job was successful.
   * @throws Exception If there is an exception.
   */
  private boolean invertAndSortByPopularity(
      Path inputPath, Path outputPath, int numSplits, KijiConfiguration wibiConf)
      throws Exception {
    LOG.info("Configuring a transform job...");
    KijiTransformJobBuilder jobBuilder = new KijiTransformJobBuilder()
        .withKijiConfiguration(wibiConf)
        .withInput(new SequenceFileMapReduceJobInput(inputPath))
        .withMapper(InvertCountMapper.class)
        .withReducer(TextListReducer.class)
        .withOutput(new AvroKeyValueMapReduceJobOutput(outputPath, numSplits));

    LOG.info("Building the transform job...");
    MapReduceJob job = jobBuilder.build();

    // Configure the job to sort by decreasing key, so the most popular email domain is first.
    job.getHadoopJob().setSortComparatorClass(DescendingIntWritableComparator.class);

    LOG.info("Running the transform job...");
    return job.run();
  }

  /** A comparator that sorts IntWritables in descending order. */
  public static class DescendingIntWritableComparator extends IntWritable.Comparator {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      // Invert the order.
      return super.compare(b2, s2, l2, b1, s1, l1);
    }
  }

  /**
   * The program's entry point.
   *
   * <pre>
   * USAGE:
   *
   *     EmailDomainPopularityJob &lt;wibi-instance&gt; &lt;wibi-table&gt; &lt;output-path&gt;
   *      &lt;num-splits&gt;
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
    System.exit(ToolRunner.run(new EmailDomainPopularityJob(), args));
  }
}
