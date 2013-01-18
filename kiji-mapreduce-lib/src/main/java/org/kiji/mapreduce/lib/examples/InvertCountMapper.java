// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.KijiBaseMapper;

/**
 * A map that inverts its (Text, IntWritable) records into (IntWritable, Text) records.
 *
 * <p>This useful if you need to sort the value from the results of a previous job.  For
 * example, you could use this map to invert the output of the
 * EmailDomainCountGatherJob to generate a dataset of email domains sorted by their
 * popularity.</p>
 */
public class InvertCountMapper extends KijiBaseMapper<Text, IntWritable, IntWritable, Text> {
  /** {@inheritDoc} */
  @Override
  public void map(Text key, IntWritable value, Context context)
      throws IOException, InterruptedException {
    context.write(value, key);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return IntWritable.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return Text.class;
  }
}
