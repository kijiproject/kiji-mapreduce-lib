// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.reduce;

import org.kiji.mapreduce.reducer.KeyPassThroughReducer;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;

import com.wibidata.core.license.Feature;
import com.wibidata.core.license.LicensedBy;

/**
 * <p>A WibiReducer that works on key/value pairs where the value is
 * an IntWritable.  For all integer values with the same key, the
 * IntSumReducer will output a single pair with a value equal to the
 * sum, leaving the key unchanged.</p>
 *
 * @param <K> The type of the reduce input key.
 */
@LicensedBy(Feature.WIBI_LIB)
public class IntSumReducer<K> extends KeyPassThroughReducer<K, IntWritable, IntWritable> {
  private IntWritable mValue;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    mValue = new IntWritable();
  }

  /** {@inheritDoc} */
  @Override
  protected void reduce(K key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable value : values) {
      sum += value.get();
    }
    mValue.set(sum);
    context.write(key, mValue);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return IntWritable.class;
  }
}
