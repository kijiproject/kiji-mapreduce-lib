// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.reduce;

import org.kiji.mapreduce.reducer.KeyPassThroughReducer;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

import com.wibidata.core.license.Feature;
import com.wibidata.core.license.LicensedBy;

/**
 * <p>A WibiReducer that works of key/value pairs where the value is
 * an LongWritable.  For all integer values with the same key, the
 * LongSumReducer will output a single pair with a value equal to the
 * sum, leaving the key unchanged.</p>
 *
 * @param <K> The type of the reduce input key.
 */
@LicensedBy(Feature.WIBI_LIB)
public class LongSumReducer<K> extends KeyPassThroughReducer<K, LongWritable, LongWritable> {
  private LongWritable mValue;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) {
    mValue = new LongWritable();
  }

  /** {@inheritDoc} */
  @Override
  protected void reduce(K key, Iterable<LongWritable> values,
      Context context) throws IOException, InterruptedException {
    long sum = 0;
    for (LongWritable value : values) {
      sum += value.get();
    }
    mValue.set(sum);
    context.write(key, mValue);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return LongWritable.class;
  }
}
