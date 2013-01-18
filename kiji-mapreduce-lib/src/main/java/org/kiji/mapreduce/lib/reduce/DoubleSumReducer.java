// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.reduce;

import org.kiji.mapreduce.reducer.KeyPassThroughReducer;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import com.wibidata.core.license.Feature;
import com.wibidata.core.license.LicensedBy;

/**
 * <p>A WibiReducer that works of key/value pairs where the value is
 * an DoubleWritable.  For all integer values with the same key, the
 * DoubleSumReducer will output a single pair with a value equal to the
 * sum, leaving the key unchanged.</p>
 *
 * @param <K> The type of the reduce input key.
 */
@LicensedBy(Feature.WIBI_LIB)
public class DoubleSumReducer<K> extends KeyPassThroughReducer<K, DoubleWritable, DoubleWritable> {
  private DoubleWritable mValue;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    mValue = new DoubleWritable();
  }

  /** {@inheritDoc} */
  @Override
  protected void reduce(K key, Iterable<DoubleWritable> values,
      Context context) throws IOException, InterruptedException {
    double sum = 0.0;
    for (DoubleWritable value : values) {
      sum += value.get();
    }
    mValue.set(sum);
    context.write(key, mValue);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return DoubleWritable.class;
  }
}
