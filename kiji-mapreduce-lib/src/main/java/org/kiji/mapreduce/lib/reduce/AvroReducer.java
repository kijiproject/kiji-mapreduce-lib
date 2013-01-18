// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.reduce;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;

import org.kiji.mapreduce.AvroKeyWriter;
import org.kiji.mapreduce.KijiBaseReducer;
import com.wibidata.core.license.Feature;
import com.wibidata.core.license.LicensedBy;

/**
 * Base class for reducers used with AvroOutputFormat to write Avro container files.
 *
 * @param <K> The type of the MapReduce reducer input key.
 * @param <V> The type of the MapReduce reducer input value.
 * @param <T> The Avro type of the messages to output to the Avro container files.
 */
@LicensedBy(Feature.WIBI_CORE)
public abstract class AvroReducer<K, V, T> extends KijiBaseReducer<K, V, AvroKey<T>, NullWritable>
    implements AvroKeyWriter {
  /** A shared AvroKey wrapper that is reused when writing MapReduce output keys. */
  private AvroKey<T> mKey;

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    mKey = new AvroKey<T>(null);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return AvroKey.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return NullWritable.class;
  }

  /**
   * Subclasses can use this instead of context.write() to output Avro
   * messages directly instead of having to wrap them in AvroKey
   * container objects.
   *
   * @param value The avro value to write.
   * @param context The reducer context.
   * @throws IOException If there is an error.
   * @throws InterruptedException If the thread is interrupted.
   */
  protected void write(T value, Context context) throws IOException, InterruptedException {
    mKey.datum(value);
    context.write(mKey, NullWritable.get());
  }
}
