// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.gather;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.kiji.hadoop.configurator.HadoopConf;
import org.kiji.hadoop.configurator.HadoopConfigurator;
import org.kiji.schema.EntityId;
import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

import org.kiji.mapreduce.KijiGatherer;
import com.wibidata.core.license.Feature;
import com.wibidata.core.license.LicensedBy;

/**
 * Gatherer to flatten map-type wibi data into delimited files in hdfs.
 * <p>This implementation writes one file for each mapper created.
 * Each line contains one key-value pair from a map-type family.
 * By default, data is output in the format:</p>
 * <br/>
 * <p><tt>[entityid]|[timestamp]|[key]|[value_as_json]</tt></p>
 *
 * <p>As an exception to this pattern, strings are printed explicitly, instead of within
 * quotes, as json would normally print them.</p>
 *
 * <p>Values are output as json representations of the underlying data.
 * By default, the delimiter between timestamp and data (and subsequent data) is a pipe ("|").
 * You can set this value by setting the wibi.export.field.delimiter configuration variable.
 * You must guarantee that the delimiter you choose does not appear in the json
 * representation of your data.</p>
 */
@LicensedBy(Feature.WIBI_CORE)
public class MapTypeDelimitedFileGatherer extends KijiGatherer<Text, NullWritable> {
  /**
   * Delimiter used to separate entityid, timestamp, key, and value data when writing to hdfs.
   * In order for hive to parse the generated file correctly, this character can NOT
   * match any character in the json representation of wibi data.
   * Choose an appropriate delimiter.
   */
  private static final String CONF_FIELD_DELIMITER = "wibi.export.field.delimiter";

  /** Default delimiter to use for writing column values into hdfs. */
  private static final String DEFAULT_FIELD_DELIMITER = "|";

  /** The Map-type family to export. */
  private static final String CONF_EXPORT_FAMILY = "wibi.export.map.family";

  /** The maximum number of versions to return for any key. */
  private static final String CONF_MAX_VERSIONS = "wibi.export.max.versions";

  /** Delimiter to write between field data. */
  private String mFieldDelimiter;

  /** Map-type family to export. */
  private String mFamily;

  @HadoopConf(key=CONF_MAX_VERSIONS, usage="Max number of versions to return for any key.")
  private int mMaxVersions = 1;

  /** Single text object for map taske to write into to reduce object creation. */
  private Text mLine;

  /**
   * Initializes internal state from the Configuration.
   * Sets the delimiter to write between columns, the family to write to, and
   * the max versions to read from each column.
   *
   * @param conf The Configuration to initialize from.
   */
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    HadoopConfigurator.configure(this);
  }

  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column(mFamily).withMaxVersions(mMaxVersions));
    return dataRequest;
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    mLine = new Text();
  }

  /**
   * Sets the delimiter.
   *
   * @param delimiter The delimiter.
   */
  @HadoopConf(key=CONF_FIELD_DELIMITER, defaultValue=DEFAULT_FIELD_DELIMITER)
  protected void setFieldDelimiter(String delimiter) {
    if (delimiter.length() != 1) {
      throw new RuntimeException("Delimiter must be exactly one character long."
          + "  Received: \"" + delimiter + "\".");
    }
    mFieldDelimiter = delimiter;
  }

  /**
   * Sets the family.
   *
   * @param family The family.
   */
  @HadoopConf(key=CONF_EXPORT_FAMILY)
  protected void setFamily(String family) {
    KijiColumnName name = new KijiColumnName(family);
    if (name.isFullyQualified()) {
      throw new RuntimeException("Expected an unqualified map type family. "
          + "Requested family was: " + name.getName());
    }
    mFamily = family;
  }

  /**
   * Outputs flattened data without schema definitions.
   * A single line of data contains one key-value record from a wibi family, formatted as:
   * [entityid]|[timestamp]|[key]|[value]
   *
   * @param input The row data to export.
   * @param context The context to write export to.
   * @throws IOException if there's an error.
   * @throws InterruptedException if there's an error.
   */
  @Override
  protected void gather(KijiRowData input, Context context)
      throws IOException, InterruptedException {
    for (String key : input.getQualifiers(mFamily)) {
    NavigableMap<Long, Object> values = input.getValues(mFamily, key, (Schema) null);
      for (Map.Entry<Long, Object> e : values.entrySet()) {
        // Write this entry out on a single line.
        mLine.set(makeLine(input.getEntityId(), e.getKey(), key, e.getValue()));
        context.write(mLine, NullWritable.get());
      }
    }
  }

  /**
   * Returns a line formatted as:
   * [entityid]|[timestamp]|[key]|[datum_as_json]
   *
   * As an exception to this format, Strings are printed without surrounding quotes.
   * All other data types are returned as json.
   *
   * @param row The EntityId of this row.
   * @param timestamp The timestamp.
   * @param key The key for this map (identical to the row qualifier).
   * @param datum The datum to encode as json.
   * @return The line to export.
   */
  private String makeLine(EntityId row, long timestamp, String key, Object datum) {
    StringBuilder sb = new StringBuilder();
    sb.append(Bytes.toStringBinary(row.getHBaseRowKey()));
    sb.append(mFieldDelimiter);

    sb.append(Long.toString(timestamp));
    sb.append(mFieldDelimiter);

    sb.append(key);
    sb.append(mFieldDelimiter);

    // If datum is a string, print it without quotes.  Otherwise, convert datum to json.
    if (datum instanceof CharSequence) {
      sb.append(datum.toString());
    } else {
      sb.append(GenericData.get().toString(datum)); // Converts the datum to json.
    }

    return sb.toString();
  }

  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  @Override
  public Class<?> getOutputValueClass() {
    return NullWritable.class;
  }
}
