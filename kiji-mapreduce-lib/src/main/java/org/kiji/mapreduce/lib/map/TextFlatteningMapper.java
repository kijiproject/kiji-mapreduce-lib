// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.mapper;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.kiji.mapreduce.KijiBaseMapper;
import com.wibidata.core.license.Feature;
import com.wibidata.core.license.LicensedBy;

/**
 * A Mapper that takes name/text records, and outputs name/flattened-text pairs.
 * Here, "flattened-text" refers to the original text with all newlines
 * replaced by single spaces.
 */
@LicensedBy(Feature.WIBI_LIB)
public class TextFlatteningMapper extends KijiBaseMapper<Text, Text, Text, Text> {
  /** Reusable Text to store the contents of each file to write. */
  private Text mFlattenedFile;

  /**
   * Prepares internal state for executing map tasks.
   *
   * @param context The Context to read.  Unused.
   */
  @Override
  protected void setup(Context context) {
    mFlattenedFile = new Text();
  }

  /**
   * Converts The Bytes stored in fileContents to a single String with all new-line
   * characters removed.
   *
   * @param fileName The qualified path to this file.
   * @param fileContents The file to convert, encoded in UTF8.
   * @param context The Context to write to.
   * @throws IOException if there is an error.
   * @throws InterruptedException if there is an error.
   */
  @Override
  protected void map(Text fileName, Text fileContents, Context context)
      throws IOException, InterruptedException {
    // Run over file and remove each newline character.
    // These files are expected to be small (and already fit in a Text object)
    // so we should be able to toString() them.
    String text = fileContents.toString();

    // Replace all newlines with spaces.
    String withoutNewlines = text.replaceAll("\n", " ");
    mFlattenedFile.set(withoutNewlines);

    context.write(fileName, mFlattenedFile);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return Text.class;
  }
}
