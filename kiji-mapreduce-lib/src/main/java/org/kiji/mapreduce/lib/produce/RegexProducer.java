// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.produce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kiji.schema.KijiRowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiContext;
import com.wibidata.core.license.Feature;
import com.wibidata.core.license.LicensedBy;

/**
 * A producer which uses a full regular expression match on a string
 * input column to generate a new column.  The regular expression
 * should have a single group (things inside parentheses), which will
 * be extracted and used as the new data.
 */
@LicensedBy(Feature.WIBI_LIB)
public abstract class RegexProducer extends SingleInputProducer {
  private static final Logger LOG = LoggerFactory.getLogger(RegexProducer.class);

  private Pattern mPattern;

  /**
   * @return the regular expression string (will match regex against full string).
   */
  protected abstract String getRegex();

  /** {@inheritDoc} */
  @Override
  public void setup(KijiContext context) throws IOException {
    super.setup(context);
    mPattern = Pattern.compile(getRegex());
  }

  /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData input, Context context)
      throws IOException, InterruptedException {
    if (!input.containsColumn(getInputColumnName().getFamily(),
            getInputColumnName().getQualifier())) {
      LOG.debug("No " + getInputColumnName().getName() + " for entity: " + input.getEntityId());
    }
    String string = input.getStringValue(getInputColumnName().getFamily(),
        getInputColumnName().getQualifier()).toString();

    // Run the regex on the input string.
    Matcher matcher = mPattern.matcher(string);
    if (matcher.matches()) {
      if (matcher.groupCount() == 1) {
        context.write(matcher.group(1));
      }
    } else {
      LOG.debug(input.getEntityId().toString() + "'s data '" + string + "' does not match "
          + mPattern.pattern());
    }
  }
}
