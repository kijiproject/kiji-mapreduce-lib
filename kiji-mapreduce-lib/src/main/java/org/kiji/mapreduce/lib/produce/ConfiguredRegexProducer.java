// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.produce;

import org.apache.hadoop.conf.Configuration;
import org.kiji.hadoop.configurator.HadoopConf;
import org.kiji.hadoop.configurator.HadoopConfigurator;

import com.wibidata.core.license.Feature;
import com.wibidata.core.license.LicensedBy;

/**
 * A regex producer that is ready to use "out of the box."  All you
 * need to do is specify a couple of configuration values.
 *
 * Here is an example of the EmailDomainProfiler implemented with
 * ConfiguredRegexProducer that reads from "info:email" and writes to
 * "derived:domain":
 *
 * bin/wibi produce \
 *   -Dwibi.regexproducer.input.column=info:email \
 *   -Dwibi.regexproducer.output.column=derived:domain \
 *   -Dwibi.regexproducer.regex='[^@]+@(.*)' \
 *   --input=wibi:foo \
 *   --output=wibi \
 *   --producer=com.wibidata.core.produce.ConfiguredRegexProducer
 */
@LicensedBy(Feature.WIBI_LIB)
public class ConfiguredRegexProducer extends RegexProducer {
  public static final String CONF_INPUT_COLUMN = "wibi.regexproducer.input.column";
  public static final String CONF_OUTPUT_COLUMN = "wibi.regexproducer.output.column";
  public static final String CONF_REGEX = "wibi.regexproducer.regex";

  @HadoopConf(key=CONF_INPUT_COLUMN, usage="The input column name.")
  private String mInputColumn;

  @HadoopConf(key=CONF_OUTPUT_COLUMN, usage="The output column name.")
  private String mOutputColumn;

  @HadoopConf(key=CONF_REGEX, usage="The regular expression used to extract from the input column.")
  private String mRegex;

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    HadoopConfigurator.configure(this);
  }

  @Override
  protected String getInputColumn() {
    if (null == mInputColumn || mInputColumn.isEmpty()) {
      throw new RuntimeException(CONF_INPUT_COLUMN + " not found");
    }
    return mInputColumn;
  }

  @Override
  protected String getRegex() {
    if (null == mRegex || mRegex.isEmpty()) {
      throw new RuntimeException(CONF_REGEX + " not found");
    }
    return mRegex;
  }

  @Override
  public String getOutputColumn() {
    if (null == mOutputColumn || mOutputColumn.isEmpty()) {
      throw new RuntimeException(CONF_OUTPUT_COLUMN + " not found");
    }
    int colon = mOutputColumn.indexOf(":");
    if (colon < 0) {
      throw new RuntimeException(CONF_OUTPUT_COLUMN + " must be in family:qualifier format");
    }
    return mOutputColumn;
  }
}
