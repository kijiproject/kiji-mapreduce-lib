// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.examples;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

import org.kiji.mapreduce.KijiGatherer;

/**
 * Computes the domain from the email, and outputs it as the key with an int count of 1.
 *
 * <p>When used with an <code>IntSumReducer</code>, the resulting dataset is a map from
 * email domains to the number of users with that email domain.</p>
 *
 * <p>To run this from the command line:<p>
 *
 * <pre>
 * $ $WIBI_HOME/bin/wibi gather \
 * &gt;   --input=wibi:tablename \
 * &gt;   --gatherer=com.wibidata.core.client.lib.examples.EmailDomainCountGatherer \
 * &gt;   --reducer=com.wibidata.core.client.lib.reduce.IntSumReducer \
 * &gt;   --output=text:email-domain-counts@1
 * </pre>
 */
public class EmailDomainCountGatherer extends KijiGatherer<Text, IntWritable> {
  /** A reusable output key instance to hold the email domain of the user. */
  private Text mDomain;

  /** A reusable output value instance to hold the integer count (always one). */
  private static final IntWritable ONE = new IntWritable(1);

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column("info", "email"));
    return dataRequest;
  }

  /** {@inheritDoc} */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    mDomain = new Text();
  }

  /** {@inheritDoc} */
  @Override
  protected void gather(KijiRowData input, Context context)
      throws IOException, InterruptedException {
    if (!input.containsColumn("info", "email")) {
      // No email data.
      return;
    }
    String email = input.getStringValue("info", "email").toString();
    int atSymbol = email.indexOf("@");
    if (atSymbol < 0) {
      // Invalid email.
      return;
    }
    String domain = email.substring(atSymbol + 1);
    mDomain.set(domain);
    context.write(mDomain, ONE);
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputKeyClass() {
    return Text.class;
  }

  /** {@inheritDoc} */
  @Override
  public Class<?> getOutputValueClass() {
    return IntWritable.class;
  }
}
