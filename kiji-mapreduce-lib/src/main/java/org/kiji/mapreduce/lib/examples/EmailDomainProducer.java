// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.examples;

import java.io.IOException;

import org.kiji.schema.KijiDataRequest;
import org.kiji.schema.KijiRowData;

import org.kiji.mapreduce.KijiProducer;

/**
 * Extracts the domain of a user by reading their email address from the <i>info:email</i> column.
 *
 * <p>The extracted email domain is written to the <i>derived:domain</i> column.</p>
 *
 * <p>To run this from the command line:</p>
 *
 * <pre>
 * $ $WIBI_HOME/bin/wibi produce \
 * &gt;   --input=wibi:tablename \
 * &gt;   --producer=com.wibidata.core.client.lib.examples.EmailDomainProducer
 * &gt;   --output=wibi \
 * </pre>
 */
public class EmailDomainProducer extends KijiProducer {
  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    // We only need to read the most recent email address field from the user's row.
    return new KijiDataRequest()
        .addColumn(new KijiDataRequest.Column("info", "email")
            .withMaxVersions(1));
  }

  /** {@inheritDoc} */
  @Override
  public String getOutputColumn() {
    return "derived:domain";
  }

  /** {@inheritDoc} */
  @Override
  public void produce(KijiRowData input, Context context)
      throws IOException, InterruptedException {
    if (!input.containsColumn("info", "email")) {
      // This user doesn't have an email address.
      return;
    }
    String email = input.getStringValue("info", "email").toString();
    int atSymbol = email.indexOf("@");
    if (atSymbol < 0) {
      // Couldn't find the '@' in the email address. Give up.
      return;
    }
    String domain = email.substring(atSymbol + 1);
    context.write(domain);
  }
}
