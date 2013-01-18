// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.produce;

import java.io.IOException;

import org.kiji.schema.KijiColumnName;
import org.kiji.schema.KijiDataRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.kiji.mapreduce.KijiContext;
import org.kiji.mapreduce.KijiProducer;

/**
 * Base class for producers that read from the most recent value of a single input column.
 */
public abstract class SingleInputProducer extends KijiProducer {
  private static final Logger LOG = LoggerFactory.getLogger(SingleInputProducer.class);

  private KijiColumnName mInputColumn;

  /**
   * @return the name of the wibi input column to feed to this producer.
   */
  protected abstract String getInputColumn();

  /**
   * Initialize the family and qualifier instance variables.
   */
  private void initializeInputColumn() {
    mInputColumn = new KijiColumnName(getInputColumn());
    if (!mInputColumn.isFullyQualified()) {
      throw new RuntimeException("getInputColumn() must contain a colon (':')");
    }
  }

  /** {@inheritDoc} */
  @Override
  public KijiDataRequest getDataRequest() {
    initializeInputColumn();
    KijiDataRequest dataRequest = new KijiDataRequest();
    dataRequest.addColumn(new KijiDataRequest.Column(mInputColumn));
    return dataRequest;
  }

  /** {@inheritDoc} */
  @Override
  public void setup(KijiContext context) throws IOException {
    initializeInputColumn();
  }

  /** @return the input column family. */
  protected KijiColumnName getInputColumnName() {
    return mInputColumn;
  }
}
