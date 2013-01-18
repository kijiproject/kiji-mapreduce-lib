// (c) Copyright 2011 WibiData, Inc.

package org.kiji.mapreduce.lib.examples;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.kiji.schema.EntityId;

import org.kiji.mapreduce.KijiBulkImporter;

/**
 * <p>A bulk importer that takes qualified_path/raw_article key/value pairs,
 * and loads these into wibi.  The article name is specified by the
 * parent folder and name of this article (This is to guarantee unique names).
 * The classification is specified by the parent
 * folder to this article.  The raw article to store is passed in as the value.</p>
 *
 * <p>For example, the calling the produce() method with key:value of<p>
 * <code>"some/path/sci.med/12345":"This is the article text"</code>
 * <p>will generate a single row in wibi with fields:<p>
 * <ul>
 *   <li>name: "sci.med.12345"</li>
 *   <li>category: "sci.med"</li>
 *   <li>raw_article: "This is the article text"</li>
 * </ul>
 */
public class News20BulkImporter extends KijiBulkImporter<Text, Text> {
  /** The family to write input data to. */
  public static final String FAMILY = "info";

  /** Qualifier storing the article name. */
  public static final String ARTICLE_NAME_QUALIFIER = "name";
  /** Qualifier storing the article category. */
  public static final String CATEGORY_QUALIFIER = "category";
  /** Qualifier storing the raw text of an article. */
  public static final String RAW_ARTICLE_QUALIFIER = "raw_article";

  /**
   * <p>Returns the family "info" so that we can write to
   * info:name, info:category, and info:raw_text.</p>
   *
   * {@inheritDoc}
   */
  @Override
  public String getOutputColumn() throws IOException {
    return FAMILY;
  }

  /**
   * Reads a single news article, and writes its contents to a new wibi row,
   * indexed by the article's name (A string consisting of the parent folder, and
   * this article's hash), and the a priori categorization of this article.
   *
   * @param key The fully qualified path to the current file we're reading.
   * @param value The raw data to insert into this column.
   * @param context The context to write to.
   * @throws IOException if there is an error.
   * @throws InterruptedException if there is an error.
   */
  @Override
  public void produce(Text key, Text value, Context context)
      throws IOException, InterruptedException {
    Path qualifiedPath = new Path(key.toString());

    // Category is specified on the containing folder.
    String category = qualifiedPath.getParent().getName();
    // Name is the concatenation of category and file name.
    String name = category + "." + qualifiedPath.getName();

    // write name, category, and raw article.
    EntityId entity = context.getEntityId(name);
    context.write(entity, ARTICLE_NAME_QUALIFIER, name);
    context.write(entity, CATEGORY_QUALIFIER, category);
    context.write(entity, RAW_ARTICLE_QUALIFIER, value.toString());
  }
}
