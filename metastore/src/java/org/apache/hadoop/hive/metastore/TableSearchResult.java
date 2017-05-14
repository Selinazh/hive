package org.apache.hadoop.hive.metastore;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * This describes the results from each search category for Tables.
 */
public class TableSearchResult {

  public static final String TABLE_NAME_MATCH = "Table name match";
  public static final String TABLE_COMMENTS_MATCH = "Table comments match";
  public static final String PARTITION_NAME_MATCH = "Partition name match";
  public static final String PARTITION_COMMENTS_MATCH = "Partition comments match";
  public static final String COLUMN_NAME_MATCH = "Column name match";
  public static final String COLUMN_COMMENTS_MATCH = "Column comments match";
  public static final String OWNER_MATCH = "Owner match";
  public static final String LOCATION_MATCH = "Location match";
  public static final String TABLE_PROPERTY_MATCH = "Table properties match";

  // The property used to represent tags in search results
  public static final String HIVE_METASTORE_SEARCH_TAGS = "hive.metastore.search.tags";

  private String database;
  private String table;
  private List<String> tags;

  public TableSearchResult(String db, String tbl, String tag) {
    this.database = db;
    this.table = tbl;
    this.tags = new ArrayList<String>();
    this.tags.add(tag);
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getTable() {
    return table;
  }

  public List<String> getTags() {
    return tags;
  }

  public String getTagsAsString() {
    return StringUtils.join(tags, ",");
  }

  public void addTag(String tag) {
    if (!tags.contains(tag)) {
      tags.add(tag);
    }
  }

  @Override
  public boolean equals(Object other) {

    if (this == other) {
      return true;
    }

    boolean result = false;
    if (other instanceof TableSearchResult) {
      TableSearchResult that = (TableSearchResult) other;
      result = (this.database.equals(that.database) && this.table.equals(that.table));
    }
    return result;
  }

  @Override
  public int hashCode() {
    final int prime = 11;
    int result = 1;
    result = prime * result + database.hashCode();
    result = prime * result + table.hashCode();
    return result;
  }
}
