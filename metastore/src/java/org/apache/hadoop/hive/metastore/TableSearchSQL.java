package org.apache.hadoop.hive.metastore;

/**
 * This class is used to represent each query used to search tables.
 */
public class TableSearchSQL {

  private String directSQLBaseQuery;
  private String searchQuery;
  private String expression;
  private String tag;

  public TableSearchSQL(String directSQLBaseQuery,
      String searchQuery, String expression, String tag) {

    this.directSQLBaseQuery = directSQLBaseQuery;
    this.searchQuery = searchQuery;
    this.expression = expression;
    this.tag = tag;
  }

  public String getFinalDirectSQLQuery(String[] keywords) {
    StringBuilder filter   = new StringBuilder();
    for (String keyword : keywords) {
      filter.append("lower(").append(searchQuery.trim()).
          append(") like '%").append(keyword.toLowerCase()).append("%'").
          append(expression);
    }
    filter.setLength(filter.length() - expression.length());
    return directSQLBaseQuery + filter.toString();
  }

  public String getTag() {
    return tag;
  }
}
