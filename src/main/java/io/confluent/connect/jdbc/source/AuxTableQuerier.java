/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import io.confluent.connect.jdbc.util.JdbcUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * AuxTableQuerier always returns the entire table.
 */
public class AuxTableQuerier extends TableQuerier {
  private static final Logger log = LoggerFactory.getLogger(AuxTableQuerier.class);

  TableQuerier mainQuerier;
  private String column;
  private String relatedColumn;

  private String queryList;

  public AuxTableQuerier(TableQuerier mainQuerier, String query, String column, String relatedColumn, String topic) {
    super(QueryMode.QUERY, query, topic, "");
    this.mainQuerier = mainQuerier;
    this.column = column;
    this.relatedColumn = relatedColumn;
  }

  public void maybeStartQuery(Connection db, List<SourceRecord> results) throws SQLException {
    if (resultSet == null) {
      queryList = createQueryList();
      stmt = getOrCreatePreparedStatement(db);
      resultSet = executeQuery();
      schema = DataConverter.convertSchema(name, resultSet.getMetaData());
      if (topicPrefix == "") {
        results.clear();
        if (true/*size() == mainQuerier.size()*/) {
          while (next()) {
            SourceRecord record = extractJointRecord();
            if (record != null) {
              results.add(record);
            }
          }
        } else {
          // ???
        }
      } else {
        while (next()) {
          results.add(extractRecord());
        }
      }
    }
  }

  @Override
  protected void createPreparedStatement(Connection db) throws SQLException {
    if (queryList == null || queryList.isEmpty()) {
      return;
    }

    String quoteString = JdbcUtils.getIdentifierQuoteString(db);
    StringBuilder builder = new StringBuilder();
    builder.append(query);
    builder.append(" WHERE ");
    builder.append(JdbcUtils.quoteString(column, quoteString));
    builder.append(" IN " + queryList);
    String queryString = builder.toString();
    log.debug("{} prepared SQL query: {}", this, queryString);
    stmt = db.prepareStatement(queryString);
  }

  @Override
  protected ResultSet executeQuery() throws SQLException {
    return stmt.executeQuery();
  }

  @Override
  public SourceRecord extractRecord() throws SQLException {
    Struct record = DataConverter.convertRecord(schema, resultSet);
    // TODO: key from primary key? partition?
    final String topic;
    final Map<String, String> partition;

    partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
            JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
    topic = topicPrefix;

    return new SourceRecord(partition, null, topic, record.schema(), record);
  }

  public SourceRecord extractJointRecord() throws SQLException {
    ResultSet mainResultSet = mainQuerier.resultSet;
    boolean foundMatch = false;
    mainResultSet.beforeFirst();
    while (mainResultSet.next()) {
      if (resultSet.getInt(column) == mainResultSet.getInt(relatedColumn)) {
        foundMatch = true;
        break;
      }
    }
    if (foundMatch) {
      Schema jointSchema = DataConverter.convertJointSchema(mainQuerier.name,
              mainQuerier.resultSet.getMetaData(),
              resultSet.getMetaData(),
              column);
      Struct record = DataConverter.convertJointRecord(jointSchema,
              mainResultSet,
              resultSet,
              column);
      // TODO: key from primary key? partition?
      final String topic;
      final Map<String, String> partition;

      partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
              JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
      topic = mainQuerier.topicPrefix + mainQuerier.name;

      return new SourceRecord(partition, null, topic, record.schema(), record);
    } else {
      return null;
    }

  }

  @Override
  public String toString() {
    return "AuxTableQuerier{" +
           "query='" + query + '\'' +
           ", topicPrefix='" + topicPrefix + '\'' +
           ", column='" + column + '\'' +
           ", relatedColumn='" + relatedColumn + '\'' +
           '}';
  }

  private String createQueryList() throws SQLException {
    if (mainQuerier.size() == 0) {
      return null;
    }

    ResultSet mainResultSet = mainQuerier.resultSet;
    String result;
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    Boolean isFirst = true;
    while (mainResultSet.next()) {
      if (isFirst) {
        isFirst = false;
      } else {
        builder.append(", ");
      }
      builder.append(mainResultSet.getInt(relatedColumn));
    }
    builder.append(")");
    result = builder.toString();
    log.debug("{} prepared query list: {}", this, result);
    return result;
  }

}
