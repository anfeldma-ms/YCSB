/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License. See accompanying LICENSE file.
 */

// Authors: Anthony F. Voellm and Khoa Dang

package site.ycsb.db;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosClient;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosContainer;
import com.azure.cosmos.CosmosDatabase;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.DirectConnectionConfig;
import com.azure.cosmos.GatewayConnectionConfig;
import com.azure.cosmos.ThrottlingRetryOptions;
import com.azure.cosmos.models.CosmosItemRequestOptions;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.SqlParameter;
import com.azure.cosmos.models.SqlQuerySpec;
import com.azure.cosmos.util.CosmosPagedIterable;
import com.fasterxml.jackson.databind.JsonNode;
// import com.fasterxml.jackson.core.JsonProcessingException;
// import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
// import site.ycsb.StringByteIterator;

/**
 * Azure Cosmos DB v1.00 client for YCSB.
 */

public class AzureCosmosClient extends DB {

  protected static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // Default configuration values
  private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.SESSION;
  private static final String DEFAULT_DATABASE_NAME = "ycsb";
  private static final boolean DEFAULT_USE_GATEWAY = false;
  private static final boolean DEFAULT_USE_UPSERT = false;
  private static final int DEFAULT_MAX_DEGREE_OF_PARALLELISM = 0;
  private static final int DEFAULT_MAX_BUFFERED_ITEM_COUNT = 0;
  private static final boolean DEFAULT_INCLUDE_EXCEPTION_STACK_IN_LOG = false;
  private static final String DEFAULT_USER_AGENT = "ycsb-4.0.1";

  private static final Logger LOGGER = LoggerFactory
      .getLogger(AzureCosmosClient.class);

  /**
   * Count the number of times initialized to teardown on the last
   * {@link #cleanup()}.
   */
  private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

  private static CosmosClient client;
  private static CosmosDatabase database;
  private String databaseName;
  private boolean useUpsert;
  private int maxDegreeOfParallelism;
  private int maxBufferedItemCount;
  private boolean includeExceptionStackInLog;
  private Map<String, CosmosContainer> containerCache;
  private String userAgent;

  @Override
  public synchronized void init() throws DBException {
    INIT_COUNT.incrementAndGet();
    if (client != null) {
      return;
    }
    // org.apache.log4j.Logger.getLogger("io.netty")
    // .setLevel(org.apache.log4j.Level.OFF);
    initAzureCosmosClient();
  }

  private void initAzureCosmosClient() throws DBException {

    // Connection properties
    String primaryKey = this.getStringProperty("azurecosmos.primaryKey", null);
    if (primaryKey == null || primaryKey.isEmpty()) {
      throw new DBException(
          "Missing primary key required to connect to the database.");
    }

    String uri = this.getStringProperty("azurecosmos.uri", null);
    if (primaryKey == null || primaryKey.isEmpty()) {
      throw new DBException("Missing uri required to connect to the database.");
    }

    this.userAgent = this.getStringProperty("azurecosmos.userAgent",
        DEFAULT_USER_AGENT);

    this.useUpsert = this.getBooleanProperty("azurecosmos.useUpsert",
        DEFAULT_USE_UPSERT);

    this.databaseName = this.getStringProperty("azurecosmos.databaseName",
        DEFAULT_DATABASE_NAME);

    this.maxDegreeOfParallelism = this.getIntProperty(
        "azurecosmos.maxDegreeOfParallelism",
        DEFAULT_MAX_DEGREE_OF_PARALLELISM);

    this.maxBufferedItemCount = this.getIntProperty(
        "azurecosmos.maxBufferedItemCount", DEFAULT_MAX_BUFFERED_ITEM_COUNT);

    this.includeExceptionStackInLog = this.getBooleanProperty(
        "azurecosmos.includeExceptionStackInLog",
        DEFAULT_INCLUDE_EXCEPTION_STACK_IN_LOG);

    ConsistencyLevel consistencyLevel = ConsistencyLevel
        .valueOf(this.getStringProperty("azurecosmos.consistencyLevel",
            DEFAULT_CONSISTENCY_LEVEL.toString().toUpperCase()));
    boolean useGateway = this.getBooleanProperty("azurecosmos.useGateway",
        DEFAULT_USE_GATEWAY);

    ThrottlingRetryOptions retryOptions = new ThrottlingRetryOptions();
    retryOptions.setMaxRetryAttemptsOnThrottledRequests(
        this.getIntProperty("azurecosmos.maxRetryAttemptsOnThrottledRequests",
            retryOptions.getMaxRetryAttemptsOnThrottledRequests()));
    retryOptions.setMaxRetryWaitTime(Duration
        .ofSeconds(this.getIntProperty("azurecosmos.maxRetryWaitTimeInSeconds",
            Math.toIntExact(retryOptions.getMaxRetryWaitTime().toSeconds()))));

    DirectConnectionConfig directConnectionConfig = new DirectConnectionConfig();
    directConnectionConfig.setMaxConnectionsPerEndpoint(
        this.getIntProperty("azurecosmos.maxConnectionPoolSize",
            directConnectionConfig.getMaxConnectionsPerEndpoint()));
    directConnectionConfig.setIdleConnectionTimeout(Duration.ofSeconds(this
        .getIntProperty("azurecosmos.idleConnectionTimeout", Math.toIntExact(
            directConnectionConfig.getIdleConnectionTimeout().toSeconds()))));
    directConnectionConfig.setMaxRequestsPerConnection(10000);

    GatewayConnectionConfig gatewayConnectionConfig = new GatewayConnectionConfig();
    gatewayConnectionConfig.setMaxConnectionPoolSize(
        this.getIntProperty("azurecosmos.maxConnectionPoolSize",
            gatewayConnectionConfig.getMaxConnectionPoolSize()));
    gatewayConnectionConfig.setIdleConnectionTimeout(Duration.ofSeconds(this
        .getIntProperty("azurecosmos.idleConnectionTimeout", Math.toIntExact(
            gatewayConnectionConfig.getIdleConnectionTimeout().toSeconds()))));

    try {
      LOGGER.info(
          "Creating Cosmos DB client {}, useGateway={}, consistencyLevel={},"
              + " maxRetryAttemptsOnThrottledRequests={}, maxRetryWaitTimeInSeconds={}"
              + " useUpsert={}, maxDegreeOfParallelism={}, maxBufferedItemCount={}",
          uri, useGateway, consistencyLevel.toString(),
          retryOptions.getMaxRetryAttemptsOnThrottledRequests(),
          retryOptions.getMaxRetryWaitTime().toSeconds(), this.useUpsert,
          this.maxDegreeOfParallelism, this.maxBufferedItemCount);

      // This is creating the actual client.
      // I have tested this out also by completely removing all the options
      // except for endpoint, key, and consistency level, to see if the defaults
      // were fast, but it didn't change the 8000 ms issue.
      CosmosClientBuilder builder = new CosmosClientBuilder().endpoint(uri)
          .key(primaryKey).throttlingRetryOptions(retryOptions)
          .endpointDiscoveryEnabled(false).consistencyLevel(consistencyLevel)
          .userAgentSuffix(userAgent);

      // This is how we specify between gateway and direct in V4.
      if (useGateway) {
        builder = builder.gatewayMode(gatewayConnectionConfig);
      } else {
        builder = builder.directMode(directConnectionConfig);
      }

      AzureCosmosClient.client = builder.buildClient();
      LOGGER.info("Azure Cosmos DB connection created to {}", uri);
    } catch (IllegalArgumentException e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      throw new DBException(
          "Illegal argument passed in. Check the format of your parameters.",
          e);
    }

    this.containerCache = new HashMap<>();

    // Verify the database exists
    try {
      AzureCosmosClient.database = AzureCosmosClient.client
          .getDatabase(databaseName);
      AzureCosmosClient.database.read();
    } catch (CosmosException e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      throw new DBException("Invalid database name (" + this.databaseName
          + ") or failed to read database.", e);
    }
  }

  private String getStringProperty(String propertyName, String defaultValue) {
    return getProperties().getProperty(propertyName, defaultValue);
  }

  private boolean getBooleanProperty(String propertyName,
      boolean defaultValue) {
    String stringVal = getProperties().getProperty(propertyName, null);
    if (stringVal == null) {
      return defaultValue;
    }
    return Boolean.parseBoolean(stringVal);
  }

  private int getIntProperty(String propertyName, int defaultValue) {
    String stringVal = getProperties().getProperty(propertyName, null);
    if (stringVal == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(stringVal);
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    if (INIT_COUNT.decrementAndGet() == 0) {
      try {
        AzureCosmosClient.client.close();
      } catch (Exception e) {
        if (!this.includeExceptionStackInLog) {
          e = null;
        }
        LOGGER.error("Could not close DocumentClient", e);
      } finally {
        client = null;
      }
    }
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {

    // This is the read method
    try {
      CosmosContainer container = this.containerCache.get(key);
      if (container == null) {
        container = AzureCosmosClient.database.getContainer(table);
        this.containerCache.put(table, container);
      }

      CosmosItemResponse<ObjectNode> response = container.readItem(key,
          new PartitionKey(key), ObjectNode.class);
      LOGGER.info("end-to-end request latency in ms: "
          + response.getDuration().toMillis());

      ObjectNode node = response.getItem();
      Map<String, String> stringResults = new HashMap<>();
      if (fields == null) {
        Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
        while (iter.hasNext()) {
          Entry<String, JsonNode> pair = iter.next();
          stringResults.put(pair.getKey().toString(),
              pair.getValue().toString());
        }
        StringByteIterator.putAllAsByteIterators(result, stringResults);
      } else {
        Iterator<Map.Entry<String, JsonNode>> iter = node.fields();
        while (iter.hasNext()) {
          Entry<String, JsonNode> pair = iter.next();
          if (fields.contains(pair.getKey())) {
            stringResults.put(pair.getKey().toString(),
                pair.getValue().toString());
          }
        }
        StringByteIterator.putAllAsByteIterators(result, stringResults);
        return Status.OK;
      }

    } catch (CosmosException e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error("Failed to read key {} in collection {} in database {}", key,
          table, this.databaseName, e);
    }
    return Status.OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value
   *        pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    CosmosQueryRequestOptions queryOptions = new CosmosQueryRequestOptions();
    queryOptions.setMaxDegreeOfParallelism(this.maxDegreeOfParallelism);
    queryOptions.setMaxBufferedItemCount(this.maxBufferedItemCount);

    CosmosContainer container = database.getContainer(table);

    List<SqlParameter> paramList = new ArrayList<>();
    paramList.add(new SqlParameter("@startkey", startkey));

    SqlQuerySpec querySpec = new SqlQuerySpec(
        this.createSelectTop(fields, recordcount)
            + " FROM root r WHERE r.id >= @startkey",
        paramList);
    CosmosPagedIterable<ObjectNode> pagedIterable = container
        .queryItems(querySpec, queryOptions, ObjectNode.class);
    Iterator<ObjectNode> queryIterator = pagedIterable.iterator();
    while (queryIterator.hasNext()) {
      Map<String, String> stringResults = new HashMap<>();
      ObjectNode node = queryIterator.next();
      Iterator<Map.Entry<String, JsonNode>> nodeIterator = node.fields();
      while (nodeIterator.hasNext()) {
        Entry<String, JsonNode> pair = nodeIterator.next();
        stringResults.put(pair.getKey().toString(), pair.getValue().toString());
      }
      HashMap<String, ByteIterator> byteResults = new HashMap<>();
      StringByteIterator.putAllAsByteIterators(byteResults, stringResults);

      result.add(byteResults);
    }

    /*
     * List<Document> documents; FeedResponse<Document> feedResponse = null; try
     * { FeedOptions feedOptions = new FeedOptions();
     * feedOptions.setEnableCrossPartitionQuery(true); feedOptions
     * .setMaxDegreeOfParallelism(this.maxDegreeOfParallelismForQuery);
     * feedResponse = AzureCosmosClient.client.queryDocuments(
     * getDocumentCollectionLink(this.databaseName, table), new SqlQuerySpec(
     * "SELECT TOP @recordcount * FROM root r WHERE r.id >= @startkey", new
     * SqlParameterCollection( new SqlParameter("@recordcount", recordcount),
     * new SqlParameter("@startkey", startkey))), feedOptions); documents =
     * feedResponse.getQueryIterable().toList(); } catch (Exception e) { if
     * (!this.includeExceptionStackInLog) { e = null; }
     * LOGGER.error("Failed to scan with startKey={}, recordCount={}", startkey,
     * recordcount, e); return Status.ERROR; }
     * 
     * if (documents != null) { for (Document document : documents) {
     * result.add(this.extractResult(document)); } }
     */

    return Status.OK;
  }

  /**
   * Update a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key, overwriting any existing values with the same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {

    try {

      // read first

      CosmosContainer container = database.getContainer(table);

      // Test if this needs a null check
      CosmosItemResponse<ObjectNode> response = container.readItem(key,
          new PartitionKey(key), ObjectNode.class);
      ObjectNode node = response.getItem();

      for (Entry<String, ByteIterator> pair : values.entrySet()) {
        node.put(pair.getKey(), pair.getValue().toString());
      }

      // refactor this
      PartitionKey pk = new PartitionKey(key);
      container.replaceItem(node, key, pk, new CosmosItemRequestOptions());

      return Status.OK;
    } catch (CosmosException e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error("Failed to update key {} to collection {} in database {}",
          key, table, this.databaseName, e);

      // Azure Cosmos does not have patch support. Until then we need to read
      // the document, update in place, and then write back.
      // This could actually be made more efficient by using a stored procedure
      // and doing the read/modify write on the server side. Perhaps
      // that will be a future improvement.

      /*
       * String documentLink = getDocumentLink(this.databaseName, table, key);
       * ResourceResponse<Document> updatedResource = null;
       * ResourceResponse<Document> readResouce = null; RequestOptions
       * reqOptions = null; Document document = null;
       * 
       * try { reqOptions = getRequestOptions(key); readResouce =
       * AzureCosmosClient.client.readDocument(documentLink, reqOptions);
       * document = readResouce.getResource(); } catch (DocumentClientException
       * e) { if (!this.includeExceptionStackInLog) { e = null; } LOGGER.error(
       * "Failed to read key {} in collection {} in database {} during update operation"
       * , key, table, this.databaseName, e); return Status.ERROR; }
       * 
       * // Update values for (Entry<String, ByteIterator> entry :
       * values.entrySet()) { document.set(entry.getKey(),
       * entry.getValue().toString()); }
       * 
       * AccessCondition accessCondition = new AccessCondition();
       * accessCondition.setCondition(document.getETag());
       * accessCondition.setType(AccessConditionType.IfMatch);
       * reqOptions.setAccessCondition(accessCondition);
       * 
       * try { updatedResource =
       * AzureCosmosClient.client.replaceDocument(documentLink, document,
       * reqOptions); } catch (DocumentClientException e) { if
       * (!this.includeExceptionStackInLog) { e = null; }
       * LOGGER.error("Failed to update key {}", key, e); return Status.ERROR; }
       */

    }
    return Status.ERROR;
  }

  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Insert key: " + key + " into table: " + table);
    }

    try {
      CosmosContainer container = AzureCosmosClient.database
          .getContainer(table);
      PartitionKey pk = new PartitionKey(key);
      ObjectNode node = OBJECT_MAPPER.createObjectNode();

      node.put("id", key);
      for (Map.Entry<String, ByteIterator> pair : values.entrySet()) {
        node.put(pair.getKey(), pair.getValue().toString());
      }
      if (this.useUpsert) {
        container.upsertItem(node);
      } else {
        container.createItem(node, pk, new CosmosItemRequestOptions());
      }
      return Status.OK;
    } catch (CosmosException e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error("Failed to insert key {} to collection {} in database {}",
          key, table, this.databaseName, e);
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Delete key: " + key + " from table: " + table);
    }
    try {
      CosmosContainer container = AzureCosmosClient.database
          .getContainer(table);
      container.deleteItem(key, new PartitionKey(key),
          new CosmosItemRequestOptions());

      return Status.OK;
    } catch (Exception e) {
      if (!this.includeExceptionStackInLog) {
        e = null;
      }
      LOGGER.error("Failed to delete key: {} in collection: {}", key, table, e);
    }
    return Status.ERROR;
  }

  private String createSelect(Set<String> fields) {
    if (fields == null) {
      return "SELECT * ";
    } else {
      StringBuilder result = new StringBuilder("SELECT ");
      for (String field : fields) {
        result.append(field).append(", ");
      }
      return result.toString();
    }
  }

  private String createSelectTop(Set<String> fields, int top) {
    if (fields == null) {
      return "SELECT TOP " + top + " * ";
    } else {
      StringBuilder result = new StringBuilder("SELECT TOP ").append(top)
          .append(" ");
      int initLength = result.length();
      for (String field : fields) {
        if (result.length() != initLength) {
          result.append(", ");
        }
        result.append("r['").append(field).append("'] ");
      }
      return result.toString();
    }
  }
}