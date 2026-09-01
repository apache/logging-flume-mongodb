/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.sink.mongodb;

public class MongoDbSinkConstants {

    public static final String MONGODB_PREFIX = "mongodb.";

    /* Properties */

    /** Mongo connection URI, e.g. mongodb://host1,host2/?replicaSet=rs0 */
    public static final String CONNECTION_URI = MONGODB_PREFIX + "uri";

    /** The database that documents will be written to. */
    public static final String DATABASE_NAME = MONGODB_PREFIX + "database";

    /** The default/fallback collection that documents will be written to. */
    public static final String COLLECTION = MONGODB_PREFIX + "collection";

    /**
     * The name of the Flume event header whose value is used to select the
     * target collection via {@link #COLLECTION_MAP_PREFIX}.
     */
    public static final String COLLECTION_HEADER = MONGODB_PREFIX + "collectionHeader";

    /**
     * Prefix for sub-properties that map a header value to a predefined
     * collection name, e.g. mongodb.collectionMap.foo = fooCollection
     */
    public static final String COLLECTION_MAP_PREFIX = MONGODB_PREFIX + "collectionMap.";

    /**
     * Whether to fall back to the default collection when the header value
     * does not have an entry in the collection map. If false, events with an
     * unmapped header value cause the batch to fail.
     */
    public static final String COLLECTION_MAP_FALLBACK = MONGODB_PREFIX + "collectionMapFallback";

    public static final boolean DEFAULT_COLLECTION_MAP_FALLBACK = true;

    /**
     * Whether the Flume event headers should be included in the resulting
     * Mongo document, under the {@link #HEADERS_FIELD} field name.
     */
    public static final String INCLUDE_HEADERS = MONGODB_PREFIX + "includeHeaders";

    public static final boolean DEFAULT_INCLUDE_HEADERS = false;

    public static final String HEADERS_FIELD = "headers";
    public static final String BODY_FIELD = "message";

    public static final String BATCH_SIZE = "batchSize";
    public static final int DEFAULT_BATCH_SIZE = 100;

    /** Write concern to use, e.g. ACKNOWLEDGED, MAJORITY, JOURNALED, UNACKNOWLEDGED. */
    public static final String WRITE_CONCERN = MONGODB_PREFIX + "writeConcern";
}
