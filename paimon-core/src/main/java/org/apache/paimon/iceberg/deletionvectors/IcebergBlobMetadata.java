/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.iceberg.deletionvectors;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/** doc. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergBlobMetadata {

    private static final String TYPE = "type";
    private static final String FIELDS = "fields";
    private static final String SNAPSHOT_ID = "snapshot-id";
    private static final String SEQUENCE_NUMBER = "sequence-number";
    private static final String OFFSET = "offset";
    private static final String LENGTH = "length";
    private static final String COMPRESSION_CODEC = "compression-codec";
    private static final String PROPERTIES = "properties";

    @JsonProperty(TYPE)
    private final String type;

    @JsonProperty(FIELDS)
    private final List<Integer> inputFields;

    @JsonProperty(SNAPSHOT_ID)
    private final long snapshotId;

    @JsonProperty(SEQUENCE_NUMBER)
    private final long sequenceNumber;

    @JsonProperty(OFFSET)
    private final long offset;

    @JsonProperty(LENGTH)
    private final long length;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(COMPRESSION_CODEC)
    private final String compressionCodec;

    @JsonProperty(PROPERTIES)
    private final Map<String, String> properties;

    @JsonCreator
    public IcebergBlobMetadata(
            @JsonProperty(TYPE) String type,
            @JsonProperty(FIELDS) List<Integer> inputFields,
            @JsonProperty(SNAPSHOT_ID) long snapshotId,
            @JsonProperty(SEQUENCE_NUMBER) long sequenceNumber,
            @JsonProperty(OFFSET) long offset,
            @JsonProperty(LENGTH) long length,
            @JsonProperty(COMPRESSION_CODEC) @Nullable String compressionCodec,
            @JsonProperty(PROPERTIES) Map<String, String> properties) {
        this.type = type;
        this.inputFields = inputFields;
        this.snapshotId = snapshotId;
        this.sequenceNumber = sequenceNumber;
        this.offset = offset;
        this.length = length;
        this.compressionCodec = compressionCodec;
        this.properties = properties;
    }

    @JsonGetter(TYPE)
    public String type() {
        return type;
    }

    @JsonGetter(FIELDS)
    public List<Integer> inputFields() {
        return inputFields;
    }

    @JsonGetter(SNAPSHOT_ID)
    public long snapshotId() {
        return snapshotId;
    }

    @JsonGetter(SEQUENCE_NUMBER)
    public long sequenceNumber() {
        return sequenceNumber;
    }

    @JsonGetter(OFFSET)
    public long offset() {
        return offset;
    }

    @JsonGetter(LENGTH)
    public long length() {
        return length;
    }

    @JsonGetter(COMPRESSION_CODEC)
    public String compressionCodec() {
        return compressionCodec;
    }

    @JsonGetter(PROPERTIES)
    public Map<String, String> properties() {
        return properties;
    }
}
