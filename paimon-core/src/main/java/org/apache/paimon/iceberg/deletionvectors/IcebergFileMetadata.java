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

import org.apache.paimon.utils.JsonSerdeUtil;
import org.apache.paimon.utils.Preconditions;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonGetter;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/** doc. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class IcebergFileMetadata {

    private static final String BLOBS = "blobs";
    private static final String PROPERTIES = "properties";

    @JsonProperty(BLOBS)
    private final List<IcebergBlobMetadata> blobs;

    @JsonProperty(PROPERTIES)
    private final Map<String, String> properties;

    @JsonCreator
    public IcebergFileMetadata(
            @JsonProperty(BLOBS) List<IcebergBlobMetadata> blobs,
            @JsonProperty(PROPERTIES) Map<String, String> properties) {
        Preconditions.checkNotNull(blobs, "blobs is null");
        Preconditions.checkNotNull(properties, "properties is null");
        this.blobs = ImmutableList.copyOf(blobs);
        this.properties = ImmutableMap.copyOf(properties);
    }

    @JsonGetter(BLOBS)
    public List<IcebergBlobMetadata> blobs() {
        return blobs;
    }

    @JsonGetter(PROPERTIES)
    public Map<String, String> properties() {
        return properties;
    }

    public String toJson() {
        return JsonSerdeUtil.toJson(this);
    }
}
