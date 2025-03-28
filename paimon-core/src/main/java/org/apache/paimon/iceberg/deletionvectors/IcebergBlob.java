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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/** doc. */
public class IcebergBlob {
    private final String type;
    private final List<Integer> inputFields;
    private final long snapshotId;
    private final long sequenceNumber;
    private final ByteBuffer blobData;
    private final Map<String, String> properties;

    public IcebergBlob(
            String type,
            List<Integer> inputFields,
            long snapshotId,
            long sequenceNumber,
            ByteBuffer blobData,
            Map<String, String> properties) {
        this.type = type;
        this.inputFields = inputFields;
        this.snapshotId = snapshotId;
        this.sequenceNumber = sequenceNumber;
        this.blobData = blobData;
        this.properties = properties;
    }

    public String type() {
        return type;
    }

    public List<Integer> inputFields() {
        return inputFields;
    }

    public long snapshotId() {
        return snapshotId;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public ByteBuffer blobData() {
        return blobData;
    }

    public Map<String, String> properties() {
        return properties;
    }
}
