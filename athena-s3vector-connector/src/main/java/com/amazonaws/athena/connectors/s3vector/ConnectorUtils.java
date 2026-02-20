/*-
 * #%L
 * athena-s3vector-connector
 * %%
 * Copyright (C) 2026 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

package com.amazonaws.athena.connectors.s3vector;

/** Utility class containing constant column names for S3 Vector connector. */
public class ConnectorUtils {

  public static final String COL_VECTOR_ID = "vector_id";

  public static final String COL_EMBEDDING_DATA = "embedding";

  public static final String COL_METADATA = "metadata";

  private ConnectorUtils() {
    // Utility class
  }
}
