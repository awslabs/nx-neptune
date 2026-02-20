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

import com.amazonaws.athena.connector.lambda.connection.EnvironmentProperties;
import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;

/**
 * Composite handler for the S3 Vector connector that allows us to use a single
 * Lambda function for both Metadata and Data operations. This handler composes
 * S3VectorMetadataHandler, S3VectorRecordHandler, and
 * S3VectorUserDefinedFuncHandler to provide complete S3 vector data access
 * functionality.
 */
public class S3VectorCompositeHandler extends CompositeHandler {
  /** Constructor for S3VectorCompositeHandler. */
  public S3VectorCompositeHandler() {
    super(
        new S3VectorMetadataHandler(new EnvironmentProperties().createEnvironment()),
        new S3VectorRecordHandler(new EnvironmentProperties().createEnvironment()));
  }
}