/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package io.onetable.catalog.glue;

import java.util.Map;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import org.apache.hudi.hive.MultiPartKeysValueExtractor;

import com.fasterxml.jackson.annotation.JsonProperty;

@Getter
@EqualsAndHashCode
@ToString
public class GlueCatalogConfig {

  public static final String CLIENT_CREDENTIAL_PROVIDER_PREFIX =
      "externalCatalog.glue.credentials.provider.";

  @JsonProperty("externalCatalog.glue.catalogId")
  private String catalogId;

  @JsonProperty("externalCatalog.glue.region")
  private String region;

  @JsonProperty("externalCatalog.glue.credentialsProviderClass")
  private String clientCredentialsProviderClass;

  @Setter private Map<String, String> clientCredentialConfigs;

  @JsonProperty("externalCatalog.glue.lakeFormationEnabled")
  // TODO: Add lake formation support for Iceberg<>Glue sync
  // [https://app.clickup.com/t/18029943/ENG-16363]
  private boolean lakeFormationEnabled;

  @JsonProperty("externalCatalog.glue.schema_string_length_thresh")
  private int schemaLengthThreshold = 4000;

  @JsonProperty("externalCatalog.glue.partition_extractor_class")
  private String partitionExtractorClass = MultiPartKeysValueExtractor.class.getName();

  @JsonProperty("externalCatalog.glue.partition_extractor_class")
  private int maxPartitionsPerRequest = 1000;
}
