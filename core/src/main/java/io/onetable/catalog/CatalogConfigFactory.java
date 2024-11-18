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
 
package io.onetable.catalog;

import static io.onetable.catalog.glue.GlueCatalogConfig.CLIENT_CREDENTIAL_PROVIDER_PREFIX;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.onetable.catalog.glue.GlueCatalogConfig;
import io.onetable.catalog.hms.HMSCatalogConfig;

/**
 * CatalogConfigFactory defines the configs specific to each catalog and returns the configuration
 * for each catalog. Each config follows the format of externalCatalog.catalogType.propertyName
 */
public class CatalogConfigFactory {
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  ;

  public static GlueCatalogConfig getGlueCatalogConfig(Map<String, String> properties) {
    try {
      GlueCatalogConfig glueCatalogConfig =
          OBJECT_MAPPER.readValue(
              OBJECT_MAPPER.writeValueAsString(properties), GlueCatalogConfig.class);
      Map<String, String> clientCredentialProperties =
          propertiesWithPrefix(properties, CLIENT_CREDENTIAL_PROVIDER_PREFIX);
      glueCatalogConfig.setClientCredentialConfigs(clientCredentialProperties);
      return glueCatalogConfig;
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, String> propertiesWithPrefix(
      Map<String, String> properties, String prefix) {
    if (properties == null || properties.isEmpty()) {
      return Collections.emptyMap();
    }

    return properties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .collect(Collectors.toMap(e -> e.getKey().replaceFirst(prefix, ""), Map.Entry::getValue));
  }

  public static HMSCatalogConfig getHMSCatalogConfig(Map<String, String> properties) {
    try {
      return OBJECT_MAPPER.readValue(
          OBJECT_MAPPER.writeValueAsString(properties), HMSCatalogConfig.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
