/*
 * Copyright (c) 2023. Ned Wolpert
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.codeheadsystems.pretender;

import com.codeheadsystems.pretender.factory.JdbiFactory;
import com.codeheadsystems.pretender.model.Configuration;
import com.codeheadsystems.pretender.model.ImmutableConfiguration;
import com.codeheadsystems.pretender.model.ImmutableDatabase;
import java.util.UUID;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;

public abstract class BaseJdbiTest {

  protected Jdbi jdbi;

  @BeforeEach
  void setupJdbi() {
    final Configuration configuration = ImmutableConfiguration.builder()
        .database(
            ImmutableDatabase.builder()
                .url("jdbc:hsqldb:mem:" + getClass().getSimpleName() + ":" + UUID.randomUUID())
                .username("SA")
                .password("")
                .build()
        ).build();
    jdbi = new JdbiFactory(configuration).createJdbi();
  }

}
