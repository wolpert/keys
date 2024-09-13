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

package com.codeheadsystems.keys.server.component;

import com.codeheadsystems.keys.server.initializer.Initializer;
import java.util.Set;

/**
 * Expected drop wizard sets needed for initialization.
 */
public interface DropWizardComponent {

  /**
   * Returns the initializers for this application.
   *
   * @return initializer set.
   */
  Set<Initializer> initializers();

}
