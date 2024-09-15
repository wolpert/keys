package com.codeheadsystems.keys;

import com.codeheadsystems.keys.component.DaggerKeysServerComponent;
import com.codeheadsystems.server.Server;
import com.codeheadsystems.server.component.DropWizardComponent;
import com.codeheadsystems.server.module.DropWizardModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Keys server.
 */
public class KeysServer extends Server<KeysServerConfiguration> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeysServer.class);

  /**
   * Run the world.
   *
   * @param args from the command line.
   * @throws Exception if we could not start the server.
   */
  public static void main(String[] args) throws Exception {
    LOGGER.info("main({})", (Object) args);
    final KeysServer server = new KeysServer();
    server.run(args);
  }

  @Override
  protected DropWizardComponent dropWizardComponent(final DropWizardModule module) {
    return DaggerKeysServerComponent.builder()
        .dropWizardModule(module)
        .build();
  }

}
