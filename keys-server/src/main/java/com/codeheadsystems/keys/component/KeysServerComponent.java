package com.codeheadsystems.keys.component;

import com.codeheadsystems.keys.module.KeysServerModule;
import com.codeheadsystems.server.component.DropWizardComponent;
import com.codeheadsystems.server.module.DropWizardModule;
import dagger.Component;
import javax.inject.Singleton;

/**
 * Creates the pieces needed for the control plane to run. Required.
 */
@Component(modules = {
    DropWizardModule.class,
    KeysServerModule.class,
})
@Singleton
public interface KeysServerComponent extends DropWizardComponent {
}
