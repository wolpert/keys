package com.codeheadsystems.api.keys.v1;

import org.immutables.value.Value;

@Value.Immutable
public interface Key {

  byte[] key();

}
