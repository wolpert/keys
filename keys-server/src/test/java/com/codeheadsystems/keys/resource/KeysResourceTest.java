package com.codeheadsystems.keys.resource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.codeheadsystems.api.keys.v1.Key;
import com.codeheadsystems.keys.converter.KeyConverter;
import com.codeheadsystems.keys.manager.KeyManager;
import com.codeheadsystems.keys.model.RawKey;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KeysResourceTest {

  @Mock private KeyManager keyManager;
  @Mock private KeyConverter keyConverter;
  @Mock private RawKey rawKey;
  @Mock private Key key;

  @InjectMocks private KeysResource keysResource;

  @Test
  void create() {
    when(keyManager.generateRawKey(256)).thenReturn(rawKey);
    when(keyConverter.from(rawKey)).thenReturn(key);
    assertThat(keysResource.create().getEntity()).isEqualTo(key);
  }

}