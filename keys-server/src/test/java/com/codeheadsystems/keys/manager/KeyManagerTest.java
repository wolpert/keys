package com.codeheadsystems.keys.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;

import com.codeheadsystems.keys.model.RawKey;
import java.security.SecureRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KeyManagerTest {

  @Mock private SecureRandom secureRandom;
  @Captor private ArgumentCaptor<byte[]> byteCaptor;

  private KeyManager keyManager;

  @BeforeEach
  void setUp() {
    keyManager = new KeyManager(secureRandom);
  }

  @Test
  void generateRawKey_oneByte() {
    final RawKey result = keyManager.generateRawKey(8);
    assertThat(result).isNotNull();
    verify(secureRandom).nextBytes(byteCaptor.capture());
    assertThat(byteCaptor.getValue()).hasSize(1);
  }

  @Test
  void generateRawKey_halfBytes() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> keyManager.generateRawKey(4));
  }

}