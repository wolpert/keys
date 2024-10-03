package com.codeheadsystems.keys.manager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codeheadsystems.keys.dao.RawKeyDao;
import com.codeheadsystems.keys.model.RawKey;
import java.security.SecureRandom;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KeyManagerTest {

  @Mock private SecureRandom secureRandom;
  @Mock private RawKeyDao rawKeyDao;
  @Captor private ArgumentCaptor<byte[]> byteCaptor;
  @Captor private ArgumentCaptor<RawKey> rawKeyCaptor;

  @InjectMocks private  KeyManager keyManager;

  @Test
  void generateRawKey_oneByte() {
    when(rawKeyDao.insert(rawKeyCaptor.capture())).thenReturn(true);
    final RawKey result = keyManager.generateRawKey(8);
    assertThat(result).isNotNull();
    verify(secureRandom).nextBytes(byteCaptor.capture());
    assertThat(byteCaptor.getValue()).hasSize(1);
    assertThat(rawKeyCaptor.getValue()).isEqualTo(result);
  }

  @Test
  void generateRawKey_oneByte_failedToInsert() {
    when(rawKeyDao.insert(rawKeyCaptor.capture())).thenReturn(false);
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> keyManager.generateRawKey(8));
  }

  @Test
  void generateRawKey_oneByte_exceptionOnInsert() {
    when(rawKeyDao.insert(rawKeyCaptor.capture())).thenThrow(new RuntimeException());
    assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> keyManager.generateRawKey(8));
  }

  @Test
  void generateRawKey_halfBytes() {
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> keyManager.generateRawKey(4));
  }

}