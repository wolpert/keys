package com.codeheadsystems.keys.converter;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import com.codeheadsystems.api.keys.v1.Key;
import com.codeheadsystems.keys.exception.InvalidKeyException;
import com.codeheadsystems.keys.model.RawKey;
import com.codeheadsystems.keys.utilities.KeyUtilities;
import java.util.UUID;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class KeyConverterTest {

  private static final byte[] KEY = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
  private static final UUID UUID = randomUUID();

  private KeyConverter keyConverter;

  @BeforeEach
  void setUp() {
    keyConverter = new KeyConverter();
  }

  @Test
  void from_to() throws DecoderException {
    final RawKey rawKey = RawKey.of(UUID, KEY);
    final Key key = keyConverter.from(rawKey);
    assertThat(key)
        .isNotNull()
        .hasFieldOrPropertyWithValue("key", "0102030405060708");
    assertThat(Hex.decodeHex(key.key()))
        .isEqualTo(KEY);

    final RawKey result = keyConverter.to(key);
    assertThat(result)
        .isNotNull()
        .hasFieldOrPropertyWithValue("uuid", UUID.toString())
        .hasFieldOrPropertyWithValue("key", "0102030405060708");
  }

  @Test
  void to_invalidKey() {
    final Key key = com.codeheadsystems.api.keys.v1.ImmutableKey.builder().uuid(UUID.toString()).key("invalid").build();
    assertThatExceptionOfType(InvalidKeyException.class)
        .isThrownBy(() -> keyConverter.to(key))
        .withMessage("Invalid key: " + UUID);
  }

}