package com.codeheadsystems.keys.utilities;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KeyUtilitiesTest {

  private static final byte[] KEY = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
  private static final String HEX = "0102030405060708";

  @Test
  void encode() {
    assertThat(KeyUtilities.encode.apply(KEY)).isEqualTo(HEX);
  }

  @Test
  void decode() {
    assertThat(KeyUtilities.decode.apply(HEX))
        .isPresent()
        .contains(KEY);
  }

  @Test
  void decodeError() {
    assertThat(KeyUtilities.decode.apply("error"))
        .isEmpty();
  }

}