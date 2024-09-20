package com.codeheadsystems.keys.utilities;

import java.util.Optional;
import java.util.function.Function;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyUtilities {

  public static final Function<byte[], String> encode = Hex::encodeHexString;
  private static final Logger LOGGER = LoggerFactory.getLogger(KeyUtilities.class);
  public static final Function<String, Optional<byte[]>> decode = s -> {
    try {
      return Optional.of(Hex.decodeHex(s));
    } catch (Exception e) {
      LOGGER.error("Failed to decode hex string: {}", s, e);
      return Optional.empty();
    }
  };

}
