package com.codeheadsystems.keys.exception;

/**
 * The type Invalid key exception.
 */
public class InvalidKeyException extends RuntimeException {

  /**
   * Instantiates a new Invalid key exception.
   *
   * @param uuid the uuid
   */
  public InvalidKeyException(String uuid) {
    super("Invalid key: " + uuid);
  }

}
