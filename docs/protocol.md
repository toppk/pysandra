

Status

  1. Overview - done
  2. Frame header - mostly
    2.1. version - done
    2.2. flags - partial
    2.3. stream - done (see notes below)
    2.4. opcode - done
    2.5. length - done
  3. Notations - mostly (decoders and encoders)
  4. Messages - partial
    4.1. Requests - parial
      4.1.1. STARTUP - done
      4.1.2. AUTH_RESPONSE - not done
      4.1.3. OPTIONS - not done
      4.1.4. QUERY - mostly
      4.1.5. PREPARE - mostly
      4.1.6. EXECUTE - mostly
      4.1.7. BATCH - not done
      4.1.8. REGISTER - partial
    4.2. Responses - partial
      4.2.1. ERROR - partial
      4.2.2. READY - done
      4.2.3. AUTHENTICATE - not done
      4.2.4. SUPPORTED - not done
      4.2.5. RESULT - partial 
        4.2.5.1. Void - done
        4.2.5.2. Rows - done
        4.2.5.3. Set_keyspace - not done
        4.2.5.4. Prepared - done
        4.2.5.5. Schema_change - done
      4.2.6. EVENT - partial
      4.2.7. AUTH_CHALLENGE - not done
      4.2.8. AUTH_SUCCESS - not done
  5. Compression - done
  6. Data Type Serialization Formats - not done
  7. User Defined Type Serialization - not done
  8. Result paging - not done
  9. Error codes - not done
  10. Changes from v3 - ignored



Here are some thoughts on the protocol specification (currently using v4)

stream (sec 2.3) is listed as [short] which is a 2 byte unsigned value (sec 3.) , but has a maximum value of
2 ** 15 (sec 2.3) and can contain a flag value of -1 (sec 2.3 & sec 4.2.6)

How to remove registration