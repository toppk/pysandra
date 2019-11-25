

=== Status

==== wireprotocol

  1. Overview - done
  2. Frame header - mostly
    2.1. version - done (see notes below)
    2.2. flags - partial
    2.3. stream - done (see notes below)
    2.4. opcode - done
    2.5. length - done
  3. Notations - mostly (decoders and encoders)
  4. Messages - partial
    4.1. Requests - parial
      4.1.1. STARTUP - done
      4.1.2. AUTH_RESPONSE - not done
      4.1.3. OPTIONS - done (see notes below)
      4.1.4. QUERY - mostly
      4.1.5. PREPARE - mostly
      4.1.6. EXECUTE - mostly
      4.1.7. BATCH - not done
      4.1.8. REGISTER - partial
    4.2. Responses - partial
      4.2.1. ERROR - partial
      4.2.2. READY - done
      4.2.3. AUTHENTICATE - not done
      4.2.4. SUPPORTED - done (see notes below)
      4.2.5. RESULT - partial 
        4.2.5.1. Void - done
        4.2.5.2. Rows - done
        4.2.5.3. Set_keyspace - done
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

==== cql scope support
http://cassandra.apache.org/doc/latest/cql/index.html
vs

https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile


    Definitions
        Conventions
        Identifiers and keywords
        Constants
        Terms
        Comments
        Statements
        Prepared Statements
    Data Types
        Native Types
        Working with timestamps
        Working with dates
        Working with times
        Working with durations
        Collections
        User-Defined Types
        Tuples
        Custom Types
    Data Definition
        Common definitions
        CREATE KEYSPACE
        USE
        ALTER KEYSPACE
        DROP KEYSPACE
        CREATE TABLE
        ALTER TABLE
        DROP TABLE
        TRUNCATE
    Data Manipulation
        SELECT
        INSERT
        UPDATE
        DELETE
        BATCH
    Secondary Indexes
        CREATE INDEX
        DROP INDEX
    Materialized Views
        CREATE MATERIALIZED VIEW
        ALTER MATERIALIZED VIEW
        DROP MATERIALIZED VIEW
    Security
        Database Roles
        Users
        Data Control
    Functions
        Scalar functions
        Aggregate functions
    Arithmetic Operators
        Number Arithmetic
        Datetime Arithmetic
    JSON Support
        SELECT JSON
        INSERT JSON
        JSON Encoding of Cassandra Data Types
        The fromJson() Function
        The toJson() Function
    Triggers
        CREATE TRIGGER
        DROP TRIGGER
    Appendices
        Appendix A: CQL Keywords
        Appendix B: CQL Reserved Types
        Appendix C: Dropping Compact Storage
    Changes
        3.4.5
        3.4.4
        3.4.3
        3.4.2
        3.4.1
        3.4.0
        3.3.1
        3.3.0
        3.2.0
        3.1.7
        3.1.6
        3.1.5
        3.1.4
        3.1.3
        3.1.2
        3.1.1
        3.1.0
        3.0.5
        3.0.4
        3.0.3
        3.0.2
        3.0.1
        Versioning



=== Here are some thoughts on the protocol specification (currently using v4)

stream (sec 2.3) is listed as [short] which is a 2 byte unsigned value (sec 3.) , but has a maximum value of
2 ** 15 (sec 2.3) and can contain a flag value of -1 (sec 2.3 & sec 4.2.6)

How to remove registration

What is the license for the protocol documents?

if options returns supoported method options={'PROTOCOL_VERSIONS': ['3/v3', '4/v4', '5/v5-beta'], 'COMPRESSION': ['snappy', 'lz4'], 'CQL_VERSION': ['3.4.4']}
What is the correct CQL_VERSION (sec. 4.1.1).  If you send options first, what protocol_version should you used (sec 2.1)   THROW_ON_OVERLOAD  NO_COMPACT CQL_VERSION

(sec 5)     # Cassandra writes the uncompressed message length in big endian order,
            # but the lz4 lib requires little endian order, so we wrap these
            # functions to handle that


# READY is compressed???

* should I do execute with/o values?

* extra data doesn't generate error messg

* for fun try sending QueryFlags.VALUES with n ==0 both in execute and query

* bad data in bind vs direct has differetn error strings

* speeling consitency