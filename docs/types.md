    CUSTOM = 0x0000
    ASCII = 0x0001
    BIGINT = 0x0002
    BLOB = 0x0003
    BOOLEAN = 0x0004
    COUNTER = 0x0005
    DECIMAL = 0x0006
    DOUBLE = 0x0007
    FLOAT = 0x0008
    INT = 0x0009
    TIMESTAMP = 0x000B
    UUID = 0x000C
    VARCHAR = 0x000D
    VARINT = 0x000E
    TIMEUUID = 0x000F
    INET = 0x0010
    DATE = 0x0011
    TIME = 0x0012
    SMALLINT = 0x0013
    TINYINT = 0x0014
    LIST = 0x0020
    MAP = 0x0021
    SET = 0x0022
    UDT = 0x0030
    TUPLE = 0x0031


https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile

type 	constants supported 	description
ascii 	strings              	ASCII character string
bigint 	integers 	        64-bit signed long
blob 	blobs 	                Arbitrary bytes (no validation)
boolean 	booleans 	true or false
counter 	integers 	Counter column (64-bit signed value). See Counters for details
date 	integers, strings 	A date (with no corresponding time value). See Working with dates below for more information.
decimal 	integers, floats 	Variable-precision decimal
double 	integers        	64-bit IEEE-754 floating point
float 	integers, floats 	32-bit IEEE-754 floating point
inet 	strings           	An IP address. It can be either 4 bytes long (IPv4) or 16 bytes long (IPv6). There is no inet constant, IP address should be inputed as strings
int 	integers 	        32-bit signed int
smallint 	integers 	16-bit signed int
text 	strings 	        UTF8 encoded string
time 	integers, strings 	A time with nanosecond precision. See Working with time below for more information.
timestamp 	integers, strings 	A timestamp. Strings constant are allow to input timestamps as dates, see Working with timestamps below for more information.
timeuuid 	uuids 	        Type 1 UUID. This is generally used as a “conflict-free” timestamp. Also see the functions on Timeuuid
tinyint 	integers 	8-bit signed int
uuid 	uuids           	Type 1 or type 4 UUID
varchar 	strings 	UTF8 encoded string
varint 	integers 	        Arbitrary-precision integer

collections - lists  sets tuple map
user defined types