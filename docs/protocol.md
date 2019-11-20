

Here are some thoughts on the protocol specification (currently using v4)

stream (sec 2.3) is listed as [short] which is a 2 byte unsigned value (sec 3.) , but has a maximum value of
2 ** 15 (sec 2.3) and can contain a flag value of -1 (sec 2.3 & sec 4.2.6)

How to remove registration