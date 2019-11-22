from pysandra import exceptions


def test_drivererror():
    error = exceptions.DriverError(foo="bad")
    assert error.foo == "bad"
