def test_package():
    import pysandra

    assert pysandra.Client == pysandra.client.Client


def test_about():
    import pysandra.__about__

    assert pysandra.__about__.__title__ == "pysandra"
    import pysandra
