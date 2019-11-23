import pytest

from pysandra.dispatcher import Dispatcher
from pysandra.exceptions import MaximumStreamsException


def test_max_streams():
    with pytest.raises(
        MaximumStreamsException, match=r"too many streams last_id=31159 length=32769"
    ):
        client = Dispatcher("blank", "", False, 0)
        move = 0
        while True:
            move += 1
            streamid = client._new_stream_id()
            client._update_stream_id(
                streamid, ("something", "else", "entirely"),
            )
            if (move % 19) == 0:
                client._rm_stream_id(streamid)
