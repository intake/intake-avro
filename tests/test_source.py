
import os
import pandas as pd

from intake_avro import (AvroTableSource, TablePlugin, AvroSequenceSource,
                         SequencePlugin)

here = os.path.dirname(__file__)
path = os.path.join(here, 'twitter.avro')
data = [
    {"username": "miguno", "tweet": "Rock: Nerf paper, scissors is fine.",
        "timestamp": 1366150681},
    {"username": "BlizzardCS", "tweet": "Works as intended.  Terran is IMBA.",
        "timestamp": 1366154481}
]
expected = pd.DataFrame(data)


def test_plugin():
    p = TablePlugin()
    s = p.open(path)
    out = s.read()
    assert out.equals(expected)
