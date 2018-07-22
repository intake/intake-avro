import os
import pandas as pd

from intake_avro.source import AvroTableSource, AvroSequenceSource

here = os.path.dirname(__file__)
path = os.path.join(here, 'twitter.avro')
path2 = os.path.join(here, 'twitter2.avro')
pathstar = os.path.join(here, 'twitter*.avro')
data = [
    {"username": "miguno", "tweet": "Rock: Nerf paper, scissors is fine.",
        "timestamp": 1366150681},
    {"username": "BlizzardCS", "tweet": "Works as intended.  Terran is IMBA.",
        "timestamp": 1366154481}
]
columns = ['username', 'tweet', 'timestamp']
expected = pd.DataFrame(data, columns=columns)


def test_tabular():
    s = AvroTableSource(urlpath=path)
    disc = s.discover()
    assert list(disc['dtype']) == columns
    assert s.container == 'dataframe'
    out = s.read()
    assert out.equals(expected)

    s = AvroTableSource(urlpath=[path, path2])
    out = s.read().reset_index(drop=True)
    assert out.equals(pd.concat([expected, expected], ignore_index=True))

    s = AvroTableSource(urlpath=pathstar)
    out = s.read().reset_index(drop=True)
    assert out.equals(pd.concat([expected, expected], ignore_index=True))


def test_sequence():
    s = AvroSequenceSource(urlpath=path)
    disc = s.discover()
    assert disc['dtype'] is None
    assert s.container == 'python'
    out = s.read()
    assert out == expected.to_dict('records')

    s = AvroSequenceSource(urlpath=[path, path2])
    out = s.read()
    assert out == pd.concat([expected, expected],
                            ignore_index=True).to_dict('records')

    s = AvroSequenceSource(urlpath=pathstar)
    out = s.read()
    assert out == pd.concat([expected, expected],
                            ignore_index=True).to_dict('records')
