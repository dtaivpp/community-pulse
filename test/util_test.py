from os.path import exists


def test_matches_filter():
    from util import jsonpath_filters, matches_filter
    """Tests to ensure json path filtering works"""
    test_filters = [
        {
            "filter": {"username": ["dtaivpp"]},
            "matches": True
        },
        {
            "filter": {"address.street": ["123 Fake Street"]},
            "matches": True
        },
        {
            "filter": {"family[*].username": ["remove_me"]},
            "matches": True
        },
        {
            "filter": {"missing_key": ["fake value"]},
            "matches": False
        },
        {
            "filter": {"family.username": ["not filtered"]},
            "matches": False
        }
    ]
    
    test_doc = {
        "username": "dtaivpp", 
        "address": {"street" : "123 Fake Street"},
        "family": [{"username": "test"}, {"username": "remove_me"}]
    }

    for filt in test_filters:
        assert matches_filter(jsonpath_filters(filt["filter"]), test_doc) == filt["matches"]


def test_parse_config():
    pass


def test_backoff():
    from util import backoff
    current = 3
    expected = 9
    assert backoff(current) == expected


def test_backoff_max():
    from util import backoff
    current = 20
    expected = 25
    assert backoff(current, max_backoff=25) == 25
