
def enforce_index_templates(os_client):
    """Validate index template is in place"""
    if not os_client.indices.exists_index_template("tweets"):
        body = {
            "index_patterns": [
                "tweets*"
            ],
            "template": {
                "settings": {
                    "analysis": {
                        "analyzer": {
                            "text_analyzer": {
                                "tokenizer": "standard",
                                "filter": [ "stop" ]
                            }
                        }
                    }
                },
                "mappings": {
                    "properties": {
                        "text": {
                            "analyzer": "text_analyzer",
                            "type": "text",
                            "fielddata": True
                        },
                        "created_at": {
                            "type": "date",
                            "format": "yyyy-MM-dd HH:mm:ssZZZZZ"
                        }
                    }
                }
            }
        }
        os_client.indices.put_index_template("tweets", body=body)


def create_meta_indices(os_client):
    if not os_client.indices.exists("markers"):
        os_client.indices.create("markers")