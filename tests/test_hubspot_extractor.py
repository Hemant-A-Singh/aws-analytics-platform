from extractors.hubspot_extractor import HubspotExtractor

def test_build_payload_no_filter():
    extractor = HubspotExtractor()
    payload = extractor._build_payload(last_extracted_at=None, after= None)

    assert "filterGroups" not in payload
    assert payload['limit'] == 100