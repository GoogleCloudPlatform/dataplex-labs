"""Mocks for api GET requests and header.
"""
import re
import requests


class MockResponse():
  """Mock of an HTTP response used by mocked_get_api_response.

  Attributes:
    status_code: int
    json_data: str
    ok:  bool
    reason: str
  """

  def __init__(self, json_data, status_code):
    self.status_code = status_code
    self.json_data = None if self.status_code in (502, 400) else json_data
    self.ok = self.status_code == 200
    self.reason = "OK" if self.status_code == 200 else "Request error"

  def json(self):
    return self.json_data

  def raise_for_status(self):
    if 400 <= self.status_code < 600:
      raise requests.exceptions.RequestException


class MockThrowingJSONDecodeError(MockResponse):
  def json(self):
    raise requests.exceptions.JSONDecodeError("", "", 0)


def mocked_get_api_response(url, headers=None, json=None):  # pylint: disable=unused-argument

  """Mocking GET requests for the cases.

    1. https://datacatalog.googleapis.com/v2/get_call/success
      returns simple success response.
    2. https://datacatalog.googleapis.com/v2/get_call/error
      returns simple response with error.
    3. https://datacatalog.googleapis.com/v2/get_call/error_with_exception
      return exception.
    4. GetEntry:
      r".+/v2/projects/.+/locations/.+/entryGroups
      /test_entry_group_with_no_glossary/entries/glossary_not_found"
        Glossary entry doesn't exist.
    5. ListEntries:
      r".+/v2/projects/.+/locations/.+/entryGroups
      /test_entry_group_with_no_terms/entries?view=FULL$"
        Glossary entry exists, but there are no terms associated to it.
    6. GetEntry:
      r".+/v2/projects/.+/locations/.+/entryGroups
      /test_entry_group_with_no_terms/entries/empty_glossary_exists"
        Glossary entry exists, but there are no terms associated to it.
    7. ListEntries:
      r".+/v2/projects/.+/locations/.+/entryGroups
      /test_entry_group_with_terms/entries?view=FULL$"
        Glossary entry exists and there are terms associated to it.
    8. GetEntry:
      r".+/v2/projects/123/locations/us/entryGroups
      /test_entry_group_with_terms/entries/glossary_exists
        Glossary entry exists and there are terms associated to it.
    9. ListEntries:
      r".+/v2/projects/.+/locations/.+/entryGroups
      /test_entry_group_with_categories_and_terms/entries?view=FULL$"
        Glossary entry exists and there are categories and terms associated to
        it.
    10. GetEntry:
      r".+/v2/projects/123/locations/us/entryGroups
      /test_entry_group_with_categories_and_terms/entries/glossary_not_empty
        Glossary entry exists and there are categories and terms associated to
        it.
    11. ListEntries:
      r".+/v2/projects/.+/locations/.+/entryGroups
      /test_entry_group_with_categories/entries?view=FULL$"
        Glossary entry exists and there are categories and terms associated to
        it.
    12. GetEntry:
      r".+/v2/projects/123/locations/us/entryGroups
      /test_entry_group_with_categories/entries/glossary_not_empty
        Glossary entry exists and there are categories and terms associated to
        it.

  Args:
    url: str
    headers: Dict
    json: Dict

  Returns:
    MockResponse(dict, status_code)
  """

  if url == "https://datacatalog.googleapis.com/v2/get_call/success":
    return MockResponse({"method": "get", "status": "success"}, 200)
  elif url == "https://datacatalog.googleapis.com/v2/get_call/error":
    error_response = {"message": "error message", "error_code": 400}
    return MockResponse({"error": error_response}, 400)
  elif url == (
      "https://datacatalog.googleapis.com/v2/get_call/error_with_exception"
  ):
    return MockThrowingJSONDecodeError("", 200)
  elif re.fullmatch(
      (
          ".+/v2/projects/.+/locations/.+/"
          "entryGroups/test_entry_group_with_no_glossary/"
          "entries/glossary_not_found"
      ),
      url,
  ):
    return MockResponse(
        {
            "error": {
                "code": 404,
                "message": "Requested entity was not found.",
                "status": "NOT_FOUND",
                "details": [{
                    "@type": "type.googleapis.com/google.rpc.ErrorInfo",
                    "reason": "notFound",
                    "domain": "datacatalog.googleapis.com",
                    "metadata": {"code": "ENTRY_NOT_FOUND"},
                }],
            }
        },
        404,
    )

  # List entry group with no business terms, but glossary entry existing
  elif re.fullmatch(
      (
          ".+/v2/projects/.+/locations/.+/"
          r"entryGroups/test_entry_group_with_no_terms/entries\?view=FULL$"
      ),
      url,
  ):
    return MockResponse(
        {
            "entries": [{
                "name": "dc_glossary_test",
                "displayName": "Glossary 2",
                "entryType": "glossary",
                "coreAspects": {
                    "business_context": {
                        "name": (
                            "projects/123/locations/us/"
                            "entryGroups/test_entry_group_with_no_terms/"
                            "entries/empty_glossary_exists/"
                            "aspects/3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                        ),
                        "aspectType": "business_context",
                        "jsonContent": {
                            "description": (
                                "\u003cp\u003eEmpty glossary\u003c/p\u003e"
                            )
                        },
                        "createTime": "2023-05-11T17:18:00.838415Z",
                        "modifyTime": "2023-05-12T08:32:21.859231Z",
                    }
                },
                "createTime": "2023-04-26T07:30:05.022015Z",
                "modifyTime": "2023-04-26T07:30:05.022015Z",
                "entryUid": "71372af7-bb1a-4020-aba8-223c57c366d2",
            }]
        },
        200,
    )
  # Get glossary entry
  elif re.fullmatch(
      (
          ".+/v2/projects/.+/locations/.+/"
          "entryGroups/test_entry_group_with_no_terms/"
          "entries/empty_glossary_exists"
      ),
      url,
  ):
    return MockResponse(
        {
            "name": "dc_glossary_test",
            "displayName": "Glossary 2",
            "entryType": "glossary",
            "coreAspects": {
                "business_context": {
                    "name": (
                        "projects/123/locations/us/"
                        "entryGroups/test_entry_group_with_no_terms/"
                        "entries/empty_glossary_exists/"
                        "aspects/3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                    ),
                    "aspectType": "business_context",
                    "jsonContent": {
                        "description": (
                            "\u003cp\u003eEmpty glossary\u003c/p\u003e"
                        )
                    },
                    "createTime": "2023-05-11T17:18:00.838415Z",
                    "modifyTime": "2023-05-12T08:32:21.859231Z",
                }
            },
            "createTime": "2023-04-26T07:30:05.022015Z",
            "modifyTime": "2023-04-26T07:30:05.022015Z",
            "entryUid": "71372af7-bb1a-4020-aba8-223c57c366d2",
        },
        200,
    )

  elif re.fullmatch(
      (
          ".+/v2/projects/.+/locations/.+/"
          r"entryGroups/test_entry_group_with_terms/entries\?view=FULL$"
      ),
      url,
  ):
    return MockResponse(
        {
            "entries": [
                {
                    "name": "purchase_numberxswfrh",
                    "displayName": "Purchase number",
                    "entryType": "glossary_term",
                    "createTime": "2023-04-26T07:30:05.022015Z",
                    "modifyTime": "2023-04-26T07:30:05.022015Z",
                    "entryUid": "c1df00c7-2e2f-4d47-9d1c-d16a9dfbb7a9",
                    "coreAspects": {
                        "business_context": {
                            "name": (
                                "projects/123/locations/us/"
                                "entryGroups/test_entry_group_with_terms/"
                                "entries/purchase_numberxswfrh/"
                                "aspects/"
                                "3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                            ),
                            "aspectType": "business_context",
                            "jsonContent": {
                                "description": (
                                    "\u003cp\u003ePurchase number"
                                    "description\u003c/p\u003e"
                                )
                            },
                            "createTime": "2023-05-11T17:18:00.838415Z",
                            "modifyTime": "2023-05-12T08:32:21.859231Z",
                        }
                    },
                    "coreRelationships": [{
                        "name": "projects/2e64c8013eb2c67a0ee2e",
                        "relationshipType": "is_child_of",
                        "destinationEntryName": (
                            "entries/71372af7-bb1a-4020-aba8-223c57c366d2"
                        ),
                    }],
                },
                {
                    "name": "dc_glossary_test",
                    "displayName": "Glossary 2",
                    "entryType": "glossary",
                    "coreAspects": {
                        "business_context": {
                            "name": (
                                "projects/123/locations/us/"
                                "entryGroups/test_entry_group_with_terms/"
                                "entries/glossary_exists/"
                                "aspects/"
                                "3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                            ),
                            "aspectType": "business_context",
                            "jsonContent": {
                                "description": (
                                    "\u003cp\u003eGlossary with"
                                    "terms.\u003c/p\u003e"
                                )
                            },
                            "createTime": "2023-05-11T17:18:00.838415Z",
                            "modifyTime": "2023-05-12T08:32:21.859231Z",
                        }
                    },
                    "createTime": "2023-04-26T07:30:05.022015Z",
                    "modifyTime": "2023-04-26T07:30:05.022015Z",
                    "entryUid": "71372af7-bb1a-4020-aba8-223c57c366d2",
                },
            ]
        },
        200,
    )
  elif re.fullmatch(
      (
          ".+/v2/projects/123/locations/us/"
          "entryGroups/test_entry_group_with_terms/entries/glossary_exists"
      ),
      url,
  ):
    return MockResponse(
        {
            "name": "dc_glossary_test",
            "displayName": "Glossary 2",
            "entryType": "glossary",
            "coreAspects": {
                "business_context": {
                    "name": (
                        "projects/123/locations/us/"
                        "entryGroups/test_entry_group_with_terms/"
                        "entries/glossary_exists/"
                        "aspects/3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                    ),
                    "aspectType": "business_context",
                    "jsonContent": {
                        "description": (
                            "\u003cp\u003eGlossary with terms.\u003c/p\u003e"
                        )
                    },
                    "createTime": "2023-05-11T17:18:00.838415Z",
                    "modifyTime": "2023-05-12T08:32:21.859231Z",
                }
            },
            "createTime": "2023-04-26T07:30:05.022015Z",
            "modifyTime": "2023-04-26T07:30:05.022015Z",
            "entryUid": "71372af7-bb1a-4020-aba8-223c57c366d2",
        },
        200,
    )
  elif re.fullmatch(
      (
          ".+/v2/projects/.+/locations/.+/"
          r"entryGroups/test_entry_group_with_categories_and_terms/entries\?view=FULL$"
      ),
      url,
  ):
    return MockResponse(
        {
            "entries": [
                {
                    "name": "PII_data",
                    "displayName": "PII data",
                    "entryType": "glossary_category",
                    "coreAspects": {
                        "business_context": {
                            "name": (
                                "projects/123/locations/us/entryGroups/"
                                "test_entry_group_with_categories_and_terms/"
                                "entries/PII_data/aspects/"
                                "3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                            ),
                            "aspectType": "business_context",
                            "jsonContent": {
                                "description": (
                                    "\u003cp\u003ePII data"
                                    " description\u003c/p\u003e"
                                )
                            },
                            "createTime": "2023-08-29T07:36:45.771375Z",
                            "modifyTime": "2023-08-29T07:36:45.771375Z",
                        }
                    },
                    "createTime": "2023-08-29T07:36:45.771375Z",
                    "modifyTime": "2023-08-29T07:36:45.771375Z",
                    "entryUid": "2352bc5f-2fae-4350-86be-1708f1b04b27",
                    "coreRelationships": [{
                        "name": "projects/3486de3ed96e322999e2c3b66ab0eb94",
                        "relationshipType": "is_child_of",
                        "destinationEntryName": (
                            "entries/8f130395-ca37-4e99-b0cc-0ac975e66607"
                        ),
                    }],
                },
                {
                    "name": "FirstName",
                    "displayName": "First Name",
                    "entryType": "glossary_term",
                    "coreAspects": {
                        "business_context": {
                            "name": (
                                "projects/123/locations/us/entryGroups/"
                                "test_entry_group_with_categories_and_terms/"
                                "entries/FirstName/aspects/"
                                "3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                            ),
                            "aspectType": "business_context",
                            "jsonContent": {
                                "description": (
                                    "\u003cp\u003eFirst name"
                                    " description\u003c/p\u003e"
                                )
                            },
                            "createTime": "2023-08-29T07:36:44.459135Z",
                            "modifyTime": "2023-08-29T07:36:44.459135Z",
                        }
                    },
                    "createTime": "2023-08-29T07:36:44.459135Z",
                    "modifyTime": "2023-08-29T07:36:44.459135Z",
                    "entryUid": "c57bd02e-f55b-4226-b8af-2061a28f4cee",
                    "coreRelationships": [{
                        "name": "projects/4c7c287071346e4f096d552172e28a08",
                        "relationshipType": "is_child_of",
                        "destinationEntryName": (
                            "entries/8f130395-ca37-4e99-b0cc-0ac975e66607"
                        ),
                    }],
                },
                {
                    "name": "glossary_not_empty",
                    "displayName": "glossary_not_empty",
                    "entryType": "glossary",
                    "coreAspects": {
                        "business_context": {
                            "name": (
                                "projects/123/locations/us/entryGroups/"
                                "test_entry_group_with_categories_and_terms/"
                                "entries/glossary_not_empty/aspects/"
                                "3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                            ),
                            "aspectType": "business_context",
                            "jsonContent": {
                                "description": (
                                    "\u003cp\u003eGlossary with categories and"
                                    "terms.\u003c/p\u003e"
                                )
                            },
                            "createTime": "2023-08-29T07:36:44.155839Z",
                            "modifyTime": "2023-08-29T07:36:44.155839Z",
                        }
                    },
                    "createTime": "2023-08-29T07:36:44.155839Z",
                    "modifyTime": "2023-08-29T08:54:29.506959Z",
                    "entryUid": "8f130395-ca37-4e99-b0cc-0ac975e66607",
                },
            ]
        },
        200,
    )
  elif re.fullmatch(
      (
          ".+/v2/projects/123/locations/us/"
          "entryGroups/test_entry_group_with_categories_and_terms/entries/glossary_not_empty"
      ),
      url,
  ):
    return MockResponse(
        {
            "name": "glossary_not_empty",
            "displayName": "glossary_not_empty",
            "entryType": "glossary",
            "coreAspects": {
                "business_context": {
                    "name": (
                        "projects/123/locations/us/entryGroups/"
                        "test_entry_group_with_categories_and_terms/entries/"
                        "glossary_not_empty/aspects/"
                        "3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                    ),
                    "aspectType": "business_context",
                    "jsonContent": {
                        "description": (
                            "\u003cp\u003eGlossary with categories and"
                            "terms.\u003c/p\u003e"
                        )
                    },
                    "createTime": "2023-08-29T07:36:44.155839Z",
                    "modifyTime": "2023-08-29T07:36:44.155839Z",
                }
            },
            "createTime": "2023-08-29T07:36:44.155839Z",
            "modifyTime": "2023-08-29T08:54:29.506959Z",
            "entryUid": "8f130395-ca37-4e99-b0cc-0ac975e66607",
        },
        200,
    )
  elif re.fullmatch(
      (
          ".+/v2/projects/.+/locations/.+/"
          r"entryGroups/test_entry_group_with_categories/entries\?view=FULL$"
      ),
      url,
  ):
    return MockResponse(
        {
            "entries": [
                {
                    "name": "PII_data",
                    "displayName": "PII data",
                    "entryType": "glossary_category",
                    "coreAspects": {
                        "business_context": {
                            "name": (
                                "projects/123/locations/us/entryGroups/"
                                "test_entry_group_with_categories/"
                                "entries/PII_data/aspects/"
                                "3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                            ),
                            "aspectType": "business_context",
                            "jsonContent": {
                                "description": (
                                    "\u003cp\u003ePII data"
                                    " description\u003c/p\u003e"
                                )
                            },
                            "createTime": "2023-08-29T07:36:45.771375Z",
                            "modifyTime": "2023-08-29T07:36:45.771375Z",
                        }
                    },
                    "createTime": "2023-08-29T07:36:45.771375Z",
                    "modifyTime": "2023-08-29T07:36:45.771375Z",
                    "entryUid": "2352bc5f-2fae-4350-86be-1708f1b04b27",
                    "coreRelationships": [{
                        "name": "projects/3486de3ed96e322999e2c3b66ab0eb94",
                        "relationshipType": "is_child_of",
                        "destinationEntryName": (
                            "entries/8f130395-ca37-4e99-b0cc-0ac975e66607"
                        ),
                    }],
                },
                {
                    "name": "glossary_not_empty",
                    "displayName": "glossary_not_empty",
                    "entryType": "glossary",
                    "coreAspects": {
                        "business_context": {
                            "name": (
                                "projects/123/locations/us/entryGroups/"
                                "test_entry_group_with_categories/"
                                "entries/glossary_not_empty/aspects/"
                                "3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                            ),
                            "aspectType": "business_context",
                            "jsonContent": {
                                "description": (
                                    "\u003cp\u003eGlossary with categories and"
                                    "terms.\u003c/p\u003e"
                                )
                            },
                            "createTime": "2023-08-29T07:36:44.155839Z",
                            "modifyTime": "2023-08-29T07:36:44.155839Z",
                        }
                    },
                    "createTime": "2023-08-29T07:36:44.155839Z",
                    "modifyTime": "2023-08-29T08:54:29.506959Z",
                    "entryUid": "8f130395-ca37-4e99-b0cc-0ac975e66607",
                },
            ]
        },
        200,
    )
  elif re.fullmatch(
      (
          ".+/v2/projects/123/locations/us/"
          "entryGroups/test_entry_group_with_categories/entries/glossary_not_empty"
      ),
      url,
  ):
    return MockResponse(
        {
            "name": "glossary_not_empty",
            "displayName": "glossary_not_empty",
            "entryType": "glossary",
            "coreAspects": {
                "business_context": {
                    "name": (
                        "projects/123/locations/us/entryGroups/"
                        "test_entry_group_with_categories/entries/"
                        "glossary_not_empty/aspects/"
                        "3f6ee7a1-07d3-4d2b-a76a-7f4d06aaa34e"
                    ),
                    "aspectType": "business_context",
                    "jsonContent": {
                        "description": (
                            "\u003cp\u003eGlossary with"
                            "categories\u003c/p\u003e"
                        )
                    },
                    "createTime": "2023-08-29T07:36:44.155839Z",
                    "modifyTime": "2023-08-29T07:36:44.155839Z",
                }
            },
            "createTime": "2023-08-29T07:36:44.155839Z",
            "modifyTime": "2023-08-29T08:54:29.506959Z",
            "entryUid": "8f130395-ca37-4e99-b0cc-0ac975e66607",
        },
        200,
    )


def mocked_post_failed_api_response(url, headers=None, json=None):  # pylint: disable=unused-argument
  return MockResponse({}, 404)
