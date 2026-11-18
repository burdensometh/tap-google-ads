"""Tests that sync mode advertises the delete column to the target."""
import io
import json
import unittest
from contextlib import redirect_stdout
from unittest.mock import patch

import singer
from tap_google_ads import sync as tap_sync


REPORT_STREAM = "campaign_performance_report"
CUSTOMER = {"customerId": "123", "loginCustomerId": "456"}


class FakeReportStream:
    """Stand-in for a ReportStream; records the catalog entry it was given."""

    def __init__(self):
        self.synced_entries = []

    def sync(self, sdk_client, customer, stream, config, state, query_limit):
        self.synced_entries.append(stream)


def catalog_without_deleted_at():
    """A catalog as discovered before date partition reconciliation existed."""
    metadata = singer.metadata.to_list({
        (): {
            "inclusion": "available",
            "table-key-properties": ["_sdc_record_hash"],
            "forced-replication-method": "INCREMENTAL",
            "valid-replication-keys": ["date"],
            "selected": True,
        },
        ("properties", "_sdc_record_hash"): {
            "inclusion": "automatic",
            "behavior": "PRIMARY KEY",
        },
        ("properties", "campaign_id"): {
            "inclusion": "automatic",
            "behavior": "ATTRIBUTE",
            "fieldExclusions": [],
            "tap-google-ads.api-field-names": ["campaign.id"],
        },
        ("properties", "clicks"): {
            "inclusion": "automatic",
            "behavior": "METRIC",
            "fieldExclusions": [],
            "tap-google-ads.api-field-names": ["metrics.clicks"],
        },
        ("properties", "date"): {
            "inclusion": "automatic",
            "behavior": "SEGMENT",
            "fieldExclusions": [],
            "tap-google-ads.api-field-names": ["segments.date"],
        },
    })
    return {
        "streams": [{
            "tap_stream_id": REPORT_STREAM,
            "stream": REPORT_STREAM,
            "schema": {
                "type": ["null", "object"],
                "is_report": True,
                "properties": {
                    "_sdc_record_hash": {"type": "string"},
                    "campaign_id": {"type": ["null", "integer"]},
                    "clicks": {"type": ["null", "integer"]},
                    "date": {"type": ["null", "string"], "format": "date-time"},
                },
            },
            "metadata": metadata,
        }]
    }


class DoSyncSchemaTest(unittest.TestCase):

    def run_do_sync(self, config, catalog):
        report_streams = {REPORT_STREAM: FakeReportStream()}
        stdout = io.StringIO()
        with patch("tap_google_ads.sync.create_sdk_client"), \
             patch("tap_google_ads.sync.initialize_core_streams", return_value={}), \
             patch("tap_google_ads.sync.initialize_reports", return_value=report_streams), \
             redirect_stdout(stdout):
            tap_sync.do_sync(config, catalog, {}, {})
        messages = [json.loads(line) for line in stdout.getvalue().splitlines() if line]
        return messages, report_streams[REPORT_STREAM]

    def test_delete_column_is_in_the_schema_sent_to_the_target(self):
        config = {
            "login_customer_ids": [CUSTOMER],
            "start_date": "2022-01-01T00:00:00Z",
            "date_partition_reconciliation": True,
        }
        messages, _ = self.run_do_sync(config, catalog_without_deleted_at())

        schemas = [m for m in messages if m["type"] == "SCHEMA"]
        self.assertEqual(1, len(schemas))
        self.assertIn("_sdc_deleted_at", schemas[0]["schema"]["properties"])

    def test_delete_column_is_absent_when_mode_is_off(self):
        config = {
            "login_customer_ids": [CUSTOMER],
            "start_date": "2022-01-01T00:00:00Z",
        }
        messages, _ = self.run_do_sync(config, catalog_without_deleted_at())

        schemas = [m for m in messages if m["type"] == "SCHEMA"]
        self.assertEqual(1, len(schemas))
        self.assertNotIn("_sdc_deleted_at", schemas[0]["schema"]["properties"])

    def test_stream_receives_the_upgraded_catalog_entry(self):
        config = {
            "login_customer_ids": [CUSTOMER],
            "start_date": "2022-01-01T00:00:00Z",
            "date_partition_reconciliation": True,
        }
        _, report_stream = self.run_do_sync(config, catalog_without_deleted_at())

        self.assertEqual(1, len(report_stream.synced_entries))
        synced_schema = report_stream.synced_entries[0]["schema"]
        self.assertIn("_sdc_deleted_at", synced_schema["properties"])

        metadata = singer.metadata.to_map(report_stream.synced_entries[0]["metadata"])
        self.assertEqual(
            "automatic",
            metadata[("properties", "_sdc_deleted_at")]["inclusion"],
        )


if __name__ == "__main__":
    unittest.main()
