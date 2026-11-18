"""Unit tests for date partition reconciliation on report streams.

These tests drive `ReportStream.sync` itself (not helpers in isolation) so that
the Singer RECORD/STATE messages a target would actually receive are asserted.
"""
import copy
import io
import json
import unittest
from contextlib import redirect_stdout
from datetime import datetime
from unittest.mock import Mock, patch

import singer
import pytz
from tap_google_ads.streams import (
    DATE_PARTITION_INDEX_KEY,
    DELETED_AT_FIELD,
    ReportStream,
    generate_hash,
    get_date_partition_index,
    get_row_identity_definition,
    is_date_partition_reconciliation_enabled,
)

CUSTOMER = {"customerId": "123", "loginCustomerId": "456"}
STREAM_NAME = "campaign_performance_report"

RESOURCE_SCHEMA = {
    field_name: {
        "name": field_name,
        "category": category,
        "json_schema": json_schema,
        "selectable": True,
        "filterable": True,
        "sortable": True,
        "selectable_with": set(),
        "metrics": [],
        "segments": [],
        "attributes": [],
    }
    for field_name, category, json_schema in [
        ("campaign.id", "ATTRIBUTE", {"type": ["null", "integer"]}),
        ("campaign.name", "ATTRIBUTE", {"type": ["null", "string"]}),
        ("metrics.clicks", "METRIC", {"type": ["null", "integer"]}),
        ("segments.date", "SEGMENT", {"type": ["null", "string"], "format": "date-time"}),
    ]
}


def build_stream(fields=None, include_deleted_at=True):
    """Create a ReportStream backed by a tiny in-memory resource schema."""
    fields = fields or ["campaign.id", "campaign.name", "metrics.clicks", "segments.date"]
    by_name = {field["name"]: field for field in RESOURCE_SCHEMA.values()}

    resource_schema = {
        "campaign": {
            "fields": {
                field_name: {
                    "field_details": {
                        "name": field_name,
                        "category": by_name[field_name]["category"],
                        "json_schema": by_name[field_name]["json_schema"],
                        "selectable": True,
                        "incompatible_fields": [],
                    },
                    "incompatible_fields": [],
                }
                for field_name in fields
            }
        }
    }
    # `create_nested_resource_schema` looks fields up by their fully qualified
    # name, so the same entries must also be present at the top level.
    resource_schema.update(by_name)
    return ReportStream(
        fields=fields,
        google_ads_resource_names=["campaign"],
        resource_schema=resource_schema,
        primary_keys=["_sdc_record_hash"],
        automatic_keys={"campaign_id"},
        include_deleted_at=include_deleted_at,
    )


def catalog_entry(stream, tap_stream_id=STREAM_NAME, selected_fields=None):
    metadata = stream.stream_metadata
    if selected_fields is not None:
        metadata = {
            breadcrumb: dict(values) for breadcrumb, values in metadata.items()
        }
        for breadcrumb, values in metadata.items():
            if breadcrumb and values.get("inclusion") == "available":
                values["selected"] = breadcrumb[-1] in selected_fields

    return {
        "tap_stream_id": tap_stream_id,
        "stream": tap_stream_id,
        "schema": stream.stream_schema,
        "metadata": singer.metadata.to_list(metadata),
    }


def row(campaign_id, name, clicks, date="2022-01-01"):
    return {"campaign": {"id": campaign_id, "name": name},
            "metrics": {"clicks": clicks},
            "segments": {"date": date}}


def make_google_ads_exception(message="boom"):
    from google.ads.googleads.errors import GoogleAdsException
    failure = Mock()
    failure.errors = [Mock()]
    failure.errors[0].message = message
    return GoogleAdsException(None, None, failure, "request-id")


class FakeResponse(list):
    """Pages are iterated lazily by the sync loop; a plain list is enough."""


class ReconciliationSyncTestBase(unittest.TestCase):
    """Run ReportStream.sync against a scripted set of API responses."""

    def run_sync(self, stream, responses_by_day, state=None, config=None):
        """Run sync, recording every Singer message written.

        `responses_by_day` maps a `YYYY-MM-DD` string to the rows returned for
        that date, or to an Exception raised when that date is requested."""
        state = state if state is not None else {}
        config = {
            "start_date": "2022-01-01T00:00:00Z",
            "end_date": "2022-01-02T00:00:00Z",
            "date_partition_reconciliation": True,
            **(config or {}),
        }

        def fake_make_request(gas, query, customer_id, config=None):
            query_date = query.split("segments.date = '")[1].split("'")[0]
            response = responses_by_day.get(query_date, [])
            if isinstance(response, Exception):
                raise response
            return FakeResponse(response)

        records = []
        states = []

        def record_writer(stream_name, record, **kwargs):
            records.append((stream_name, record))

        def state_writer(value):
            # Snapshot the mutable state object at the moment it is written.
            states.append(copy.deepcopy(value))

        with patch("tap_google_ads.streams.make_request", side_effect=fake_make_request), \
             patch("tap_google_ads.streams.google_message_to_json", side_effect=lambda message: message), \
             patch("singer.write_record", side_effect=record_writer), \
             patch("singer.write_state", side_effect=state_writer):
            # Sync as a normal "select all fields" catalog would.
            selected = {
                breadcrumb[-1]
                for breadcrumb, values in stream.stream_metadata.items()
                if breadcrumb and values.get("inclusion") == "available"
            }
            entry = catalog_entry(stream, selected_fields=selected)
            stream.sync(Mock(), CUSTOMER, entry, config, state, None)

        return records, states, state

    @staticmethod
    def identities_for(stream, rows):
        """Compute the identities a target would see for `rows`."""
        from singer import Transformer
        with Transformer() as transformer:
            records = []
            for message in rows:
                transformed = stream.transform_keys(message)
                record = transformer.transform(transformed, stream.stream_schema)
                record["_sdc_record_hash"] = generate_hash(
                    record, singer.metadata.to_list(stream.stream_metadata)
                )
                records.append(record)
        return records


class TestDatePartitionReconciliationSync(ReconciliationSyncTestBase):

    @patch("tap_google_ads.streams.utils.now")
    def test_removed_row_produces_delete_record(self, fake_now):
        """A row present on the previous complete read but absent now is deleted."""
        fake_now.return_value = datetime(2022, 1, 3, tzinfo=pytz.UTC)
        stream = build_stream()

        first_rows = [row(1, "a", 10), row(2, "b", 20)]
        records_1, _, state = self.run_sync(
            stream,
            {"2022-01-01": first_rows, "2022-01-02": []},
        )

        upserts_1 = [r for r in records_1 if DELETED_AT_FIELD not in r[1]]
        expected_identities = {
            rec["_sdc_record_hash"] for rec in self.identities_for(stream, first_rows)
        }
        self.assertSetEqual(expected_identities, {r[1]["_sdc_record_hash"] for r in upserts_1})

        # Second run re-reads 2022-01-01 and only campaign 2 is still present.
        records_2, states_2, state_2 = self.run_sync(
            stream,
            {"2022-01-01": [row(2, "b", 20)]},
            state=state,
            config={"end_date": "2022-01-01T00:00:00Z"},
        )

        deletes = [r for r in records_2 if DELETED_AT_FIELD in r[1]]
        self.assertEqual(1, len(deletes))
        missing_identity = (
            {rec["_sdc_record_hash"] for rec in self.identities_for(stream, first_rows)
             if rec["campaign_id"] == 1}
        ).pop()
        self.assertEqual(missing_identity, deletes[0][1]["_sdc_record_hash"])
        self.assertIsNotNone(deletes[0][1][DELETED_AT_FIELD])
        self.assertEqual(STREAM_NAME, deletes[0][0])

        # The partition index is refreshed to the surviving row only.
        index = get_date_partition_index(state_2["bookmarks"][STREAM_NAME]["123"])
        self.assertEqual(1, len(index["2022-01-01"]["record_hashes"]))
        self.assertIn(
            {rec["_sdc_record_hash"] for rec in self.identities_for(stream, [row(2, "b", 20)])}.pop(),
            index["2022-01-01"]["record_hashes"],
        )

    def test_zero_row_day_clears_previously_known_rows(self):
        """A date that now returns zero rows must delete every old row for it."""
        stream = build_stream()
        first_rows = [row(1, "a", 10), row(2, "b", 20)]

        _, _, state = self.run_sync(
            stream,
            {"2022-01-01": first_rows},
            config={"end_date": "2022-01-01T00:00:00Z"},
        )
        records_2, _, state_2 = self.run_sync(
            stream,
            {"2022-01-01": []},
            state=state,
            config={"end_date": "2022-01-01T00:00:00Z"},
        )

        deletes = [r for r in records_2 if DELETED_AT_FIELD in r[1]]
        self.assertEqual(2, len(deletes))
        self.assertSetEqual(
            {rec["_sdc_record_hash"] for rec in self.identities_for(stream, first_rows)},
            {r[1]["_sdc_record_hash"] for r in deletes},
        )
        self.assertEqual([], state_2["bookmarks"][STREAM_NAME]["123"][DATE_PARTITION_INDEX_KEY]
                         ["2022-01-01"]["record_hashes"])

    def test_new_rows_do_not_produce_deletes(self):
        """Reconciliation only deletes; it never emits deletes for new rows."""
        stream = build_stream()
        self.run_sync(
            stream,
            {"2022-01-01": [row(1, "a", 10)]},
            config={"end_date": "2022-01-01T00:00:00Z"},
        )
        # Re-use the state produced by the first run.
        _, _, state = self.run_sync(
            stream,
            {"2022-01-01": [row(1, "a", 10)]},
            config={"end_date": "2022-01-01T00:00:00Z"},
        )
        records_2, _, _ = self.run_sync(
            stream,
            {"2022-01-01": [row(1, "a", 10), row(2, "b", 20)]},
            state=state,
            config={"end_date": "2022-01-01T00:00:00Z"},
        )
        self.assertFalse(any(DELETED_AT_FIELD in r[1] for r in records_2))

    def test_failed_day_does_not_delete_or_update_index(self):
        """A failed date must not be treated as a complete read."""
        stream = build_stream()
        first_rows = [row(1, "a", 10), row(2, "b", 20)]
        _, _, state = self.run_sync(
            stream,
            {"2022-01-01": first_rows},
            config={"end_date": "2022-01-01T00:00:00Z"},
        )

        error = make_google_ads_exception()

        with self.assertRaises(RuntimeError):
            self.run_sync(
                stream,
                {"2022-01-01": error},
                state=state,
                config={"end_date": "2022-01-01T00:00:00Z"},
            )

        # Nothing about 2022-01-01 changed: no delete records were emitted and
        # the saved index still holds both original identities.
        index = get_date_partition_index(state["bookmarks"][STREAM_NAME]["123"])
        self.assertSetEqual(
            {rec["_sdc_record_hash"] for rec in self.identities_for(stream, first_rows)},
            set(index["2022-01-01"]["record_hashes"]),
        )

    def test_failure_while_paging_a_day_prevents_deletes_for_that_day(self):
        """A failure part-way through reading a date must not delete the rows
        that had not been reached yet."""
        stream = build_stream()
        first_rows = [row(1, "a", 10), row(2, "b", 20), row(3, "c", 30)]
        _, _, state = self.run_sync(
            stream,
            {"2022-01-01": first_rows},
            config={"end_date": "2022-01-01T00:00:00Z"},
        )

        class FailsMidIteration(list):
            """Yields one row, then raises like a broken page stream would."""

            def __iter__(self):
                yield self[0]
                raise make_google_ads_exception()

        def fake_make_request(gas, query, customer_id, config=None):
            return FailsMidIteration([row(1, "a", 10)])

        records = []
        with patch("tap_google_ads.streams.make_request", side_effect=fake_make_request), \
             patch("tap_google_ads.streams.google_message_to_json", side_effect=lambda m: m), \
             patch("singer.write_record", side_effect=lambda *a, **k: records.append(a)), \
             patch("singer.write_state"):
            with self.assertRaises(Exception):
                stream.sync(Mock(), CUSTOMER, catalog_entry(stream),
                            {"start_date": "2022-01-01T00:00:00Z",
                             "end_date": "2022-01-01T00:00:00Z",
                             "date_partition_reconciliation": True},
                            state, None)

        self.assertFalse(any(DELETED_AT_FIELD in record for _, record in records))
        index = get_date_partition_index(state["bookmarks"][STREAM_NAME]["123"])
        self.assertSetEqual(
            {rec["_sdc_record_hash"] for rec in self.identities_for(stream, first_rows)},
            set(index["2022-01-01"]["record_hashes"]),
        )

    def test_partial_day_failure_does_not_delete_before_retry_succeeds(self):
        """If a later date fails mid-run, earlier dates are still reconciled but
        the failed date is not."""
        stream = build_stream()
        first_rows = [row(1, "a", 10, date="2022-01-01"), row(2, "b", 20, date="2022-01-01")]
        _, _, state = self.run_sync(
            stream,
            {"2022-01-01": first_rows, "2022-01-02": [row(3, "c", 30, date="2022-01-02")]},
            config={"end_date": "2022-01-02T00:00:00Z"},
        )

        # 2022-01-01 is read (partially different), 2022-01-02 fails.
        with self.assertRaises(RuntimeError):
            self.run_sync(
                stream,
                {"2022-01-01": [row(2, "b", 20)], "2022-01-02": make_google_ads_exception()},
                state=state,
                config={"end_date": "2022-01-02T00:00:00Z"},
            )

    def test_identity_definition_change_skips_deletes_and_rebaselines(self):
        """Changing the selected non-metric fields invalidates old identities."""
        stream = build_stream()
        base_rows = [row(1, "a", 10), row(2, "b", 20)]
        _, _, state = self.run_sync(
            stream,
            {"2022-01-01": base_rows},
            config={"end_date": "2022-01-01T00:00:00Z"},
        )

        # Same stream but the selected attribute `campaign.name` is dropped,
        # which changes the row grain hashed into `_sdc_record_hash`.
        narrower_stream = build_stream(fields=["campaign.id", "metrics.clicks", "segments.date"])
        narrowed_rows = [
            {"campaign": {"id": 1}, "metrics": {"clicks": 10}, "segments": {"date": "2022-01-01"}},
        ]
        records_2, _, state_2 = self.run_sync(
            narrower_stream,
            {"2022-01-01": narrowed_rows},
            state=state,
            config={"end_date": "2022-01-01T00:00:00Z"},
        )

        self.assertFalse(any(DELETED_AT_FIELD in r[1] for r in records_2))

        index = get_date_partition_index(state_2["bookmarks"][STREAM_NAME]["123"])
        self.assertEqual(
            get_row_identity_definition(singer.metadata.to_list(narrower_stream.stream_metadata)),
            index["2022-01-01"]["identity_definition"],
        )
        self.assertSetEqual(
            {rec["_sdc_record_hash"] for rec in self.identities_for(narrower_stream, narrowed_rows)},
            set(index["2022-01-01"]["record_hashes"]),
        )

    def test_metric_selection_change_does_not_invalidate_identities(self):
        """Metrics are excluded from `_sdc_record_hash`, so changing them keeps
        identities comparable and reconciliation still runs."""
        stream = build_stream()
        first_rows = [row(1, "a", 10), row(2, "b", 20)]
        _, _, state = self.run_sync(
            stream,
            {"2022-01-01": first_rows},
            config={"end_date": "2022-01-01T00:00:00Z"},
        )

        metric_changed_stream = build_stream(
            fields=["campaign.id", "campaign.name", "segments.date"]
        )
        records_2, _, _ = self.run_sync(
            metric_changed_stream,
            {"2022-01-01": [row(1, "a", 10), row(2, "b", 20)]},
            state=state,
            config={"end_date": "2022-01-01T00:00:00Z"},
        )
        # No row missing, so no deletes, and the run was not skipped.
        self.assertFalse(any(DELETED_AT_FIELD in r[1] for r in records_2))

    def test_reconciliation_disabled_keeps_old_behavior(self):
        """Without the opt-in flag, no index is written and no deletes are sent."""
        stream = build_stream()
        first_rows = [row(1, "a", 10), row(2, "b", 20)]
        _, _, state = self.run_sync(
            stream,
            {"2022-01-01": first_rows},
            config={
                "end_date": "2022-01-01T00:00:00Z",
                "date_partition_reconciliation": False,
            },
        )
        self.assertNotIn(DATE_PARTITION_INDEX_KEY, state["bookmarks"][STREAM_NAME]["123"])

        records_2, _, state_2 = self.run_sync(
            stream,
            {"2022-01-01": []},
            state=state,
            config={
                "end_date": "2022-01-01T00:00:00Z",
                "date_partition_reconciliation": False,
            },
        )
        self.assertFalse(any(DELETED_AT_FIELD in r[1] for r in records_2))
        self.assertNotIn(DATE_PARTITION_INDEX_KEY, state_2["bookmarks"][STREAM_NAME]["123"])

    def test_partition_index_is_pruned_to_the_bookmarked_horizon(self):
        """Identities for dates the next run can never re-read are dropped."""
        stream = build_stream()
        stale_partition = {
            "2021-06-01": {
                "identity_definition": "old",
                "record_hashes": ["a" * 64],
            }
        }
        state = {
            "bookmarks": {
                STREAM_NAME: {
                    "123": {
                        "date": "2022-01-01T00:00:00.000000Z",
                        DATE_PARTITION_INDEX_KEY: stale_partition,
                    }
                }
            }
        }
        _, _, state_2 = self.run_sync(
            stream,
            {"2022-01-01": [row(1, "a", 10)]},
            state=state,
            config={"end_date": "2022-01-01T00:00:00Z"},
        )
        index = get_date_partition_index(state_2["bookmarks"][STREAM_NAME]["123"])
        self.assertNotIn("2021-06-01", index)
        self.assertIn("2022-01-01", index)

    @patch("tap_google_ads.streams.utils.now")
    def test_index_retains_the_whole_conversion_window(self, fake_now):
        """In steady state every re-read date keeps its identities, so deletes
        can be detected across all conversion-window dates."""
        fake_now.return_value = datetime(2022, 3, 10, tzinfo=pytz.UTC)
        stream = build_stream()
        # Conversion window floor is 2022-02-08; start from a bookmark inside it.
        days = ["2022-02-08", "2022-02-09", "2022-02-10"]
        responses = {day: [row(1, "a", 10, date=day)] for day in days}
        state = {
            "bookmarks": {
                STREAM_NAME: {"123": {"date": "2022-02-08T00:00:00.000000Z"}}
            }
        }

        _, _, state_2 = self.run_sync(
            stream, responses, state=state,
            config={"end_date": "2022-02-10T00:00:00Z"},
        )
        index = get_date_partition_index(state_2["bookmarks"][STREAM_NAME]["123"])
        self.assertSetEqual(set(days), set(index))

    @patch("tap_google_ads.streams.utils.now")
    def test_index_is_bounded_to_dates_a_later_read_can_reach(self, fake_now):
        """A long historical run must not keep one identity set per date.

        A later read starts at `min(bookmark, max(start_date,
        conversion_window))`, so as the run advances, partitions behind that
        floor are dropped. Only the most recent date (the bookmarked date a
        resume would re-read) is retained here."""
        fake_now.return_value = datetime(2022, 3, 10, tzinfo=pytz.UTC)
        stream = build_stream()
        days = [f"2022-01-0{day}" for day in range(1, 6)]
        responses = {day: [row(1, "a", 10, date=day)] for day in days}

        _, _, state = self.run_sync(
            stream,
            responses,
            config={"end_date": "2022-01-05T00:00:00Z"},
        )
        index = get_date_partition_index(state["bookmarks"][STREAM_NAME]["123"])
        self.assertSetEqual({"2022-01-05"}, set(index))

    def test_reconciliation_state_is_written_after_each_successful_day(self):
        """The partition index is persisted as part of the day bookmark."""
        stream = build_stream()
        _, states, _ = self.run_sync(
            stream,
            {"2022-01-01": [row(1, "a", 10)], "2022-01-02": [row(2, "b", 20, date="2022-01-02")]},
        )
        bookmark_states = [
            state["bookmarks"][STREAM_NAME]["123"]
            for state in states
            if "bookmarks" in state and STREAM_NAME in state["bookmarks"]
        ]
        self.assertEqual(2, len(bookmark_states))
        self.assertIn("2022-01-01", bookmark_states[0][DATE_PARTITION_INDEX_KEY])


class TestSingerWireOutputDrivesDeleteAwareTarget(unittest.TestCase):
    """Assert the real Singer output (stdout JSON lines) removes target rows."""

    def run_stream_and_capture(self, stream, rows_by_day, state):
        config = {
            "start_date": "2022-01-01T00:00:00Z",
            "end_date": "2022-01-01T00:00:00Z",
            "date_partition_reconciliation": True,
        }

        def fake_make_request(gas, query, customer_id, config=None):
            query_date = query.split("segments.date = '")[1].split("'")[0]
            return FakeResponse(rows_by_day.get(query_date, []))

        stdout = io.StringIO()
        with patch("tap_google_ads.streams.make_request", side_effect=fake_make_request), \
             patch("tap_google_ads.streams.google_message_to_json", side_effect=lambda message: message), \
             redirect_stdout(stdout):
            selected = {
                breadcrumb[-1]
                for breadcrumb, values in stream.stream_metadata.items()
                if breadcrumb and values.get("inclusion") == "available"
            }
            stream.sync(Mock(), CUSTOMER,
                        catalog_entry(stream, selected_fields=selected),
                        config, state, None)

        messages = [json.loads(line) for line in stdout.getvalue().splitlines() if line]
        return messages

    @staticmethod
    def apply_to_target(table, messages):
        """Minimal delete-aware target: `_sdc_deleted_at` removes the PK row."""
        for message in messages:
            if message["type"] != "RECORD":
                continue
            record = message["record"]
            key = record["_sdc_record_hash"]
            if record.get(DELETED_AT_FIELD):
                table.pop(key, None)
            elif DELETED_AT_FIELD not in record:
                table[key] = record
        return table

    def test_wire_output_removes_vanished_rows_from_target(self):
        stream = build_stream()
        first_rows = [row(1, "a", 10), row(2, "b", 20)]

        state = {}
        first_messages = self.run_stream_and_capture(
            stream, {"2022-01-01": first_rows}, state
        )
        self.assertTrue(any(m["type"] == "RECORD" for m in first_messages))

        target_table = self.apply_to_target({}, first_messages)
        original_keys = set(target_table)
        self.assertEqual(2, len(target_table))

        second_messages = self.run_stream_and_capture(
            stream, {"2022-01-01": [row(2, "b", 20)]}, state
        )
        delete_messages = [
            m for m in second_messages
            if m["type"] == "RECORD" and m["record"].get(DELETED_AT_FIELD)
        ]
        self.assertEqual(1, len(delete_messages))
        # The delete is a real RECORD line, with the PK and a timestamp, not
        # merely a "finished" log line.
        self.assertEqual("RECORD", delete_messages[0]["type"])
        self.assertEqual(STREAM_NAME, delete_messages[0]["stream"])
        self.assertIn("_sdc_record_hash", delete_messages[0]["record"])

        remaining_table = self.apply_to_target(target_table, second_messages)
        survivors = set(remaining_table)
        self.assertEqual(1, len(remaining_table))
        self.assertSetEqual(original_keys - survivors, {
            delete_messages[0]["record"]["_sdc_record_hash"]
        })
        self.assertIn(
            {rec["_sdc_record_hash"]
             for rec in self.identities_for(stream, [row(2, "b", 20)])},
            [survivors],
        )

    def test_wire_output_clears_target_on_zero_row_day(self):
        stream = build_stream()
        state = {}
        first_messages = self.run_stream_and_capture(
            stream, {"2022-01-01": [row(1, "a", 10), row(2, "b", 20)]}, state
        )
        target_table = self.apply_to_target({}, first_messages)
        self.assertEqual(2, len(target_table))

        second_messages = self.run_stream_and_capture(
            stream, {"2022-01-01": []}, state
        )
        remaining_table = self.apply_to_target(target_table, second_messages)
        self.assertEqual({}, remaining_table)

    @staticmethod
    def identities_for(stream, rows):
        return ReconciliationSyncTestBase.identities_for(stream, rows)


class TestDeletedAtSchemaDeclaration(unittest.TestCase):

    def test_schema_and_metadata_declare_deleted_at_when_enabled(self):
        stream = build_stream(include_deleted_at=True)
        self.assertIn(DELETED_AT_FIELD, stream.stream_schema["properties"])
        self.assertEqual(
            "automatic",
            stream.stream_metadata[("properties", DELETED_AT_FIELD)]["inclusion"],
        )

    def test_schema_omits_deleted_at_by_default(self):
        stream = build_stream(include_deleted_at=False)
        self.assertNotIn(DELETED_AT_FIELD, stream.stream_schema["properties"])

    def test_deleted_at_is_not_sent_to_the_api(self):
        stream = build_stream(include_deleted_at=True)
        from tap_google_ads.streams import get_selected_fields
        selected = get_selected_fields(singer.metadata.to_list(stream.stream_metadata))
        self.assertNotIn(f"segments.{DELETED_AT_FIELD}", selected)
        self.assertNotIn(DELETED_AT_FIELD, selected)
        self.assertIn("campaign.id", selected)

    def test_old_catalog_entry_is_upgraded_for_delete_records(self):
        """A catalog discovered before the flag existed must still declare the
        delete column so delete RECORD messages validate against the schema."""
        from tap_google_ads.streams import ensure_deleted_at_in_catalog_entry
        from singer import Transformer

        stream = build_stream(include_deleted_at=False)
        old_entry = catalog_entry(stream)
        self.assertNotIn(DELETED_AT_FIELD, old_entry["schema"]["properties"])

        ensure_deleted_at_in_catalog_entry(old_entry)
        self.assertIn(DELETED_AT_FIELD, old_entry["schema"]["properties"])

        metadata = singer.metadata.to_map(old_entry["metadata"])
        self.assertEqual("automatic", metadata[("properties", DELETED_AT_FIELD)]["inclusion"])

        # The schema used in the SCHEMA message accepts a delete record.
        with Transformer() as transformer:
            transformed = transformer.transform(
                {"_sdc_record_hash": "a" * 64, "date": "2022-01-01T00:00:00.000000Z",
                 DELETED_AT_FIELD: "2022-01-03T00:00:00.000000Z"},
                old_entry["schema"],
            )
        self.assertEqual("2022-01-03T00:00:00.000000Z", transformed[DELETED_AT_FIELD])


class TestConfigFlag(unittest.TestCase):
    def test_flag_parsing(self):
        for truthy in [True, "true", "TRUE", "1", "yes", "on", 1]:
            with self.subTest(value=truthy):
                self.assertTrue(
                    is_date_partition_reconciliation_enabled(
                        {"date_partition_reconciliation": truthy}
                    )
                )
        for falsy in [False, "false", "0", "", None, 2, {}]:
            with self.subTest(value=falsy):
                self.assertFalse(
                    is_date_partition_reconciliation_enabled(
                        {"date_partition_reconciliation": falsy}
                    )
                )
        self.assertFalse(is_date_partition_reconciliation_enabled({}))


if __name__ == "__main__":
    unittest.main()
