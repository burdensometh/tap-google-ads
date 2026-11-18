"""Unit tests for collection-definition fingerprints and state strategies."""
import unittest
from datetime import datetime
from unittest.mock import Mock
from unittest.mock import patch

import singer

from tap_google_ads import collection_definition as cd
from tap_google_ads.streams import ReportStream


def report_catalog(selected_fields, aliases=None):
    """Build a minimal report catalog entry with selected-field metadata."""
    aliases = aliases or {}
    metadata = [
        {"breadcrumb": [], "metadata": {
            "inclusion": "available",
            "forced-replication-method": "INCREMENTAL",
            "valid-replication-keys": ["date"],
            "table-key-properties": ["_sdc_record_hash"],
        }},
        {"breadcrumb": ["properties", "_sdc_record_hash"], "metadata": {
            "inclusion": "automatic", "behavior": "PRIMARY KEY",
        }},
        {"breadcrumb": ["properties", "date"], "metadata": {
            "inclusion": "automatic", "behavior": "SEGMENT",
            "tap-google-ads.api-field-names": ["segments.date"],
        }},
    ]
    for field in selected_fields:
        output_name = aliases.get(field, field.split(".")[-1])
        behavior = "METRIC" if field.startswith("metrics.") else "ATTRIBUTE"
        metadata.append({
            "breadcrumb": ["properties", output_name],
            "metadata": {
                "inclusion": "available",
                "selected": True,
                "behavior": behavior,
                "tap-google-ads.api-field-names": [field],
            },
        })
    return {
        "tap_stream_id": "campaign_performance_report",
        "stream": "campaign_performance_report",
        "metadata": metadata,
        "schema": {},
    }


def report_stream(**kwargs):
    return ReportStream(
        fields=[],
        google_ads_resource_names=["campaign"],
        resource_schema={"campaign": {"fields": {}}},
        primary_keys=["_sdc_record_hash"],
        **kwargs
    )


class TestFingerprint(unittest.TestCase):

    def test_fingerprint_is_stable_across_metadata_ordering(self):
        stream_a = report_catalog(["campaign.id", "metrics.clicks", "segments.device"])
        stream_b = report_catalog(["campaign.id", "metrics.clicks", "segments.device"])
        stream_b["metadata"] = list(reversed(stream_b["metadata"]))
        stream_obj = report_stream()

        first = cd.build_collection_definition(stream_a, stream_obj)
        second = cd.build_collection_definition(stream_b, stream_obj)
        self.assertEqual(cd.compute_fingerprint(first), cd.compute_fingerprint(second))

    def test_unselected_fields_do_not_change_fingerprint(self):
        stream = report_catalog(["campaign.id", "metrics.clicks"])
        other = report_catalog(["campaign.id", "metrics.clicks"])
        other["metadata"].append({
            "breadcrumb": ["properties", "campaign_name"],
            "metadata": {
                "inclusion": "available",
                "selected": False,
                "behavior": "ATTRIBUTE",
                "tap-google-ads.api-field-names": ["campaign.name"],
            },
        })
        stream_obj = report_stream()
        self.assertEqual(
            cd.compute_fingerprint(cd.build_collection_definition(stream, stream_obj)),
            cd.compute_fingerprint(cd.build_collection_definition(other, stream_obj)),
        )


class TestClassification(unittest.TestCase):

    def setUp(self):
        self.stream_obj = report_stream()
        self.base = report_catalog(
            ["campaign.id", "metrics.clicks", "segments.device"]
        )
        self.base_def = cd.build_collection_definition(self.base, self.stream_obj)

    def classify(self, stream):
        current = cd.build_collection_definition(stream, self.stream_obj)
        return cd.classify_collection_definition(self.base_def, current)

    def test_unchanged(self):
        category, _ = self.classify(report_catalog(
            ["campaign.id", "metrics.clicks", "segments.device"]
        ))
        self.assertEqual(category, cd.CATEGORY_UNCHANGED)

    def test_added_metric_only(self):
        category, _ = self.classify(report_catalog(
            ["campaign.id", "metrics.clicks", "metrics.impressions", "segments.device"]
        ))
        self.assertEqual(category, cd.CATEGORY_METRIC_ONLY)

    def test_added_attribute_is_additive(self):
        category, _ = self.classify(report_catalog(
            ["campaign.id", "campaign.name", "metrics.clicks", "segments.device"]
        ))
        self.assertEqual(category, cd.CATEGORY_ADDITIVE)

    def test_removed_report_attribute_is_identity_change(self):
        # For reports identity is the hash over non-metric values, so dropping
        # an attribute changes the identity of previously emitted rows.
        current = cd.build_collection_definition(
            report_catalog(["metrics.clicks", "segments.device"]), self.stream_obj
        )
        category, reason = cd.classify_collection_definition(self.base_def, current)
        self.assertEqual(category, cd.CATEGORY_IDENTITY_CHANGE)
        self.assertIn("identity", reason)

    def test_removed_core_attribute_is_columns_changed(self):
        previous = {
            "stream_kind": cd.CORE_STREAM,
            "resource_names": ["campaign"],
            "primary_keys": ["id"],
            "replication_method": "FULL_TABLE",
            "replication_keys": [],
            "selected_fields": ["campaign.id", "campaign.name", "metrics.clicks"],
            "segments": [],
            "attributes": ["campaign.id", "campaign.name"],
            "metrics": ["metrics.clicks"],
            "aliases": {},
            "identity_fields": ["id"],
        }
        current = dict(previous)
        current["attributes"] = ["campaign.id"]
        current["selected_fields"] = ["campaign.id", "metrics.clicks"]
        category, reason = cd.classify_collection_definition(previous, current)
        self.assertEqual(category, cd.CATEGORY_COLUMNS_CHANGED)
        self.assertIn("removed", reason)

    def test_added_segment_is_segment_change(self):
        category, _ = self.classify(report_catalog(
            ["campaign.id", "metrics.clicks", "segments.device", "segments.ad_network_type"]
        ))
        self.assertEqual(category, cd.CATEGORY_SEGMENT_CHANGE)

    def test_removed_segment_is_segment_change(self):
        current = cd.build_collection_definition(
            report_catalog(["campaign.id", "metrics.clicks"]), self.stream_obj
        )
        category, _ = cd.classify_collection_definition(self.base_def, current)
        self.assertEqual(category, cd.CATEGORY_SEGMENT_CHANGE)

    def test_alias_remap_is_identity_change(self):
        current = cd.build_collection_definition(
            report_catalog(["campaign.id", "metrics.clicks", "segments.device"]),
            self.stream_obj,
        )
        # simulate the tap lifting a field under a different output name
        current["aliases"]["campaign_name"] = ["campaign.name"]
        current["identity_fields"] = sorted(
            set(current["identity_fields"]) | {"campaign_name"}
        )
        previous = dict(self.base_def)
        previous["aliases"] = dict(self.base_def["aliases"])
        previous["aliases"]["campaign_name"] = ["campaign.name"]
        category, reason = cd.classify_collection_definition(previous, current)
        self.assertEqual(category, cd.CATEGORY_UNCHANGED)

        current["aliases"]["campaign_name"] = ["campaign.resource_name"]
        category, reason = cd.classify_collection_definition(previous, current)
        self.assertEqual(category, cd.CATEGORY_IDENTITY_CHANGE)
        self.assertIn("alias", reason)

    def test_resource_remap_is_identity_change(self):
        current = dict(self.base_def)
        current["resource_names"] = ["ad_group"]
        category, _ = cd.classify_collection_definition(self.base_def, current)
        self.assertEqual(category, cd.CATEGORY_IDENTITY_CHANGE)

    def test_missing_previous_is_legacy(self):
        category, _ = cd.classify_collection_definition(None, self.base_def)
        self.assertEqual(category, cd.CATEGORY_LEGACY)


class TestStrategy(unittest.TestCase):

    def test_report_strategies(self):
        self.assertEqual(
            cd.strategy_for(cd.CATEGORY_UNCHANGED, cd.REPORT_STREAM),
            cd.STRATEGY_CONTINUE,
        )
        self.assertEqual(
            cd.strategy_for(cd.CATEGORY_LEGACY, cd.REPORT_STREAM),
            cd.STRATEGY_CONTINUE,
        )
        self.assertEqual(
            cd.strategy_for(cd.CATEGORY_METRIC_ONLY, cd.REPORT_STREAM),
            cd.STRATEGY_CONTINUE,
        )
        self.assertEqual(
            cd.strategy_for(cd.CATEGORY_ADDITIVE, cd.REPORT_STREAM),
            cd.STRATEGY_LIMITED_BACKFILL,
        )
        self.assertEqual(
            cd.strategy_for(cd.CATEGORY_SEGMENT_CHANGE, cd.REPORT_STREAM),
            cd.STRATEGY_NEW_GENERATION,
        )
        self.assertEqual(
            cd.strategy_for(cd.CATEGORY_COLUMNS_CHANGED, cd.REPORT_STREAM),
            cd.STRATEGY_NEW_GENERATION,
        )

    def test_core_column_change_continues(self):
        self.assertEqual(
            cd.strategy_for(cd.CATEGORY_ADDITIVE, cd.CORE_STREAM),
            cd.STRATEGY_CONTINUE,
        )
        self.assertEqual(
            cd.strategy_for(cd.CATEGORY_COLUMNS_CHANGED, cd.CORE_STREAM),
            cd.STRATEGY_CONTINUE,
        )


class TestResolveDefinition(unittest.TestCase):

    def setUp(self):
        self.stream_obj = report_stream()
        self.stream = report_catalog(["campaign.id", "metrics.clicks", "segments.device"])

    def resolve(self, state, stream=None):
        return cd.resolve_collection_definition(state, stream or self.stream, self.stream_obj)

    def test_first_run_records_definition_without_touching_bookmarks(self):
        state = {"bookmarks": {"campaign_performance_report": {"123": {"date": "2022-01-01"}}}}
        definition = self.resolve(state)
        self.assertEqual(definition["category"], cd.CATEGORY_LEGACY)
        self.assertEqual(definition["strategy"], cd.STRATEGY_CONTINUE)
        self.assertEqual(definition["generation"], 1)
        self.assertIn("campaign_performance_report", state["bookmarks"])
        self.assertIn("collection_definitions", state)

    def test_metric_only_change_continues(self):
        state = {}
        self.resolve(state)
        changed = report_catalog(
            ["campaign.id", "metrics.clicks", "metrics.impressions", "segments.device"]
        )
        definition = self.resolve(state, changed)
        self.assertEqual(definition["category"], cd.CATEGORY_METRIC_ONLY)
        self.assertEqual(definition["strategy"], cd.STRATEGY_CONTINUE)
        self.assertEqual(definition["generation"], 1)
        self.assertIsNone(definition["identity_scope"])

    def test_additive_change_requests_limited_backfill(self):
        state = {}
        self.resolve(state)
        changed = report_catalog(
            ["campaign.id", "campaign.name", "metrics.clicks", "segments.device"]
        )
        definition = self.resolve(state, changed)
        self.assertEqual(definition["category"], cd.CATEGORY_ADDITIVE)
        self.assertEqual(definition["strategy"], cd.STRATEGY_LIMITED_BACKFILL)
        self.assertEqual(definition["generation"], 1)

    def test_segment_change_starts_new_generation_and_archives_bookmarks(self):
        state = {
            "bookmarks": {
                "campaign_performance_report": {"123": {"date": "2022-01-24"}}
            }
        }
        self.resolve(state)
        changed = report_catalog(
            ["campaign.id", "metrics.clicks", "segments.device", "segments.ad_network_type"]
        )
        definition = self.resolve(state, changed)

        self.assertEqual(definition["category"], cd.CATEGORY_SEGMENT_CHANGE)
        self.assertEqual(definition["strategy"], cd.STRATEGY_NEW_GENERATION)
        self.assertEqual(definition["generation"], 2)
        self.assertIsNotNone(definition["identity_scope"])
        # Active bookmark is cleared so the next query starts from start_date.
        self.assertNotIn("campaign_performance_report", state["bookmarks"])
        # ...but the old state remains identifiable and migratable.
        history = state["collection_definitions"]["campaign_performance_report"]["history"]
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["bookmarks"]["123"]["date"], "2022-01-24")
        self.assertEqual(history[0]["generation"], 1)
        self.assertIn("segments changed", history[0]["reason"])
        self.assertIsNotNone(history[0]["fingerprint"])

    def test_history_survives_unchanged_subsequent_runs(self):
        state = {"bookmarks": {"campaign_performance_report": {"123": {"date": "2022-01-24"}}}}
        self.resolve(state)
        changed = report_catalog(
            ["campaign.id", "metrics.clicks", "segments.device", "segments.ad_network_type"]
        )
        transition = self.resolve(state, changed)
        self.assertEqual(transition["strategy"], cd.STRATEGY_NEW_GENERATION)
        # The decision stays latched until every customer has been traversed.
        self.assertIn(
            "pending_transition",
            state["collection_definitions"]["campaign_performance_report"],
        )
        cd.complete_stream_transition(state, "campaign_performance_report")

        definition = self.resolve(state, changed)
        self.assertEqual(definition["category"], cd.CATEGORY_UNCHANGED)
        self.assertEqual(definition["generation"], 2)
        self.assertEqual(
            len(state["collection_definitions"]["campaign_performance_report"]["history"]), 1
        )
        self.assertEqual(
            state["collection_definitions"]["campaign_performance_report"]["identity_scope"],
            definition["identity_scope"],
        )

    def test_transition_is_latched_for_later_customers(self):
        state = {"bookmarks": {"campaign_performance_report": {
            "111": {"date": "2022-01-24"},
            "222": {"date": "2022-01-20"},
        }}}
        self.resolve(state)
        changed = report_catalog(
            ["campaign.id", "metrics.clicks", "segments.device", "segments.ad_network_type"]
        )

        first_customer = self.resolve(state, changed)
        second_customer = self.resolve(state, changed)

        # The second customer must still see the transition; otherwise it
        # would silently resume from a bookmark written under the old grain.
        self.assertEqual(first_customer["strategy"], cd.STRATEGY_NEW_GENERATION)
        self.assertEqual(second_customer["strategy"], cd.STRATEGY_NEW_GENERATION)
        self.assertEqual(second_customer["generation"], 2)
        self.assertEqual(
            first_customer["identity_scope"], second_customer["identity_scope"]
        )
        self.assertNotIn("campaign_performance_report", state["bookmarks"])

    def test_incompatible_definition_does_not_reuse_other_customers_bookmarks(self):
        state = {
            "bookmarks": {
                "campaign_performance_report": {
                    "111": {"date": "2022-01-24"},
                    "222": {"date": "2022-01-20"},
                }
            }
        }
        self.resolve(state)
        changed = report_catalog(
            ["campaign.id", "metrics.clicks", "segments.ad_network_type"]
        )
        self.resolve(state, changed)
        self.assertNotIn("campaign_performance_report", state["bookmarks"])


class TestReportSyncUsesDefinition(unittest.TestCase):
    """The resolved strategy must actually change the query start point."""

    @staticmethod
    def get_queries(fake_make_request):
        return [call.args[1] for call in fake_make_request.call_args_list]

    @staticmethod
    def run_sync(stream, config, state, fake_make_request, customer_id="123"):
        stream_obj = report_stream()
        stream_obj.sync(
            Mock(),
            {"customerId": customer_id, "loginCustomerId": "456"},
            stream,
            config,
            state,
            None,
        )

    @patch("tap_google_ads.streams.make_request")
    def test_segment_change_restarts_at_start_date(self, fake_make_request):
        base = report_catalog(["campaign.id", "metrics.clicks", "segments.device"])
        state = {"bookmarks": {"campaign_performance_report": {"123": {"date": "2022-03-01T00:00:00.000000Z"}}}}
        config = {"start_date": "2022-02-01T00:00:00Z", "end_date": "2022-03-03T00:00:00Z"}

        # Seed generation 1.
        first_state = {
            "bookmarks": {"campaign_performance_report": {"123": {"date": "2022-03-01T00:00:00.000000Z"}}}
        }
        self.run_sync(base, config, first_state, fake_make_request)

        fake_make_request.reset_mock()
        changed = report_catalog(["campaign.id", "metrics.clicks", "segments.ad_network_type"])
        self.run_sync(changed, config, first_state, fake_make_request)

        queries = self.get_queries(fake_make_request)
        self.assertTrue(queries)
        self.assertIn("2022-02-01", queries[0])
        self.assertNotIn("2022-03-01", queries[0])

    @patch("tap_google_ads.streams.make_request")
    def test_metric_only_change_keeps_bookmark_start(self, fake_make_request):
        base = report_catalog(["campaign.id", "metrics.clicks", "segments.device"])
        config = {"start_date": "2022-02-01T00:00:00Z", "end_date": "2022-03-03T00:00:00Z"}
        state = {}

        self.run_sync(base, config, state, fake_make_request)
        state["bookmarks"] = {"campaign_performance_report": {"123": {"date": "2022-03-01T00:00:00.000000Z"}}}

        fake_make_request.reset_mock()
        changed = report_catalog(
            ["campaign.id", "metrics.clicks", "metrics.impressions", "segments.device"]
        )
        self.run_sync(changed, config, state, fake_make_request)
        queries = self.get_queries(fake_make_request)
        self.assertTrue(queries)
        self.assertIn("2022-03-01", queries[0])

    @patch("tap_google_ads.streams.make_request")
    def test_additive_change_rewinds_bounded_window(self, fake_make_request):
        base = report_catalog(["campaign.id", "metrics.clicks", "segments.device"])
        config = {
            "start_date": "2022-01-01T00:00:00Z",
            "end_date": "2022-03-05T00:00:00Z",
            "definition_backfill_days": 3,
        }
        state = {}
        self.run_sync(base, config, state, fake_make_request)
        state["bookmarks"] = {"campaign_performance_report": {"123": {"date": "2022-03-01T00:00:00.000000Z"}}}

        fake_make_request.reset_mock()
        changed = report_catalog(["campaign.id", "campaign.name", "metrics.clicks", "segments.device"])
        self.run_sync(changed, config, state, fake_make_request)

        queries = self.get_queries(fake_make_request)
        self.assertTrue(queries)
        # 2022-03-01 minus 3 days == 2022-02-26, not start_date.
        self.assertIn("2022-02-26", queries[0])

    def test_identity_scope_changes_record_hash(self):
        from tap_google_ads.streams import generate_hash

        metadata = [
            {"breadcrumb": [], "metadata": {}},
            {"breadcrumb": ["properties", "campaign_id"], "metadata": {"behavior": "ATTRIBUTE"}},
            {"breadcrumb": ["properties", "clicks"], "metadata": {"behavior": "METRIC"}},
        ]
        record = {"campaign_id": 1, "clicks": 5}
        unscoped = generate_hash(record, metadata)
        scoped = generate_hash(record, metadata, "abc123")
        self.assertNotEqual(unscoped, scoped)
        self.assertEqual(scoped, generate_hash(record, metadata, "abc123"))

    @patch("tap_google_ads.streams.make_request")
    def test_resume_of_partially_migrated_stream_keeps_progress(self, fake_make_request):
        """An interrupted generation change must not restart migrated customers.

        Customer 111 finished under the new definition; customer 123 did not.
        On resume, 111 keeps its progress while 123 restarts from start_date.
        """
        base = report_catalog(["campaign.id", "metrics.clicks", "segments.device"])
        config = {"start_date": "2022-02-01T00:00:00Z", "end_date": "2022-03-03T00:00:00Z"}
        state = {}
        self.run_sync(base, config, state, fake_make_request)

        changed = report_catalog(
            ["campaign.id", "metrics.clicks", "segments.ad_network_type"]
        )
        # Start the generation change; the stream does not complete.
        self.run_sync(changed, config, state, fake_make_request)

        definition = state["collection_definitions"]["campaign_performance_report"]
        self.assertTrue(definition.get("pending_transition"))

        # Simulate an interruption where only customer 111 restarted into the
        # new generation; 123 still holds a stale pre-change bookmark.
        definition["customer_definitions"] = {
            "111": [definition["generation"], definition["fingerprint"]],
        }
        state["bookmarks"]["campaign_performance_report"] = {
            "111": {"date": "2022-02-20T00:00:00.000000Z"},
            "123": {"date": "2022-02-28T00:00:00.000000Z"},
        }

        # Resume as the not-yet-migrated customer: restart from start_date.
        fake_make_request.reset_mock()
        self.run_sync(changed, config, state, fake_make_request, customer_id="123")
        queries = self.get_queries(fake_make_request)
        self.assertIn("2022-02-01", queries[0])
        self.assertNotIn("2022-02-28", queries[0])

        # Resume as the customer that already migrated: keep its progress.
        fake_make_request.reset_mock()
        self.run_sync(changed, config, state, fake_make_request, customer_id="111")
        queries = self.get_queries(fake_make_request)
        self.assertIn("2022-02-20", queries[0])
        self.assertNotIn("2022-02-01", queries[0])


if __name__ == "__main__":
    unittest.main()
