# tap_google_ads

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the Google Ads API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap: 

- Pulls data from the [Google Ads API](https://developers.google.com/google-ads/api/docs/start).
- Extracts the following resources from Google Ads
  - [Accessible Bidding Strategies](https://developers.google.com/google-ads/api/reference/rpc/v10/AccessibleBiddingStrategy)
  - [Accounts](https://developers.google.com/google-ads/api/reference/rpc/v10/Customer)
  - [Ad Groups](https://developers.google.com/google-ads/api/reference/rpc/v10/AdGroup)
  - [Ads](https://developers.google.com/google-ads/api/reference/rpc/v10/Ad)
  - [Assets](https://developers.google.com/google-ads/api/reference/rpc/v20/Asset)
  - [Bidding Strategies](https://developers.google.com/google-ads/api/reference/rpc/v10/BiddingStrategy)
  - [Call Details](https://developers.google.com/google-ads/api/reference/rpc/v10/CallView)
  - [Campaigns](https://developers.google.com/google-ads/api/reference/rpc/v10/Campaign)
  - [Campaign Budgets](https://developers.google.com/google-ads/api/reference/rpc/v10/CampaignBudget)
  - [Campaign Labels](https://developers.google.com/google-ads/api/reference/rpc/v10/CampaignLabel)
  - [Labels](https://developers.google.com/google-ads/api/reference/rpc/v10/Label)
  - [Reporting](https://developers.google.com/google-ads/api/docs/reporting/overview)
    - [Account Performance Report](https://developers.google.com/google-ads/api/fields/v10/customer)
    - [Ad Group Performance Report](https://developers.google.com/google-ads/api/fields/v10/ad_group)
    - [Ad Group Audience Performance Report](https://developers.google.com/google-ads/api/fields/v10/ad_group_audience_view)
    - [Ad Performance Report](https://developers.google.com/google-ads/api/fields/v10/ad_group_ad)
    - [Age Range Performance Report](https://developers.google.com/google-ads/api/fields/v10/age_range_view)
    - [Campaign Performance Report](https://developers.google.com/google-ads/api/fields/v10/campaign)
    - [Campaign Audience Performance Report](https://developers.google.com/google-ads/api/fields/v10/campaign_audience_view)
    - [Call Metrics Call Details Report](https://developers.google.com/google-ads/api/fields/v10/call_view)
    - [Click Performance Report](https://developers.google.com/google-ads/api/fields/v10/click_view)
    - [Display Keyword Performance Report](https://developers.google.com/google-ads/api/fields/v10/display_keyword_view)
    - [Display Topics Performance Report](https://developers.google.com/google-ads/api/fields/v10/topic_view)
    - [Expanded Landing Page Report](https://developers.google.com/google-ads/api/fields/v10/expanded_landing_page_view)
    - [Gender Performance Report](https://developers.google.com/google-ads/api/fields/v10/gender_view)
    - [Geo Performance Report](https://developers.google.com/google-ads/api/fields/v10/geographic_view)
    - [Keywordless Query Report](https://developers.google.com/google-ads/api/fields/v10/dynamic_search_ads_search_term_view)
    - [Keywords Performance Report](https://developers.google.com/google-ads/api/fields/v10/keyword_view)
    - [Landing Page Report](https://developers.google.com/google-ads/api/fields/v10/landing_page_view)
    - [Placeholder Feed Item Report](https://developers.google.com/google-ads/api/fields/v10/feed_item)
    - [Placeholder Report](https://developers.google.com/google-ads/api/fields/v10/feed_placeholder_view)
    - [Placement Performance Report](https://developers.google.com/google-ads/api/fields/v10/managed_placement_view)
    - [Search Query Performance Report](https://developers.google.com/google-ads/api/fields/v10/search_term_view)
    - [Shopping Performance Report](https://developers.google.com/google-ads/api/fields/v10/shopping_performance_view)
    - [User Location Performance Report](https://developers.google.com/google-ads/api/fields/v10/user_location_view)
    - [UserLocation Performance Report](https://developers.google.com/google-ads/api/fields/v10/user_location_view)
    - [Video Performance Report](https://developers.google.com/google-ads/api/fields/v10/video)

## Bookmarking Strategy

The Google Ads API supports the `start_date` and `end_date` parameters that limits the records which filters the analytics records in the given time period.

### Collection-definition fingerprints

Changing the selected fields, field aliases, or resource mapping can change what a bookmark means.  Without a guard, a bookmark written under an old selection would silently skip the data the new selection has not yet collected.  Each stream therefore stores a fingerprint of its *collection definition* in `state["collection_definitions"][<stream>]`:

```json
{
  "collection_definitions": {
    "campaign_performance_report": {
      "fingerprint": "…",
      "identity_fingerprint": "…",
      "components": {
        "stream_kind": "report",
        "resource_names": ["campaign"],
        "primary_keys": ["_sdc_record_hash"],
        "segments": ["segments.date", "segments.device"],
        "attributes": ["campaign.id"],
        "metrics": ["metrics.clicks"],
        "aliases": {"campaign_id": ["campaign.id"]},
        "identity_fields": ["campaign_id", "date", "device"]
      },
      "category": "ADDITIVE",
      "strategy": "limited_backfill",
      "generation": 1,
      "pending_transition": { "…": "…" }
    }
  }
}
```

Every transition is classified and mapped to an explicit strategy:

| Classification | Meaning | Strategy |
| --- | --- | --- |
| `UNCHANGED` | Definition is identical | `continue` |
| `LEGACY` | No fingerprint stored yet (upgrading the tap) | `continue` — the fingerprint is adopted and progress is preserved |
| `METRIC_ONLY` | Only metrics were added/removed | `continue` — metrics never affect grain or record identity |
| `ADDITIVE` | Non-segment attribute columns were added | `limited_backfill` — re-read `definition_backfill_days` (default 30) before the bookmark |
| `SEGMENT_CHANGE` | The selected segments changed (the grain changed) | `new_generation` |
| `IDENTITY_CHANGE` | Aliases, resource mapping, primary keys, or report attributes changed | `new_generation` |
| `COLUMNS_CHANGED` | Core-stream attributes were removed (primary-key cursor is unaffected) | `continue` |

A `new_generation` never silently reuses an incompatible bookmark: the stream's active bookmarks are archived (not deleted) under `collection_definitions.<stream>.history`, the query restarts at `start_date`, and `identity_scope` is folded into `_sdc_record_hash` so the new generation's rows are distinguishable from the old generation's rows.

The decision is latched in `pending_transition` for the entire customer loop and cleared only when the stream completes, so an interrupted sync applies the same strategy to customers it has not reached yet.  Per-customer progress is tracked in `collection_definitions.<stream>.customer_definitions`, so a customer that already restarted into the new generation keeps its progress when the sync resumes, while a customer still on the old bookmark is rewound.  Core (FULL_TABLE) streams are unaffected by column changes for their primary-key cursor and therefore always continue.

`definition_backfill_days` can be set in the config to control the bounded re-read window.

## Configuration

This tap requires a `config.json` which specifies details regarding [OAuth 2.0](https://developers.google.com/google-ads/api/docs/oauth/overview) authentication and a cutoff date for syncing historical data. See [config.sample.json](config.sample.json) for an example.

To run the discover mode of `tap-google-ads` with the configuration file, use this command:

```bash
$ tap-google-ads -c my-config.json -d
```

To run the sync mode of `tap-google-ads` with the catalog file, use the command:

```bash
$ tap-google-ads -c my-config.json --catalog catalog.json
```
---

Copyright &copy; 2021 Stitch
