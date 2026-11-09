Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
# Impact-CI-ga4_models


## v1.1.0: exclude mapped old Google CPC campaign attribution

The goal/channel and ecommerce transformations exclude rows when the original
`sessionCampaignName` matches `old_campaign_name` in
`together-internal.google_ads_campaign_mapping.campaign_name_mapping` and
`sessionSourceMediumraw` is `google / cpc`. Matching ignores case and surrounding
spaces. The raw source/medium field is used because `sessionSourceMedium` has
already been reduced to the source name. Other sources, unmapped campaigns,
and null values are retained. The shared channel macro covers both goal and
session inputs.

This is a SELECT filter, not a DELETE statement against raw tables. Existing
materializations need a full rebuild to remove matching historical rows.
Kiwibank's custom goal/session models implement the same filter.

Validated by a full rebuild of Kiwibank's goal, session, ecommerce, combined,
and eleven brand-split outputs. No excluded rows remained; non-mapped Google
CPC records and history from January 2024 were retained. Kiwibank's ecommerce
source was empty at validation time.
