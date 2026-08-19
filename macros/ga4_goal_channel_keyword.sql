{% macro ga4_goal_channel_keyword(source_name, table_name,dash_union_source_name,dash_union_table_name) %}

WITH raw_data as ( SELECT
  JSON_VALUE(data,'$.sessionCampaignName') AS sessionCampaignName,
  JSON_VALUE(data,'$.sessionCampaignName') AS campaign_name,
  PARSE_DATE('%Y%m%d', JSON_VALUE(data, '$.date')) AS date,
  JSON_VALUE(data,'$.keyEvents') AS keyEvents,
  JSON_VALUE(data,'$.sessionSourceMedium') AS sessionSourceMedium,
  JSON_VALUE(data,'$.eventName') AS eventName,
  JSON_VALUE(data,'$.report_start_date') AS report_start_date,
  JSON_VALUE(data,'$.report_end_date') AS report_end_date,
  JSON_VALUE(data,'$.googleAdsKeyword') AS googleAdsKeyWord,
  JSON_VALUE(data,'$.sessionManualAdContent') AS sessionManualAdContent,
      CASE 
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%organic%' THEN 'organic_search'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%direct%' 
              AND JSON_VALUE(data, '$.sessionCampaignName') NOT LIKE 'wat-' THEN 'direct'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%email%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%mailout%' 
              OR (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%automated%' 
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%email%') THEN 'email'
        WHEN (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%facebook%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%instagram%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%social%')
              AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'facebook'
        WHEN JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%google / cpc%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%search%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%sem%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%performance max%'
                  or lower(json_value(data, '$.sessionCampaignName')) like '%pmax%')
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%google / cpc%' 
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%_uow0%'
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%wat-%') THEN 'google_ads_search'
        WHEN (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%googleads%' 
              OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%googleads%' 
              OR LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%native%' 
              OR (LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%demand%' 
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) LIKE '%gen%'))
            OR (JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%google / cpc%' 
                  AND JSON_VALUE(data, '$.sessionCampaignName') LIKE 'wat-%')
            OR (JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%google / cpc%' 
                  AND JSON_VALUE(data, '$.sessionCampaignName') LIKE '_uow%'
                  AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%sem%'
              AND LOWER(JSON_VALUE(data, '$.sessionCampaignName')) NOT LIKE '%search%') THEN 'demand_gen'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%kargo%'  
            AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%referral%' THEN 'kargo'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%linkedin%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'linkedin'
        WHEN ((LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%facebook%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%instagram%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%twitter%' 
              OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%linkedin%')
              AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%cpm%' 
              AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%cpc%')
            OR ((LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%facebook%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%instagram%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%twitter%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%linkedin%')
                  AND JSON_VALUE(data, '$.sessionSourceMedium') LIKE '%referral%') THEN 'own_social'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%referral%' THEN 'referral'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%snapchat%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'snapchat'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%spotify%'  
            AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%referral%' THEN 'spotify'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%stuff%'  
            AND LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) NOT LIKE '%referral%' THEN 'stuff'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%tiktok%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'tiktok'
        WHEN LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%twitter%' 
            AND (LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpm%' 
                  OR LOWER(JSON_VALUE(data, '$.sessionSourceMedium')) LIKE '%cpc%') THEN 'twitter'
        ELSE SPLIT(JSON_VALUE(data, '$.sessionSourceMedium'),'/')[OFFSET(0)]
    END AS site_name,
      ROW_NUMBER() OVER (
      PARTITION BY 
        PARSE_DATE('%Y%m%d', JSON_VALUE(data, '$.date')),
        JSON_VALUE(data, '$.sessionSourceMedium'),
        JSON_VALUE(data, '$.sessionCampaignName'),
        JSON_VALUE(data, '$.eventName'),
        JSON_VALUE(data,'$.googleAdsKeyword'),
        JSON_VALUE(data,'$.sessionManualAdContent')
      ORDER BY _sdc_extracted_at DESC
    ) AS row_num
  FROM {{ source(source_name, table_name) }}
  {% if is_incremental() %}
  WHERE
    _sdc_batched_at >= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY))
    AND PARSE_DATE('%Y%m%d', JSON_VALUE(data, '$.date')) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  {% endif %}
),
deduplicated AS (
  SELECT * FROM raw_data WHERE row_num = 1
),
with_publisher AS (
  SELECT *,
    CASE 
                WHEN LOWER(site_name) like '%own_social%' THEN 'Owned Social'
                WHEN LOWER(site_name) LIKE '%meta%' AND (lower(sessionCampaignName) like '%wat%' or lower(split(sessionCampaignName,'_')[safe_offset(0)]) like '%00%') THEN 'Meta'
                WHEN LOWER(site_name) LIKE '%facebook%' THEN 'Facebook'
                WHEN LOWER(site_name) LIKE '%demand_gen%' THEN 'Demand Gen'
                WHEN LOWER(site_name) LIKE '%organic_search' THEN 'Organic Search'
                WHEN LOWER(sessionSourceMedium) LIKE '%ttd%' THEN 'Ttd'
                WHEN LOWER(site_name) LIKE '%email%' THEN 'Email'
                WHEN LOWER(site_name) LIKE '%tiktok%' AND (lower(sessionCampaignName) like '%wat%' or lower(split(sessionCampaignName,'_')[safe_offset(0)]) like '%00%') THEN 'Tiktok'
                WHEN LOWER(sessionSourceMedium) LIKE '%snapchat%' THEN 'Snapchat'
                WHEN LOWER(sessionSourceMedium) LIKE '%youtube%' OR LOWER(sessionSourceMedium) LIKE '%yt%' THEN 'Youtube'
                WHEN LOWER(sessionSourceMedium) LIKE '%3now%' OR LOWER(sessionSourceMedium) LIKE '%three%' THEN 'Threenow'
                WHEN LOWER(sessionSourceMedium) LIKE '%nzme%' THEN 'Nzme'
                WHEN LOWER(sessionSourceMedium) LIKE '%acast%' THEN 'Acast'
                WHEN (lower(site_name) like '%referral%') THEN 'Referral'
                WHEN (LOWER(sessionCampaignName) LIKE '%perf%' and lower(sessionCampaignName) like '%max%') or lower(sessionCampaignName) like '%pmax%' THEN 'Performance Max'
                WHEN LOWER(site_name) LIKE '%google_ads_search%' THEN 'Search'
                ELSE TRIM(INITCAP(sessionSourceMedium))
            END AS publisher
  FROM deduplicated
),
campaign_base AS (
  SELECT *,
    CASE WHEN ARRAY_LENGTH(SPLIT(campaign_name,'_')) >= 2
      THEN SPLIT(campaign_name,'_')[SAFE_OFFSET(1)]
      ELSE campaign_name
    END AS campaign_name_raw
  FROM with_publisher
),
campaign_name_selection_duplicate AS (
  SELECT COUNT(*) AS indicator, lower(campaign_name_raw) AS lower_campaign
  FROM (SELECT DISTINCT campaign_name_raw FROM campaign_base)
  GROUP BY LOWER(campaign_name_raw)
  HAVING COUNT(*) > 1
),
duplicate_raw AS (
  SELECT DISTINCT campaign_name_raw,
    ROW_NUMBER() OVER (PARTITION BY LOWER(campaign_name_raw) ORDER BY (campaign_name_raw)) AS row_number
  FROM campaign_base cb
  JOIN campaign_name_selection_duplicate cd
    ON LOWER(cb.campaign_name_raw) = LOWER(cd.lower_campaign)
),
deduplicate_raw AS (
  SELECT * FROM duplicate_raw WHERE row_number = 1
),
with_selection AS (
  SELECT camb.* EXCEPT(campaign_name_raw),
    CASE
      WHEN lower(camb.campaign_name_raw) = lower(deduplicate_raw.campaign_name_raw)
        THEN deduplicate_raw.campaign_name_raw
      ELSE camb.campaign_name_raw
    END AS campaign_name_selection
  FROM campaign_base camb
  LEFT JOIN deduplicate_raw
    ON LOWER(deduplicate_raw.campaign_name_raw) = LOWER(camb.campaign_name_raw)
),
semi_final AS (
  SELECT
    COALESCE(t2.present, t1.publisher) AS publisher,
    t1.* EXCEPT(publisher)
  FROM with_selection AS t1
  LEFT JOIN `together-internal.publisher_naming.publisher_naming` AS t2
    ON LOWER(trim(t1.publisher)) = LOWER(trim(t2.publisher))
),
remove_outdated_data AS (
  SELECT * FROM semi_final
  WHERE NOT (
     date between DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) and CURRENT_DATE()
     AND  ABS(DATE_DIFF(DATE(report_end_date), CURRENT_DATE(), DAY)) >=2
  )
),
filtered_creatives as (
  SELECT * except(sessionManualAdContent),
  CASE WHEN LOWER(sessionManualAdContent) IN (
    SELECT DISTINCT LOWER(creative_name) FROM 
    {{ source(dash_union_source_name, dash_union_table_name) }}
  ) 
  
   THEN SPLIT(sessionManualAdContent,'_')[OFFSET(ARRAY_LENGTH(SPLIT(sessionManualAdContent,'_'))-1)]
  else sessionManualAdContent
  end as sessionManualAdContent
  from remove_outdated_data
),
funnel_decleration AS (
  SELECT DISTINCT campaign_name, funnel, media_format, channel
  FROM {{ source(dash_union_source_name, dash_union_table_name) }}
)
SELECT
  filtered_creatives.* EXCEPT(row_num),
  COALESCE(funnel_media_source.funnel, 'OTHER') AS funnel,
  COALESCE(funnel_media_source.media_format, 'OTHER') AS media_format,
  -- Only campaigns present in dash_union get channel; non-overlaps stay empty
  funnel_media_source.channel AS channel
FROM filtered_creatives
LEFT JOIN funnel_decleration AS funnel_media_source
  ON filtered_creatives.campaign_name = funnel_media_source.campaign_name
{% endmacro %}
