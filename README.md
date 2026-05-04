# Meta Ads Insights Puller

Small Python CLI to pull Meta Ads Insights from the Marketing API and write them to CSV.

## 1. Create credentials

You need:

- A Meta app with Marketing API access.
- A user/system-user access token with access to the ad account.
- Common permissions: `ads_read`; depending on your business setup, `business_management` may also be needed.
- Your ad account ID. Use the number only, not `act_`.

## 2. Configure

```bash
cd meta-dashboard
cp .env.example .env
```

Edit `.env`:

```bash
META_ACCESS_TOKEN=your_token
META_AD_ACCOUNT_ID=your_ad_account_id_without_act_prefix
META_API_VERSION=v25.0
```

## 3. Install dependency

```bash
python3 -m pip install -r requirements.txt
```

## 4. Pull insights

First verify your token and find the ad account ID:

```bash
python3 list_ad_accounts.py
```

Last 7 days at campaign level:

```bash
python3 fetch_meta_insights.py --date-preset last_7d --level campaign
```

Specific dates at ad level:

```bash
python3 fetch_meta_insights.py \
  --since 2026-04-01 \
  --until 2026-04-28 \
  --level ad \
  --output insights_april.csv
```

Daily rows:

```bash
python3 fetch_meta_insights.py --date-preset last_30d --time-increment 1
```

With breakdowns:

```bash
python3 fetch_meta_insights.py --date-preset last_30d --breakdowns age,gender
```

## Useful fields

Default fields:

```text
date_start,date_stop,account_id,account_name,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,impressions,reach,clicks,spend,cpc,cpm,ctr,actions,action_values
```

Override them:

```bash
python3 fetch_meta_insights.py \
  --date-preset yesterday \
  --fields date_start,date_stop,campaign_name,spend,impressions,clicks,conversions
```

## Notes

- Large pulls can take time and may hit Meta API limits. Start with a small date range.
- Some metrics may be unavailable depending on account, objective, attribution setting, or API version.
- If Meta returns a permission error, verify the token in Graph API Explorer and confirm the user/system user has access to the ad account.
