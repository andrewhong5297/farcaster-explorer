# Farcaster Explorer

Adding features and metrics to Farcaster users and channels, so that we can come up with stronger labels and queries for use in analytics and applications. See all data in [this Dune dashboard](https://dune.com/ilemi/farcaster-explorer).

>*This repo was created [using this template](https://github.com/duneanalytics/DuneQueryRepo) to [manage your Dune queries](https://dune.mintlify.app/api-reference/crud/endpoint/create) and any [CSVs as Dune tables](https://dune.mintlify.app/api-reference/upload/endpoint/upload).*

### For Contributors

I've set up four types of issues right now:
- `bugs`: This is for data quality issues like miscalculations or broken queries.
- `chart improvements`: This is for suggesting improvements to the visualizations.
- `query improvements`: This is for suggesting improvements to the query itself, such as adding an extra column or table that enhances the results.
- `generic questions`: This is a catch all for other questions or suggestions you may have about the dashboard.

If you want to contribute, either start an issue or go directly into making a PR (using the same labels as above). Once the PR is merged, the queries will get updated in the frontend.

The best contributions you can make right now is adding to the user or channel tier methodology. Currently, those are defined in queries [3418042](https://dune.com/queries/3418402/5739663) and [3418331](https://dune.com/queries/3418331/5739528) respectively. The tiers are as follows:

**User Tiers:**
- **🤖 npc:** Less than 400 followers
- **🥉 active:** 400+ followers, 1+ casts, 100+ engagement score
- **🥈 star:** 1k+ followers, 5+ casts, 5k+ engagement score
- **🥇 influencer:** 10k+ followers, 10+ casts, 25k+ engagement score
- **💎 vip:** 50k+ followers, 10+ casts, 50k+ engagement score

**Channel Tiers:**
- **💤 quiet:** less than 5 casts and less than 50 engagement score
- **🍻 friends:** 5+ casts, 50+ engagement score
- **🔍 niche:** 25+ casts, 5,000+ engagement score, 100+ casters
- **🎭 subculture:** 100+ casts, 25,000+ engagement score, 50+ rising stars and 2+ influencers/vips
- **👑 stadium:** 250+ casts, 100,000+ engagement score, 10+ influencers/vips

Where engagement score is `[likes + recasts*3 + replies*10]`

### Query Management Scripts

You'll need python and pip installed to run the script commands. If you don't have a package manager set up, then use either [conda](https://www.anaconda.com/download) or [poetry](https://python-poetry.org/) . Then install the required packages:

```
pip install -r requirements.txt
```

| Script | Action                                                                                                                                                    | Command |
|---|-----------------------------------------------------------------------------------------------------------------------------------------------------------|---|
| `pull_from_dune.py` | updates/adds queries to your repo based on ids in `queries.yml`                                                                                           | `python scripts/pull_from_dune.py` |
| `push_to_dune.py` | updates queries to Dune based on files in your `/queries` folder                                                                                          | `python scripts/push_to_dune.py` |
| `preview_query.py` | gives you the first 20 rows of results by running a query from your `/queries` folder. Specify the id. This uses Dune API credits | `python scripts/preview_query.py 2615782` |
| `upload_to_dune.py` | uploads/updates any tables from your `/uploads` folder. Must be in CSV format, and under 200MB. | `python scripts/upload_to_dune.py` |