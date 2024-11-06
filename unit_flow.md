## Unit Flow Analysis

This analysis uses `UNION` to consolidate metrics for each type of customer activity. The types of metrics calculated are:

- **Beginning units (Beginning)**: 
  - Companies that were active in the previous month.

- **New units (New)**: 
  - Companies with payments in the current month that were not active in the previous month (no record in `units` for `month0`), indicating new acquisitions.

- **Revival units (Revival)**: 
  - Companies that resumed payments after being inactive in prior months, specifically after a period defined by `cohort <= month0`.

- **Channel transfers units (Channel Transfer In / Channel Transfer Out)**: 
  - Companies that changed their channel or subchannel between months. The `d.Channel Xfer in` section counts units that joined a new channel, while `e.Channel Xfer out` counts those leaving a channel. Adding these together would yield a net zero. 

- **Attrition units (Attrition)**: 
  - Companies that were paying in the previous month but are not present in the current month, suggesting they stopped paying.

- **Ending units (Ending)**: 
  - Total count of active companies in the current month, grouped by channel. This would account for everybody who has transacted in a given month. If, for instance, the `company` left the platform mid-month, but transacted in first half, it would be included in the `Ending units` figure. 

Each of these metrics groups data by the previous and current months (`prev_month` and `reporting_month`) and includes the relevant count of paying units. This query provides a comprehensive view of customer activity flows, tracking changes in company statuses, channels, and payment trends over time.
