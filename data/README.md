# Data Profile

This note remembers how gathered data is / should be used : 

- API / Data selection parameters
- Quirks in source data
- Choice and interpretation
- etc.

Source | Data Table | Remark
--- | --- | ---
BLS | [CPI](http://download.bls.gov/pub/time.series/cu/cu.data.1.AllItems) | Seasonally-Adjusted : series_id = CUSR0000SA0 <br> Seasonally-Unadjusted : series_id = CUUR0000SA0
CenstatD | CPI | Gauge : Class C CPI. Not distorted by government subsidies. <br> Parsing : Re-based to 1.00 at 2011-01-01.
CenstatD | Retail Sales | Interpretation : More cyclical than PCE. Covers consumer goods but not services. 
HKMA | Money Supply T020201 | Gauge : Foreign Currency M2
HKMA | Customer Deposit T030301 | Gauge : HKD Saving + HKD Time
HKMA | HIBOR T060303 | Gauge : 6-Month minus 1-Month Rate Spread
HKMA | Residential Mortgage T0307 | Gauge : Delinquency Ratio <br> Gauge : Undrawn New Loan. Liquidity indicator. <br> Broken : Outstanding Mortgage at 2001 due to more surveyed institutions.
HKMA | Asset Quality of Retail Banks T0306 | Gauge : Special Mention Loans <br> Smoother reference : Substandard Loans
RVD | Private Domestic Price | Gauge : Class C – Hong Kong Island <br> Smoother reference : Class A – Hong Kong Island
RVD | Private Domestic Stock | Broken : Private Domestic Stock from 2003 onward due to exclusion of village house.
RVD | Private Domestic Vacancy | Gauge : Class C
