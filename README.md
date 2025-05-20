1. Written to use Database called Summit_25 and Schema called Asset_health
2. Requires two base tables:  turbo_history_data and turbo_data_production links to .csv files of both found here:  https://sfquickstarts.s3.us-west-1.amazonaws.com/misc/turbo_data_production.csv https://sfquickstarts.s3.us-west-1.amazonaws.com/misc/turbo_history_data.csv
3. After creating the two base tables open SUMMIT_25_PREDICTION_V2.ipynb in a Snowflake notebook and run it.
4. Once the notebook has completed create a new Streamlit app and replace the code with what's in the dashboard.py file.
