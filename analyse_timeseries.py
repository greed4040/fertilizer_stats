import pandas as pd
import numpy as np
from statsmodels.tsa.vector_ar.var_model import VAR
from statsmodels.tsa.stattools import adfuller

from pandas import read_csv

# Load or prepare your data (assumed as a pandas DataFrame)
data = read_csv('table_output.css', header=0, parse_dates=[0], index_col=0)

print(data)
print("#", data['VALE'].isna().sum())
print("#", data['IPI'].isna().sum())
#data_no_zeros = data.loc[~(data == 0).any(axis=1)]

selected_columns = ['VALE', 'IPI']
new_df = data[selected_columns]

print(new_df.head())
print("="*20)


new_df=new_df.dropna()
print(new_df.tail())
print("tiail", "="*20)

# Assuming your original data is in a pandas DataFrame called 'data'
data_pct_change = new_df.pct_change().dropna()
data_pct_change = data_pct_change * 100

print(data_pct_change)
print("# isna().sum()", data_pct_change['VALE'].isna().sum())
print("# isna().sum()", data_pct_change['IPI'].isna().sum())


# Check for stationarity and apply differencing if necessary
def test_stationarity(timeseries):
    adf_test = adfuller(timeseries)
    if adf_test[1] <= 0.05:
        return True
    else:
        return False

if not test_stationarity(data_pct_change['VALE']):
    data_pct_change['Series1_diff'] = data_pct_change['VALE'].diff().dropna()
if not test_stationarity(data_pct_change['IPI']):
    data_pct_change['Series2_diff'] = data_pct_change['IPI'].diff().dropna()

#data_no_zeros = data.dropna()

# Determine the optimal lag order
model = VAR(data_pct_change)
lag_order_results = model.select_order(maxlags=10)
optimal_lag_order = lag_order_results.selected_orders['aic']

# Fit the VAR model with the optimal lag order
var_model = model.fit(optimal_lag_order)


from statsmodels.tsa.vector_ar.irf import IRAnalysis

# Assuming your fitted VAR model is named 'var_model'
irf = IRAnalysis(var_model)
irf.plot(orth=False)  # Set orth=True for orthogonalized impulse responses

fevd = var_model.fevd(10)  # Set the number of periods you want to compute FEVD for
fevd.plot()

# Assuming your fitted VAR model is named 'var_model'
granger_causality = var_model.test_causality('VALE', 'IPI', kind='f')  # Replace variable names
print(granger_causality.summary())