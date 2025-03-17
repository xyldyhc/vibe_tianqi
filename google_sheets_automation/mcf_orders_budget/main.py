import gspread, pandas as pd
from google.oauth2.service_account import Credentials

# scopes = [
#     'https://www.googleapis.com/auth/spreadsheets',
#     'https://www.googleapis.com/auth/drive'
# ]
# creds = Credentials.from_service_account_file(
#     'editor_credentials.json', scopes=scopes
# )
# client = gspread.authorize(creds)

# equivalent to the above codes
gs_client = gspread.service_account(filename='editor_credentials.json')

spreadsheet_id = '1bwrj3Snv9xzukUjn18aRIx6zxz0_q7SnAAnX7gbHyTI'
workbook = gs_client.open_by_key(spreadsheet_id)
# url = https://docs.google.com/spreadsheets/d/1bwrj3Snv9xzukUjn18aRIx6zxz0_q7SnAAnX7gbHyTI/edit?usp=sharing

# clean the data
fba_shipments_budget_sheet = workbook.worksheet('budget')
fba_shipments_budget_data = fba_shipments_budget_sheet.get('A1:N8')
# delete the second column
fba_shipments_budget_data = [[cell for i, cell in enumerate(row) if i != 1] for row in fba_shipments_budget_data]
fba_shipments_budget_data[0][0] = 'product_name'

if fba_shipments_budget_data:
    fba_shipments_budget_data[0] = [f"{cell[:4]}-{cell[4:]}" if isinstance(cell, str) and len(cell) == 6 else cell for cell in fba_shipments_budget_data[0]]

for row in fba_shipments_budget_data[1:]:
    if row[0] == 'S1 55 Stand':
        row[0] = '55 Stand'
    elif row[0] == 'S1 75 Stand':
        row[0] = '75 Stand'

# unpivot
# 使用第一行作为列名
df_fba_shipments_budget_data = pd.DataFrame(fba_shipments_budget_data[1:], columns=fba_shipments_budget_data[0])
unpivoted_df_fba_shipments_budget_data = df_fba_shipments_budget_data.melt(id_vars=[df_fba_shipments_budget_data.columns[0]], var_name='budget_month', value_name='budget_qty')
unpivoted_df_fba_shipments_budget_data.to_csv('unpivoted_mcf_budget_data.csv', index=False)