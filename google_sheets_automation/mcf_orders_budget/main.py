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

spreadsheet_id = '1K7DJSUB5-QAvtKE8bMWm3mOpuR9JWX3PKrNKk5lnEdw'
workbook = gs_client.open_by_key(spreadsheet_id)

# value_list = sheet.sheet1.row_values(1)
# print(value_list)

# clean the data
mcf_budget_sheet = workbook.worksheet('budget')
mcf_budget_data = mcf_budget_sheet.get('A1:N8')
# delete the second column
mcf_budget_data = [[cell for i, cell in enumerate(row) if i != 1] for row in mcf_budget_data]
mcf_budget_data[0][0] = 'product_name'

if mcf_budget_data:
    mcf_budget_data[0] = [f"{cell[:4]}-{cell[4:]}" if isinstance(cell, str) and len(cell) == 6 else cell for cell in mcf_budget_data[0]]

for row in mcf_budget_data[1:]:
    if row[0] == 'S1 55 Stand':
        row[0] = '55 Stand'
    elif row[0] == 'S1 75 Stand':
        row[0] = '75 Stand'

# unpivot
# 使用第一行作为列名
df_mcf_budget_data = pd.DataFrame(mcf_budget_data[1:], columns=mcf_budget_data[0])
unpivoted_df_mcf_budget_data = df_mcf_budget_data.melt(id_vars=[df_mcf_budget_data.columns[0]], var_name='budget_month', value_name='budget_qty')
print(unpivoted_df_mcf_budget_data)