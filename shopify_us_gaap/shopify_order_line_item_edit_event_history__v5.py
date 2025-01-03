#!/usr/bin/env python
# coding: utf-8


# 1
# data cleaning

import os
import pandas as pd

default_sheet_name = 'sheet1'

#以下这些读取的excel文件在后面把tag文件写成table之后需要join tag然后筛选出if_processed = false的那些记录

df_line_item_added_raw = pd.read_excel(
    'physical_product_added.xlsx',
    sheet_name = default_sheet_name,
    dtype={'order_id': str,
           'order_number': str,
           'is_valid': bool,
           'line_item_id': str,
           'quantity_added': int,
           'line_item_idx': int,
           'product_id': str,
           'variant_id': str,
           'taxable': bool,
           'quantity_added_for_each_line_item': int,
           #quantity_added_for_each_line_item没必要的话可以删去
           'line_item_unit_idx': int,
           'physical_product_unit_idx': int},
    parse_dates=['order_created_at_pdt', 'due_date', 'event_happened_at_pdt']
)
df_line_item_added_drop = df_line_item_added_raw.drop(columns=[ 'benchmark', 'generated_number'])
# split payment不传invoice
df_line_item_added_drop = df_line_item_added_drop[~df_line_item_added_drop['line_item_name'].str.contains('Split', case=False, na=False)]

# 选择product_name为空值并且line_item_name中不包含Extend Protection Plan字符串的行 + product_name中包含Extra|Remaining Balance的行
# 他们都作为custom product处理，跟着这个订单的第一个发货记录走
df_custom_product_added = df_line_item_added_drop[
    (df_line_item_added_drop['product_name'].isna() &
        ~df_line_item_added_drop['line_item_name'].str.contains('Extend Protection Plan', case=False, na=False)) |
    df_line_item_added_drop['product_name'].str.contains('Extra|Remaining Balance', case=False, na=False)
]
df_custom_product_added.loc[:, 'line_type'] = 'CUSTOM_PRODUCT'

df_physical_product_added = df_line_item_added_drop[df_line_item_added_drop['product_name'].notna()]
df_physical_product_added = df_physical_product_added[~df_physical_product_added['product_name'].str.contains('Warranty|Extra|Remaining Balance', case=False, na=False)]

# 选择product_name为空值并且line_item_name中包含Extend Protection Plan字符串的行 + product_name中包含Warranty的行
# 他们都作为warranty处理，首先去匹配
df_warranty_added = df_line_item_added_drop[
    df_line_item_added_drop['product_name'].str.contains('Warranty', case=False, na=False) |
    (df_line_item_added_drop['product_name'].isna() &
     df_line_item_added_drop['line_item_name'].str.contains('Extend Protection Plan', case=False, na=False))
]
df_warranty_added.loc[:, 'line_type'] = 'WARRANTY'




df_line_item_removed_raw = pd.read_excel(
    'expand_line_item_removed.xlsx',
    sheet_name=default_sheet_name,
    dtype={'order_id': str,
           'order_number': str,
           'is_valid': bool,
           'line_item_id': str,
           'quantity_removed': int,
           'line_item_idx': int,
           'product_id': str,
           'variant_id': str,
           'quantity_removed_for_each_line_item': int,
           #quantity_removed_for_each_line_item没必要的话可以删去
           'line_item_removed_unit_idx': int},
    parse_dates=['order_created_at_pdt', 'event_happened_at_pdt']
)
df_line_item_removed_drop = df_line_item_removed_raw.drop(columns=['benchmark', 'generated_number'])
# split payment不传invoice
df_line_item_removed_drop = df_line_item_removed_drop[~df_line_item_removed_drop['line_item_name'].str.contains('Split', case=False, na=False)]

df_custom_product_removed = df_line_item_removed_drop[
    (df_line_item_removed_drop['product_name'].isna() &
        ~df_line_item_removed_drop['line_item_name'].str.contains('Extend Protection Plan', case=False, na=False)) |
    df_line_item_removed_drop['product_name'].str.contains('Extra|Remaining Balance', case=False, na=False)
]
df_custom_product_removed.loc[:, 'line_type'] = 'CUSTOM_PRODUCT'

df_physical_product_removed = df_line_item_removed_drop[df_line_item_removed_drop['product_name'].notna()]
df_physical_product_removed = df_physical_product_removed[~df_physical_product_removed['product_name'].str.contains('Warranty|Extra|Remaining Balance', case=False, na=True)]

df_warranty_removed = df_line_item_removed_drop[
    df_line_item_removed_drop['product_name'].str.contains('Warranty', case=False, na=False) |
    (df_line_item_removed_drop['product_name'].isna() &
     df_line_item_removed_drop['line_item_name'].str.contains('Extend Protection Plan', case=False, na=False))
]
df_warranty_removed.loc[:, 'line_type'] = 'WARRANTY'




df_shipment = pd.read_excel(
    'physical_product_shipment.xlsx',
    sheet_name=default_sheet_name,
    dtype={'order_id': str,
           'order_number': str,
           'is_valid': bool,
           'log_v2_order_id': str,
           'package_id': str,
           'physical_product_fulfilled_unit_idx': int},
    parse_dates=['order_created_at_pdt', 'due_date', 'pkg_created_at_pdt', 'fulfilled_date_fixed']
)
#shipment的unique_identifier会因为fulfilled date的修改而发生变化，进而影响invoice date，尤其要小心跨月的情况

#出现当天order的东西当天发货的情况时，将shipment的时间标注为23：59：59，避免出现pre-shipped的情况
df_shipment['fulfilled_date_fixed'] = df_shipment['fulfilled_date_fixed'].apply(lambda x: x.replace(hour=23, minute=59, second=59))
df_shipment = df_shipment.rename(columns={'fulfilled_date_fixed': 'event_happened_at_pdt'})




df_line_item_discount = pd.read_excel(
    'line_item_discount.xlsx',
    sheet_name=default_sheet_name,
    dtype={'order_id': str,
           'product_id': str,
           'variant_id': str,
           'line_item_id': str,
           'order_discount_application_idx': int}
)
#discount不需要tag文件,直接读取即可



df_shipping_line = pd.read_excel(
    'shopify_shipping_line.xlsx',
    sheet_name=default_sheet_name,
    dtype={'order_id': str,
           'is_valid': bool,
           'agreement_idx': int,
           'agreement_id': str,
           'agreement_sales_idx': int},
    parse_dates=['event_happened_at_pdt', 'event_happened_date_pdt']
)

df_quickbooks_products = pd.read_excel(
    'dim_quickbooks_product.xlsx',
    sheet_name=default_sheet_name,
    dtype={'has_ref': bool,
           'is_ref': bool,
           'sales_price_in_usd': int,
           'cost_in_usd': int}
)


#遍历的all_events由shipment、refund构成，不再将order作为event纳入
df_all_events = pd.concat([df_shipment, df_physical_product_removed, df_custom_product_removed, df_warranty_removed], axis=0, ignore_index=True)


# 2
# for test 测试订单
# test_orders = ['SHO.1109', 'SHO.7307', 'SHO.13117', 'SHO.14244', 'SHO.18067', 'SHO.18078', 'SHO.17441']

# df_physical_product_added = df_physical_product_added[df_physical_product_added['order_name'].isin(test_orders)].reset_index(drop=True)
# df_physical_product_removed = df_physical_product_removed[df_physical_product_removed['order_name'].isin(test_orders)].reset_index(drop=True)
# df_shipment = df_shipment[df_shipment['order_name'].isin(test_orders)].reset_index(drop=True)
# df_all_events = df_all_events[df_all_events['order_name'].isin(test_orders)].reset_index(drop=True)


# 3
# 创建tag文件来记录每一行的处理情况,并找到本次需要处理的记录有哪些（去除已处理的记录）
# 如果这个文件已经存在就不再新建

# if_processed用来标记这一条记录是否在for循环中被读取处理过，在for循环的哪一步标记为processed呢？
# 以订单为单位标记为processed

def create_or_load_file(file_path, file_columns):
    
    if os.path.exists(file_path):
        return pd.read_excel(file_path)
    else:
        df_new = pd.DataFrame(columns=file_columns)
        df_new.to_excel(file_path, index=False)
        return df_new

df_physical_product_added_tag = create_or_load_file(
    'physical_product_added_tag.xlsx', 
    ['unique_identifier', 'if_shipped', 'shipment_unique_identifier', 'if_refunded', 'refund_unique_identifier']
)

df_physical_product_removed_tag = create_or_load_file(
    'physical_product_removed_tag.xlsx',
    ['unique_identifier', 'if_processed', 'if_assigned', 'order_unique_identifier']
)

df_custom_product_added_tag = create_or_load_file(
    'custom_product_added_tag.xlsx', 
    ['unique_identifier', 'if_shipped', 'shipment_unique_identifier', 'if_refunded', 'refund_unique_identifier']
)

df_custom_product_removed_tag = create_or_load_file(
    'custom_product_removed_tag.xlsx',
    ['unique_identifier', 'if_processed', 'if_assigned', 'order_unique_identifier']
)

df_warranty_added_tag = create_or_load_file(
    'warranty_added_tag.xlsx', 
    ['unique_identifier', 'if_shipped', 'shipment_unique_identifier', 'if_refunded', 'refund_unique_identifier']
)

df_warranty_removed_tag = create_or_load_file(
    'warranty_removed_tag.xlsx',
    ['unique_identifier', 'if_processed', 'if_assigned', 'order_unique_identifier']
)

df_shipment_tag = create_or_load_file(
    'shipment_tag.xlsx', 
    ['unique_identifier', 'if_processed', 'if_assigned', 'order_unique_identifier']
)

df_shipping_line_tag = create_or_load_file(
    'shipment_tag.xlsx', 
    ['unique_identifier', 'if_assigned', 'shipment_unique_identifier']
)

df_invoice = create_or_load_file(
    'invoice.xlsx', 
    ['order_name', 'order_id', 'order_created_at_pdt', 'transaction_type', 'transaction_name', 'line_type', 'store', 'transaction_date', 'ship_from',
     'shipping_date', 'ship_via', 'tracking_number', 'payment_terms', 'due_date', 'customer_name', 'customer_email', 'customer_phone_number',
     'shipping_country', 'shipping_province', 'shipping_city', 'shipping_zip', 'shipping_address', 'billing_country','billing_province',
     'billing_city', 'billing_zip', 'billing_address', 'transaction_product_name', 'qty', 'rate', 'amount', 'taxable', 'discount',
     'discount_reallocation_target', 'shipping', 'unique_identifier', 'if_sent'
    ]
)
# 不同的line_type会对应不同的unique_identifier
# PRODUCT对应shipment_unique_identifier(还是order_unique_identifier呢？保持df_invoice里仍然是unique的值)，CUSTOM_PRODUCT对应order_unique_identifier, WARRANTY对应order_unique_identifier，SHIPPING对应shipment_unique_identifier

df_credit_memo = create_or_load_file(
    'credit_memo.xlsx', 
    ['order_name', 'order_id', 'order_created_at_pdt', 'transaction_type', 'transaction_name', 'line_type', 'store', 'transaction_date', 'ship_from',
     'customer_name', 'customer_email', 'customer_phone_number', 'billing_country','billing_province', 'billing_city', 'billing_zip', 'billing_address',
     'transaction_product_name', 'qty', 'rate', 'amount', 'taxable', 'discount','discount_reallocation_target', 'unique_identifier', 'if_sent'
    ]
)

df_journal_entry = create_or_load_file(
    'journal_entry.xlsx', 
    ['transaction_type', 'currency', 'transaction_name', 'transaction_date', 'account', 'debits', 'credits', 'description', 'name', 'store', 'unique_identifier', 'if_sent']
)

df_physical_product_removed_tag_temp = df_physical_product_removed_tag[['unique_identifier', 'if_processed']]
df_custom_product_removed_tag_temp = df_custom_product_removed_tag[['unique_identifier', 'if_processed']]
df_warranty_removed_tag_temp = df_warranty_removed_tag[['unique_identifier', 'if_processed']]
df_shipment_tag_temp = df_shipment_tag[['unique_identifier', 'if_processed']]

df_processed_events = pd.concat([df_physical_product_removed_tag_temp, df_custom_product_removed_tag_temp, df_warranty_removed_tag_temp, df_shipment_tag_temp], ignore_index=True)
df_processed_events = df_processed_events[df_processed_events['if_processed'] == True]

df_unprocessed_events = df_all_events[~df_all_events['unique_identifier'].isin(df_processed_events['unique_identifier'])]

df_unprocessed_orders = df_unprocessed_events[['order_name']].drop_duplicates().reset_index(drop=True)
df_unprocessed_orders_sorted = df_unprocessed_orders.sort_values(by='order_name', ascending=True)


# 4
# 主体逻辑

def mark_tag(df_tag, row, tag_column, related_unique_identifier_column=None, related_unique_identifier_row=None):
    if related_unique_identifier_column == None:
        if row['unique_identifier'] in df_tag['unique_identifier'].values:
            df_tag.loc[
                df_tag['unique_identifier'] == row['unique_identifier'],
                tag_column
            ] = True
        else:
            new_row = pd.DataFrame({
                'unique_identifier': [row['unique_identifier']],
                tag_column: [True]
            })
            df_tag = pd.concat([df_tag, new_row], ignore_index=True)
    else:
        if row['unique_identifier'] in df_tag['unique_identifier'].values:
            df_tag.loc[
                df_tag['unique_identifier'] == row['unique_identifier'],
                [tag_column, related_unique_identifier_column]
            ] = [True, related_unique_identifier_row['unique_identifier']]
        else:
            new_row = pd.DataFrame({
                'unique_identifier': [row['unique_identifier']],
                tag_column: [True],
                related_unique_identifier_column: [related_unique_identifier_row['unique_identifier']]
            })
            df_tag = pd.concat([df_tag, new_row], ignore_index=True)
    return df_tag

def get_shipping_line_if_order_first_shipment(row):
    global df_invoice, df_shipping_line, df_shipping_line_tag
    if row['order_name'] not in df_invoice['order_name'].values:
        matching_shipping_lines = df_shipping_line[df_shipping_line['order_name'] == row['order_name']]
        if not matching_shipping_lines.empty:
            # 计算invoice里面shipping的金额填多少
            first_shipment_total_shipping = matching_shipping_lines['total_price_in_usd'].sum()
            # 标记df_shipping_line_tag
            new_data = pd.DataFrame({
                'unique_identifier': matching_shipping_lines['unique_identifier'].values,
                'if_assigned': [True] * len(matching_shipping_lines),
                'shipment_unique_identifier': [row['unique_identifier'][0]] * len(matching_shipping_lines)
            })
            df_shipping_line_tag = pd.concat([df_shipping_line_tag, new_data], ignore_index=True)
            # 生成新的invoice line
            new_row = pd.DataFrame({
                'order_name': [row['order_name']],
                'order_id': [row['order_id']],
                'order_created_at_pdt': [row['order_created_at_pdt']],
                'transaction_type': ["invoice"],
                'line_type': ["SHIPPING"],
                'transaction_date': [row['event_happened_at_pdt'].date()],
                'shipping': [first_shipment_total_shipping],
                'unique_identifier': [row['unique_identifier']], # 记录这个shipping line是跟着哪一个shipment走的
                'if_sent': [False]
            })
            df_invoice = pd.concat([df_invoice, new_row], ignore_index=True)
            
def generate_shipping_journal_entry():
    global df_journal_entry, df_invoice, df_shipping_line, df_shipping_line_tag
    matching_shipping_lines = df_shipping_line[
        (~df_shipping_line['unique_identifier'].isin(
            df_shipping_line_tag[df_shipping_line_tag['if_assigned'] == True]['unique_identifier'])
        ) &
        (df_shipping_line['order_name'].isin(df_invoice['order_name']))
    ]
    
    # 计算journal entry的金额
    results = matching_shipping_lines.groupby(['order_name', 'order_number', 'customer_name', 'store', 'event_happened_date_pdt'])['total_price_in_usd'].sum().reset_index()
    results.columns = ['order_name', 'order_number', 'customer_name', 'store', 'event_happened_date_pdt', 'total_shipping']
    
    for index, row in results.iterrows():
        # 如果shipping是正值，即income
        if row['total_shipping'] > 0 :
            new_row = pd.DataFrame({
                'transaction_type': ["journal_entry"],
                'currency': ["USD United States Dollar"],
                'transaction_name': [f"{row['order_number']}-SP-{row['event_happened_date_pdt']}"],
                'transaction_date': [row['event_happened_date_pdt']],
                'account': ["11220100 Accounts Receivable (A/R)"],
                'debits': [row['total_shipping']],
                'credits': [None],
                'description': [f"Shipping income for {row['order_name']}"],
                'name': [row['customer_name']],
                'store': [row['store']],
                'unique_identifier': [None],
                'if_sent': [False]
            })
            df_journal_entry = pd.concat([df_journal_entry, new_row], ignore_index=True)
            
            new_row = pd.DataFrame({
                'transaction_type': ["journal_entry"],
                'currency': ["USD United States Dollar"],
                'transaction_name': [f"{row['order_number']}-SP-{row['event_happened_date_pdt']}"],
                'transaction_date': [row['event_happened_date_pdt']],
                'account': ["40010305 Amazon and Shopify sales:Shopify shipping income"],
                'debits': [None],
                'credits': [row['total_shipping']],
                'description': [f"Shipping income for {row['order_name']}"],
                'name': [row['customer_name']],
                'store': [row['store']],
                'unique_identifier': [None],
                'if_sent': [False]
            })
            df_journal_entry = pd.concat([df_journal_entry, new_row], ignore_index=True)
            
        # 如果shipping是负值，即refund
        elif row['total_shipping'] < 0 :
            new_row = pd.DataFrame({
                'transaction_type': ["journal_entry"],
                'currency': ["USD United States Dollar"],
                'transaction_name': [f"{row['order_number']}-SP-{row['event_happened_date_pdt']}"],
                'transaction_date': [row['event_happened_date_pdt']],
                'account': ["11220100 Accounts Receivable (A/R)"],
                'debits': [None],
                'credits': [row['total_shipping']],
                'description': [f"Shipping refund for {row['order_name']}"],
                'name': [row['customer_name']],
                'store': [row['store']],
                'unique_identifier': [None],
                'if_sent': [False]
            })
            df_journal_entry = pd.concat([df_journal_entry, new_row], ignore_index=True)
            
            new_row = pd.DataFrame({
                'transaction_type': ["journal_entry"],
                'currency': ["USD United States Dollar"],
                'transaction_name': [f"{row['order_number']}-SP-{row['event_happened_date_pdt']}"],
                'transaction_date': [row['event_happened_date_pdt']],
                'account': ["40010305 Amazon and Shopify sales:Shopify shipping income"],
                'debits': [row['total_shipping']],
                'credits': [None],
                'description': [f"Shipping refund for {row['order_name']}"],
                'name': [row['customer_name']],
                'store': [row['store']],
                'unique_identifier': [None],
                'if_sent': [False]
            })
            df_journal_entry = pd.concat([df_journal_entry, new_row], ignore_index=True)
    
    # 标记df_shipping_line_tag
    # 这里想一想一个一个订单能不能分步标记，不要一次性标记
    new_data = pd.DataFrame({
        'unique_identifier': matching_shipping_lines['unique_identifier'].values,
        'if_assigned': [True] * len(matching_shipping_lines),
        'shipment_unique_identifier': [None] * len(matching_shipping_lines)
    })
    df_shipping_line_tag = pd.concat([df_shipping_line_tag, new_data], ignore_index=True)
    

def get_line_item_discount(order_line, shipment_row=None, if_check_first_board_needed=False):
    global df_line_item_discount, df_invoice
    df_non_first_board_discount_lines = df_line_item_discount[
        (df_line_item_discount['order_name'] == order_line['order_name']) &
        (df_line_item_discount['line_item_id'] == order_line['line_item_id'])
    ]
    non_first_board_discount = df_non_first_board_discount_lines['total_discount_in_usd'].sum() # 如果non_first_board_discount_lines为空，sum()会返回0
    
    # 如果这个shipment row的产品是board产品的话
    if if_check_first_board_needed and 'board' in shipment_row['product_name'].lower():
        matching_first_board_discount_line = df_line_item_discount[
            (df_line_item_discount['order_name'] == shipment_row['order_name']) &
            (df_line_item_discount['discount_reallocation_target'] == "first_board")
        ]
        # 如果这个shipment_row的订单包含first_board的discount的话
        if not matching_first_board_discount_line.empty:
            matching_first_board_discount_line = matching_first_board_discount_line.iloc[0]
        
            existing_first_board_discount_in_invoice = df_invoice[
                (df_invoice['order_name'] == shipment_row['order_name']) &
                (df_invoice['discount_reallocation_target'] == "first_board")
            ]
            # 如果这个first_board的discount已经在invoice里生成过的话
            if not existing_first_board_discount_in_invoice.empty:
                total_discount = non_first_board_discount
                total_discount_type = "line_item"
            # 如果这个first_board的discount没有在invoice里生成过的话
            else:
                total_discount = non_first_board_discount + matching_first_board_discount_line['total_discount_in_usd']
                total_discount_type = "first_board"
        # 如果这个shipment_row的订单不包含first_board的discount的话
        else:
            total_discount = non_first_board_discount
            total_discount_type = "line_item"
    # 如果这个shipment row的产品不是board产品的话
    else:
        total_discount = non_first_board_discount
        total_discount_type = "line_item"
    return total_discount, total_discount_type



def get_custom_product_if_order_first_shipment(shipment_row):
    global df_line_item_discount, df_invoice, df_custom_product_added, df_custom_product_added_tag
    # 如果这是这个订单的第一个shipment的话
    if shipment_row['order_name'] not in df_invoice['order_name'].values:
        matching_custom_products_added = df_custom_product_added[
            (df_custom_product_added['order_name'] == shipment_row['order_name']) &
            (~df_custom_product_added['unique_identifier'].isin(
                df_custom_product_added_tag[df_custom_product_added_tag['if_shipped'] == True]['unique_identifier'])
            ) &
            (~df_custom_product_added['unique_identifier'].isin(
                df_custom_product_added_tag[df_custom_product_added_tag['if_refunded'] == True]['unique_identifier'])
            )
        ]
        
        # 如果这个订单有需要随着这个shipment发送的custom products
        if not matching_custom_products_added.empty:
            for index, custom_product_added_row in matching_custom_products_added.iterrows():
                # 标记df_custom_product_added_tag的if_shipped
                df_custom_product_added_tag = mark_tag(df_custom_product_added_tag, custom_product_added_row, 'if_shipped', 'shipment_unique_identifier', shipment_row)
                
                # 找到这个custom_product的discount值
                total_line_item_discount, item_discount_type = get_line_item_discount(custom_product_added_row)
                
                # 生成invocie
                new_row = pd.DataFrame({
                    'order_name': [custom_product_added_row['order_name']],
                    'order_id': [custom_product_added_row['order_id']],
                    'order_created_at_pdt': [custom_product_added_row['order_created_at_pdt']],
                    'transaction_type': ["invoice"],
                    'transaction_name': [f"{custom_product_added_row['order_number']}-{shipment_row['package_id']}"],
                    'line_type': [custom_product_added_row['line_type']],
                    'store': [custom_product_added_row['store']],
                    'transaction_date': [shipment_row['event_happened_at_pdt'].date()],
                    'ship_from': [shipment_row['ship_from']],
                    'shipping_date': [shipment_row['event_happened_at_pdt'].date()],
                    'ship_via': [shipment_row['carrier']],
                    'tracking_number': [shipment_row['tracking_number']],
                    'payment_terms': [custom_product_added_row['payment_terms']],
                    'due_date': [custom_product_added_row['due_date']],
                    'customer_name': [custom_product_added_row['customer_name']],
                    'customer_email': [custom_product_added_row['customer_email']],
                    'customer_phone_number': [custom_product_added_row['customer_phone_number']],
                    'shipping_country': [custom_product_added_row['shipping_country']],
                    'shipping_province': [custom_product_added_row['shipping_province']],
                    'shipping_city': [custom_product_added_row['shipping_city']],
                    'shipping_zip': [custom_product_added_row['shipping_zip']],
                    'shipping_address': [custom_product_added_row['shipping_address']],
                    'billing_country': [custom_product_added_row['billing_country']],
                    'billing_province': [custom_product_added_row['billing_province']],
                    'billing_city': [custom_product_added_row['billing_city']],
                    'billing_zip': [custom_product_added_row['billing_zip']],
                    'billing_address': [custom_product_added_row['billing_address']],
                    'transaction_product_name': [custom_product_added_row['line_item_name']],
                    'qty': [1],
                    'rate': [custom_product_added_row['unit_price_in_usd']],
                    'amount': [custom_product_added_row['unit_price_in_usd']],
                    'taxable': [custom_product_added_row['taxable']],
                    'discount': [total_line_item_discount],
                    'discount_reallocation_target': [item_discount_type],
                    'shipping': [None],
                    'unique_identifier': [custom_product_added_row['unique_identifier']],
                    'if_sent': [False]
                })
                df_invoice = pd.concat([df_invoice, new_row], ignore_index=True)



# 分步处理不是first shipment的custom product，因为它们需要在refund标记完成之后再决定要不要发送invoice
# 如果在发送invoice之前就退款了，那就可以直接不出现任何记录
def generate_custom_product_invoice():
    global df_line_item_discount, df_invoice, df_custom_product_added, df_custom_product_added_tag
    # 找到所有符合条件的custom products
    matching_custom_products_added = df_custom_product_added[
        (df_custom_product_added['order_name'].isin(df_invoice['order_name'])) &
        (~df_custom_product_added['unique_identifier'].isin(
            df_custom_product_added_tag[df_custom_product_added_tag['if_shipped'] == True]['unique_identifier'])
        ) &
        (~df_custom_product_added['unique_identifier'].isin(
            df_custom_product_added_tag[df_custom_product_added_tag['if_refunded'] == True]['unique_identifier'])
        )
    ]
    
    # 如果有符合条件的custom products
    if not matching_custom_products_added.empty:
        for index, custom_product_added_row in matching_custom_products_added.iterrows():
            # 标记df_custom_product_added_tag的if_shipped
            df_custom_product_added_tag = mark_tag(df_custom_product_added_tag, custom_product_added_row, 'if_shipped')
            
            # 找到这个custom_product的discount值
            total_line_item_discount, item_discount_type = get_line_item_discount(custom_product_added_row)
                 
            # 生成invocie
            new_row = pd.DataFrame({
                'order_name': [custom_product_added_row['order_name']],
                'order_id': [custom_product_added_row['order_id']],
                'order_created_at_pdt': [custom_product_added_row['order_created_at_pdt']],
                'transaction_type': ["invoice"],
                'transaction_name': [f"{custom_product_added_row['order_number']}-{custom_product_added_row['event_happened_at_pdt'].date()}"],
                'line_type': [custom_product_added_row['line_type']],
                'store': [custom_product_added_row['store']],
                'transaction_date': [custom_product_added_row['event_happened_at_pdt'].date()],
                'ship_from': [None],
                'shipping_date': [None],
                'ship_via': [None],
                'tracking_number': [None],
                'payment_terms': [custom_product_added_row['payment_terms']],
                'due_date': [custom_product_added_row['due_date']],
                'customer_name': [custom_product_added_row['customer_name']],
                'customer_email': [custom_product_added_row['customer_email']],
                'customer_phone_number': [custom_product_added_row['customer_phone_number']],
                'shipping_country': [custom_product_added_row['shipping_country']],
                'shipping_province': [custom_product_added_row['shipping_province']],
                'shipping_city': [custom_product_added_row['shipping_city']],
                'shipping_zip': [custom_product_added_row['shipping_zip']],
                'shipping_address': [custom_product_added_row['shipping_address']],
                'billing_country': [custom_product_added_row['billing_country']],
                'billing_province': [custom_product_added_row['billing_province']],
                'billing_city': [custom_product_added_row['billing_city']],
                'billing_zip': [custom_product_added_row['billing_zip']],
                'billing_address': [custom_product_added_row['billing_address']],
                'transaction_product_name': [custom_product_added_row['line_item_name']],
                'qty': [1],
                'rate': [custom_product_added_row['unit_price_in_usd']],
                'amount': [custom_product_added_row['unit_price_in_usd']],
                'taxable': [custom_product_added_row['taxable']],
                'discount': [total_line_item_discount],
                'discount_reallocation_target': [item_discount_type],
                'shipping': [None],
                'unique_identifier': [custom_product_added_row['unique_identifier']],
                'if_sent': [False]
            })
            df_invoice = pd.concat([df_invoice, new_row], ignore_index=True)
            
# def get_warranty_product(row):
    
def process_events(event_list):
    global df_physical_product_added_tag
    global df_physical_product_removed_tag
    global df_custom_product_added_tag
    global df_custom_product_removed_tag
    global df_warranty_added_tag
    global df_warranty_removed_tag
    global df_shipment_tag
    global df_shipping_line_tag
    global df_invoice
    global df_journal_entry
    global df_physical_product_added
    global df_physical_product_removed
    global df_custom_product_added
    global df_custom_product_removed
    global df_warranty_added
    global df_warranty_removed
    global df_shipment
    global df_shipping_line
    
    for index, row in event_list.iterrows():

        # 如果正在处理的event是order
        if row['action_type'] == 'SHIPMENT':
            #标记df_shipment_tag里的if_processed
            df_shipment_tag = mark_tag(df_shipment_tag, row, 'if_processed')
            
            # 先处理shipping_line和custom product
            get_shipping_line_if_order_first_shipment(row)
            get_custom_product_if_order_first_shipment(row)
#             get_warranty_if_board_shipment(row)
            
            # 找符合条件的order：
            matching_orders = df_physical_product_added[
                (df_physical_product_added['order_name'] == row['order_name']) &
                (df_physical_product_added['dim_physical_product_sk'] == row['dim_physical_product_sk']) &
                (~df_physical_product_added['unique_identifier'].isin(
                    df_physical_product_added_tag[df_physical_product_added_tag['if_shipped'] == True]['unique_identifier'])
                ) &
                (~df_physical_product_added['unique_identifier'].isin(
                    df_physical_product_added_tag[df_physical_product_added_tag['if_refunded'] == True]['unique_identifier'])
                )
            ]
            
            # 如果找到了符合条件的order:
            if not matching_orders.empty:
                order_shipment_assigned_to = matching_orders.loc[matching_orders['physical_product_unit_idx'].idxmin()]
                
                # 标记df_physical_product_added_tag
                df_physical_product_added_tag = mark_tag(df_physical_product_added_tag, order_shipment_assigned_to, 'if_shipped', 'shipment_unique_identifier', row)
                    
                # 标记df_shipment_tag
                df_shipment_tag = mark_tag(df_shipment_tag, row, 'if_assigned', 'order_unique_identifier', order_shipment_assigned_to)

                # 找出这个匹配到的order item的discount
                total_line_item_discount, item_discount_type = get_line_item_discount(order_shipment_assigned_to, row, True)
                
                 # 生成invoice
                new_row = pd.DataFrame({
                    'order_name': [row['order_name']],
                    'order_id': [row['order_id']],
                    'order_created_at_pdt': [row['order_created_at_pdt']],
                    'transaction_type': ["invoice"],
                    'transaction_name': [f"{row['order_number']}-{row['package_id']}"],
                    'line_type': [order_shipment_assigned_to['line_type']],
                    'store': [order_shipment_assigned_to['store']],
                    'transaction_date': [row['event_happened_at_pdt'].date()],
                    'ship_from': [row['ship_from']],
                    'shipping_date': [row['event_happened_at_pdt'].date()],
                    'ship_via': [row['carrier']],
                    'tracking_number': [row['tracking_number']],
                    'payment_terms': [order_shipment_assigned_to['payment_terms']],
                    'due_date': [order_shipment_assigned_to['due_date']],
                    'customer_name': [order_shipment_assigned_to['customer_name']],
                    'customer_email': [order_shipment_assigned_to['customer_email']],
                    'customer_phone_number': [order_shipment_assigned_to['customer_phone_number']],
                    'shipping_country': [order_shipment_assigned_to['shipping_country']],
                    'shipping_province': [order_shipment_assigned_to['shipping_province']],
                    'shipping_city': [order_shipment_assigned_to['shipping_city']],
                    'shipping_zip': [order_shipment_assigned_to['shipping_zip']],
                    'shipping_address': [order_shipment_assigned_to['shipping_address']],
                    'billing_country': [order_shipment_assigned_to['billing_country']],
                    'billing_province': [order_shipment_assigned_to['billing_province']],
                    'billing_city': [order_shipment_assigned_to['billing_city']],
                    'billing_zip': [order_shipment_assigned_to['billing_zip']],
                    'billing_address': [order_shipment_assigned_to['billing_address']],
                    'transaction_product_name': [row['product_name']],
                    'qty': [1],
                    'rate': [order_shipment_assigned_to['unit_price_in_usd']],
                    'amount': [order_shipment_assigned_to['unit_price_in_usd']],
                    'taxable': [order_shipment_assigned_to['taxable']],
                    'discount': [total_line_item_discount],
                    'discount_reallocation_target': [item_discount_type],
                    'shipping': [None],
                    'unique_identifier': [row['unique_identifier']],
                    'if_sent': [False]
                })
                df_invoice = pd.concat([df_invoice, new_row], ignore_index=True)
            
            # 如果没找到符合条件的order，用qb单价记录，没有discount和tax
            else:
                rate_value = df_quickbooks_products.loc[
                    df_quickbooks_products['product_name'] == row['product_name'], 
                    'sales_price_in_usd'
                ].squeeze()
                
                new_row = pd.DataFrame({
                    'order_name': [row['order_name']],
                    'order_id': [row['order_id']],
                    'order_created_at_pdt': [row['order_created_at_pdt']],
                    'transaction_type': ["invoice"],
                    'transaction_name': [f"{row['order_number']}-{row['package_id']}"],
                    'line_type': ["PRODUCT PRE-SHIPPED"],
                    'store': [row['store']],
                    'transaction_date': [row['event_happened_at_pdt'].date()],
                    'ship_from': [row['ship_from']],
                    'shipping_date': [row['event_happened_at_pdt'].date()],
                    'ship_via': [row['carrier']],
                    'tracking_number': [row['tracking_number']],
                    'payment_terms': [row['payment_terms']],
                    'due_date': [row['due_date']],
                    'customer_name': [row['customer_name']],
                    'customer_email': [row['customer_email']],
                    'customer_phone_number': [row['customer_phone_number']],
                    'shipping_country': [row['shipping_country']],
                    'shipping_province': [row['shipping_province']],
                    'shipping_city': [row['shipping_city']],
                    'shipping_zip': [row['shipping_zip']],
                    'shipping_address': [row['shipping_address']],
                    'billing_country': [row['billing_country']],
                    'billing_province': [row['billing_province']],
                    'billing_city': [row['billing_city']],
                    'billing_zip': [row['billing_zip']],
                    'billing_address': [row['billing_address']],
                    'transaction_product_name': [row['product_name']],
                    'qty': [1],
                    'rate': [rate_value],
                    'amount': [rate_value],
                    'taxable': [False],
                    'discount': [None],
                    'discount_reallocation_target': [None],
                    'shipping': [None],
                    'unique_identifier': [row['unique_identifier']],
                    'if_sent': [False]
                })
                df_invoice = pd.concat([df_invoice, new_row], ignore_index=True)
        
        elif row['action_type'] in ['RETURN', 'UPDATE']:
            if row['line_type'] == 'PRODUCT':
                # 标记df_physical_product_removed_tag的if_processed
                df_physical_product_removed_tag = mark_tag(df_physical_product_removed_tag, row, 'if_processed')
                 
                #优先去找未被shipped的order
                unshipped_matching_orders = df_physical_product_added[
                    (df_physical_product_added['order_id'] == row['order_id']) &
                    (df_physical_product_added['product_name'] == row['product_name']) &
                    (df_physical_product_added['line_item_id'] == row['line_item_id']) &
                    (df_physical_product_added['event_happened_at_pdt'] <= row['event_happened_at_pdt']) &
                    (~df_physical_product_added['unique_identifier'].isin(
                        df_physical_product_added_tag[df_physical_product_added_tag['if_shipped'] == True]['unique_identifier']
                    )) &
                    (~df_physical_product_added['unique_identifier'].isin(
                        df_physical_product_added_tag[df_physical_product_added_tag['if_refunded'] == True]['unique_identifier']
                    ))
                ]
                 
                # 如果找到了未被shipped的order去匹配refund，则不需要生成任何transaction
                if not unshipped_matching_orders.empty:
                    order_refund_assigned = unshipped_matching_orders.loc[unshipped_matching_orders['physical_product_unit_idx'].idxmin()]
                    # 标记df_physical_product_added_tag的if_refunded
                    df_physical_product_added_tag = mark_tag(df_physical_product_added_tag, order_refund_assigned, 'if_refunded', 'refund_unique_identifier', row)
                    # 标记df_physical_product_removed_tag的if_assigned
                    df_physical_product_removed_tag = mark_tag(df_physical_product_removed_tag, row, 'if_assigned', 'order_unique_identifier', order_refund_assigned)
            
                # 如果没有找到未被shipped的order，只能匹配已shipped的order
                # 但是physical_product如果发生退款是不需要生成credit memo，要等return物流才会生成，只需要标记tag即可
                else:
                    shipped_matching_orders = df_physical_product_added[
                        (df_physical_product_added['order_id'] == row['order_id']) &
                        (df_physical_product_added['product_name'] == row['product_name']) &
                        (df_physical_product_added['line_item_id'] == row['line_item_id']) &
                        (df_physical_product_added['event_happened_at_pdt'] <= row['event_happened_at_pdt']) &
                        (df_physical_product_added['unique_identifier'].isin(
                            df_physical_product_added_tag[df_physical_product_added_tag['if_shipped'] == True]['unique_identifier']
                        )) &
                        (~df_physical_product_added['unique_identifier'].isin(
                            df_physical_product_added_tag[df_physical_product_added_tag['if_refunded'] == True]['unique_identifier']
                        ))
                    ]
                    
                    # shipped_matching_orders不可能为空
                    # 注意这里用了idxmax()，用以避免匹配到first board
                    order_refund_assigned = shipped_matching_orders.loc[shipped_matching_orders['physical_product_unit_idx'].idxmax()]
                    # 标记df_physical_product_added_tag的if_refunded
                    df_physical_product_added_tag = mark_tag(df_physical_product_added_tag, order_refund_assigned, 'if_refunded', 'refund_unique_identifier', row)
                    # 标记df_physical_product_removed_tag的if_assigned
                    df_physical_product_removed_tag = mark_tag(df_physical_product_removed_tag, row, 'if_assigned', 'order_unique_identifier', order_refund_assigned)
                 
            
            elif row['line_type'] == 'CUSTOM_PRODUCT':
                df_custom_product_removed_tag = mark_tag(df_custom_product_removed_tag, row, 'if_processed')

                # 优先去找未被shipped的order
                unshipped_matching_orders = df_custom_product_added[
                    (df_custom_product_added['order_id'] == row['order_id']) &
                    (df_custom_product_added['line_item_name'] == row['line_item_name']) &
                    (df_custom_product_added['line_item_id'] == row['line_item_id']) &
                    (df_custom_product_added['event_happened_at_pdt'] <= row['event_happened_at_pdt']) &
                    (~df_custom_product_added['unique_identifier'].isin(
                        df_custom_product_added_tag[df_custom_product_added_tag['if_shipped'] == True]['unique_identifier']
                    )) &
                    (~df_custom_product_added['unique_identifier'].isin(
                        df_custom_product_added_tag[df_custom_product_added_tag['if_refunded'] == True]['unique_identifier']
                    ))
                ]

                # 如果找到了未被shipped的order去匹配refund，则不需要生成任何transaction
                if not unshipped_matching_orders.empty:
                    order_refund_assigned = unshipped_matching_orders.loc[unshipped_matching_orders['physical_product_unit_idx'].idxmin()]
                    # 标记df_custom_product_added_tag的if_refunded
                    df_custom_product_added_tag = mark_tag(df_custom_product_added_tag, order_refund_assigned, 'if_refunded', 'refund_unique_identifier', row)
                    # 标记df_custom_product_removed_tag的if_assigned
                    df_custom_product_removed_tag = mark_tag(df_custom_product_removed_tag, row, 'if_assigned', 'order_unique_identifier', order_refund_assigned)

                # 如果没有找到未被shipped的order，只能匹配已shipped的order
                else:
                    shipped_matching_orders = df_custom_product_added_tag[
                        (df_custom_product_added_tag['order_id'] == row['order_id']) &
                        (df_custom_product_added_tag['line_item_name'] == row['line_item_name']) &
                        (df_custom_product_added_tag['line_item_id'] == row['line_item_id']) &
                        (df_custom_product_added['event_happened_at_pdt'] <= row['event_happened_at_pdt']) &
                        (df_custom_product_added['unique_identifier'].isin(
                            df_custom_product_added_tag[df_custom_product_added_tag['if_shipped'] == True]['unique_identifier']
                        )) &
                        (~df_custom_product_added['unique_identifier'].isin(
                            df_custom_product_added_tag[df_custom_product_added_tag['if_refunded'] == True]['unique_identifier']
                        ))
                    ]

                    # shipped_matching_orders不可能为空
                    order_refund_assigned = shipped_matching_orders.loc[shipped_matching_orders['physical_product_unit_idx'].idxmin()]
                    # 标记df_custom_product_added_tag的if_refunded
                    df_custom_product_added_tag = mark_tag(df_custom_product_added_tag, order_refund_assigned, 'if_refunded', 'refund_unique_identifier', row)
                    # 标记df_custom_product_removed_tag的if_assigned
                    df_custom_product_removed_tag = mark_tag(df_custom_product_removed_tag, row, 'if_assigned', 'order_unique_identifier', order_refund_assigned)
                
                    # 找到之前这个order在df_invoice里面对应的记录
                    custom_product_invoice_line = df_invoice[
                        (df_invoice['unique_identifier'] == order_refund_assigned['unique_identifier'])
                    ]

                    # 生成credit memo
                    new_row = pd.DataFrame({
                        'order_name': [row['order_name']],
                        'order_id': [row['order_id']],
                        'order_created_at_pdt': [row['order_created_at_pdt']],
                        'transaction_type': ["credit_memo"],
                        'transaction_name': [f"{row['order_number']}-{row['event_happened_at_pdt'].date()}"],
                        'line_type': [row['line_type']],
                        'store': [row['store']],
                        'transaction_date': [row['event_happened_at_pdt'].date()],
                        'ship_from': [row['ship_from']],
                        'customer_name': [row['customer_name']],
                        'customer_email': [row['customer_email']],
                        'customer_phone_number': [row['customer_phone_number']],
                        'billing_country': [row['billing_country']],
                        'billing_province': [row['billing_province']],
                        'billing_city': [row['billing_city']],
                        'billing_zip': [row['billing_zip']],
                        'billing_address': [row['billing_address']],
                        'transaction_product_name': [row['line_item_name']],
                        'qty': [1],
                        'rate': [custom_product_invoice_line['rate']],
                        'amount': [custom_product_invoice_line['amount']],
                        'taxable': [custom_product_invoice_line['taxable']],
                        'discount': [custom_product_invoice_line['discount']],
                        'discount_reallocation_target': [custom_product_invoice_line['discount_reallocation_target']],
                        'unique_identifier': [row['unique_identifier']],
                        'if_sent': [False]
                    })
                    df_credit_memo = pd.concat([df_credit_memo, new_row], ignore_index=True)
                 
            
            elif row['line_type'] == 'WARRANTY':
                df_warranty_removed_tag = mark_tag(df_warranty_removed_tag, row, 'if_processed')

                # 优先去找未被shipped的order
                unshipped_matching_orders = df_warranty_added[
                    (df_warranty_added['order_id'] == row['order_id']) &
                    (df_warranty_added['line_item_name'] == row['line_item_name']) &
                    (df_warranty_added['line_item_id'] == row['line_item_id']) &
                    (df_warranty_added['event_happened_at_pdt'] <= row['event_happened_at_pdt']) &
                    (~df_warranty_added['unique_identifier'].isin(
                        df_warranty_added_tag[df_warranty_added_tag['if_shipped'] == True]['unique_identifier']
                    )) &
                    (~df_warranty_added['unique_identifier'].isin(
                        df_warranty_added_tag[df_warranty_added_tag['if_refunded'] == True]['unique_identifier']
                    ))
                ]

                # 如果找到了未被shipped的order去匹配refund，则不需要生成任何transaction
                if not unshipped_matching_orders.empty:
                    order_refund_assigned = unshipped_matching_orders.loc[unshipped_matching_orders['physical_product_unit_idx'].idxmin()]
                    # 标记df_custom_product_added_tag的if_refunded
                    df_warranty_added_tag = mark_tag(df_warranty_added_tag, order_refund_assigned, 'if_refunded', 'refund_unique_identifier', row)
                    # 标记df_custom_product_removed_tag的if_assigned
                    df_warranty_removed_tag = mark_tag(df_warranty_removed_tag, row, 'if_assigned', 'order_unique_identifier', order_refund_assigned)

                # 如果没有找到未被shipped的order，只能匹配已shipped的order
                else:
                    shipped_matching_orders = df_warranty_added_tag[
                        (df_warranty_added_tag['order_id'] == row['order_id']) &
                        (df_warranty_added_tag['line_item_name'] == row['line_item_name']) &
                        (df_warranty_added_tag['line_item_id'] == row['line_item_id']) &
                        (df_warranty_added['event_happened_at_pdt'] <= row['event_happened_at_pdt']) &
                        (df_warranty_added['unique_identifier'].isin(
                            df_warranty_added_tag[df_warranty_added_tag['if_shipped'] == True]['unique_identifier']
                        )) &
                        (~df_warranty_added['unique_identifier'].isin(
                            df_warranty_added_tag[df_warranty_added_tag['if_refunded'] == True]['unique_identifier']
                        ))
                    ]

                    # shipped_matching_orders不可能为空
                    order_refund_assigned = shipped_matching_orders.loc[shipped_matching_orders['physical_product_unit_idx'].idxmin()]
                    # 标记df_custom_product_added_tag的if_refunded
                    df_warranty_added_tag = mark_tag(df_warranty_added_tag, order_refund_assigned, 'if_refunded', 'refund_unique_identifier', row)
                    # 标记df_custom_product_removed_tag的if_assigned
                    df_custom_product_removed_tag = mark_tag(df_custom_product_removed_tag, row, 'if_assigned', 'order_unique_identifier', order_refund_assigned)

                    # 找到之前这个order在df_invoice里面对应的记录
                    warranty_invoice_line = df_invoice[
                        (df_invoice['unique_identifier'] == order_refund_assigned['unique_identifier'])
                    ]

                    # 生成credit memo
                    new_row = pd.DataFrame({
                        'order_name': [row['order_name']],
                        'order_id': [row['order_id']],
                        'order_created_at_pdt': [row['order_created_at_pdt']],
                        'transaction_type': ["credit_memo"],
                        'transaction_name': [f"{row['order_number']}-{row['event_happened_at_pdt'].date()}"],
                        'line_type': [row['line_type']],
                        'store': [row['store']],
                        'transaction_date': [row['event_happened_at_pdt'].date()],
                        'ship_from': [row['ship_from']],
                        'customer_name': [row['customer_name']],
                        'customer_email': [row['customer_email']],
                        'customer_phone_number': [row['customer_phone_number']],
                        'billing_country': [row['billing_country']],
                        'billing_province': [row['billing_province']],
                        'billing_city': [row['billing_city']],
                        'billing_zip': [row['billing_zip']],
                        'billing_address': [row['billing_address']],
                        'transaction_product_name': [row['line_item_name']],
                        'qty': [1],
                        'rate': [custom_product_invoice_line['rate']],
                        'amount': [custom_product_invoice_line['amount']],
                        'taxable': [custom_product_invoice_line['taxable']],
                        'discount': [custom_product_invoice_line['discount']],
                        'discount_reallocation_target': [custom_product_invoice_line['discount_reallocation_target']],
                        'unique_identifier': [row['unique_identifier']],
                        'if_sent': [False]
                    })
                    df_credit_memo = pd.concat([df_credit_memo, new_row], ignore_index=True)


# 5
# 处理过程

for index, row in df_unprocessed_orders_sorted.iterrows():
    df_unprocessed_order_events = df_unprocessed_events[(df_unprocessed_events['order_name'] == row['order_name'])]
    df_unprocessed_order_events_sorted = df_unprocessed_order_events.sort_values(by='event_happened_at_pdt', ascending=True)
    
    process_events(df_unprocessed_order_events_sorted)
    
    df_physical_product_added_tag.to_excel('physical_product_added_tag.xlsx', index=False)
    df_physical_product_removed_tag.to_excel('physical_product_removed_tag.xlsx', index=False)
    df_custom_product_added_tag.to_excel('custom_product_added_tag.xlsx', index=False)
    df_custom_product_removed_tag.to_excel('custom_product_removed_tag.xlsx', index=False)
    df_warranty_added_tag.to_excel('warranty_added_tag.xlsx', index=False)
    df_warranty_removed_tag.to_excel('warranty_removed_tag.xlsx', index=False)
    df_shipping_line_tag.to_excel('shipping_line_tag.xlsx', index=False)
    df_shipment_tag.to_excel('shipment_tag.xlsx', index=False)
    df_invoice['if_sent'] = True
    df_invoice.to_excel('invoice.xlsx', index=False)
    
generate_shipping_journal_entry()
generate_custom_product_invoice()
df_physical_product_added_tag.to_excel('physical_product_added_tag.xlsx', index=False)
df_physical_product_removed_tag.to_excel('physical_product_removed_tag.xlsx', index=False)
df_custom_product_added_tag.to_excel('custom_product_added_tag.xlsx', index=False)
df_custom_product_removed_tag.to_excel('custom_product_removed_tag.xlsx', index=False)
df_warranty_added_tag.to_excel('warranty_added_tag.xlsx', index=False)
df_warranty_removed_tag.to_excel('warranty_removed_tag.xlsx', index=False)
df_shipping_line_tag.to_excel('shipping_line_tag.xlsx', index=False)
df_shipment_tag.to_excel('shipment_tag.xlsx', index=False)
df_invoice['if_sent'] = True
df_invoice.to_excel('invoice.xlsx', index=False)
df_journal_entry['if_sent'] = True
df_journal_entry.to_excel('journal_entry.xlsx', index=False)