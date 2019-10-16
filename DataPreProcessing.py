import pandas as pd
import warnings
warnings.filterwarnings(action='ignore')


# def build_url():
#     location = client.get_bucket_location(Bucket=Landing_BucketName)['LocationConstraint']
#     s3_urls = []
#
#     for file in list_of_files_to_download:
#         object_url = "https://s3-{0}.amazonaws.com/{1}/{2}".format(location, Landing_BucketName, file)
#         s3_urls.append(object_url)
#
#     return s3_urls

def resize_frame(df):
    # Commond Identified Columns
    common_cols = ['acc_now_delinq',
                   'acc_open_past_24mths',
                   'addr_state',
                   'zip_code',
                   'all_util',
                   'annual_inc',
                   'annual_inc_joint',
                   'application_type',
                   'avg_cur_bal',
                   'bc_open_to_buy',
                   'bc_util',
                   'chargeoff_within_12_mths',
                   'collections_12_mths_ex_med',
                   'delinq_2yrs',
                   'delinq_amnt',
                   'desc',
                   'dti',
                   'dti_joint',
                   'earliest_cr_line',
                   'emp_length',
                   'fico_range_high',
                   'fico_range_low',
                   'funded_amnt',
                   'grade',
                   'home_ownership',
                   'id',
                   'il_util',
                   'initial_list_status',
                   'inq_fi',
                   'inq_last_12m',
                   'inq_last_6mths',
                   'installment',
                   'int_rate',
                   'loan_amnt',
                   'max_bal_bc',
                   'member_id',
                   'mo_sin_old_il_acct',
                   'mo_sin_old_rev_tl_op',
                   'mo_sin_rcnt_rev_tl_op',
                   'mo_sin_rcnt_tl',
                   'mths_since_last_delinq',
                   'mths_since_last_major_derog',
                   'mths_since_last_record',
                   'mths_since_rcnt_il',
                   'mths_since_recent_bc',
                   'mths_since_recent_bc_dlq',
                   'mths_since_recent_inq',
                   'mths_since_recent_revol_delinq',
                   'num_accts_ever_120_pd',
                   'num_actv_bc_tl',
                   'num_actv_rev_tl',
                   'num_bc_sats',
                   'num_bc_tl',
                   'num_il_tl',
                   'num_op_rev_tl',
                   'num_rev_accts',
                   'num_rev_tl_bal_gt_0',
                   'num_sats',
                   'num_tl_120dpd_2m',
                   'num_tl_30dpd',
                   'num_tl_90g_dpd_24m',
                   'num_tl_op_past_12m',
                   'open_acc',
                   'open_acc_6m',
                   'open_act_il',
                   'open_il_12m',
                   'open_il_24m',
                   'open_rv_12m',
                   'open_rv_24m',
                   'pct_tl_nvr_dlq',
                   'percent_bc_gt_75',
                   'pub_rec',
                   'pub_rec_bankruptcies',
                   'purpose',
                   'revol_bal',
                   'revol_bal_joint',
                   'revol_util',
                   'sec_app_chargeoff_within_12_mths',
                   'sec_app_collections_12_mths_ex_med',
                   'sec_app_earliest_cr_line',
                   'sec_app_fico_range_high',
                   'sec_app_fico_range_low',
                   'sec_app_inq_last_6mths',
                   'sec_app_mort_acc',
                   'sec_app_mths_since_last_major_derog',
                   'sec_app_num_rev_accts',
                   'sec_app_open_acc',
                   'sec_app_open_act_il',
                   'sec_app_revol_util',
                   'sub_grade',
                   'tax_liens',
                   'term',
                   'total_acc',
                   'total_bal_ex_mort',
                   'total_bal_il',
                   'total_bc_limit',
                   'total_cu_tl',
                   'total_il_high_credit_limit',
                   'total_rev_hi_lim',
                   'tot_coll_amt',
                   'tot_cur_bal',
                   'tot_hi_cred_lim']
    df = df[common_cols]
    unwanted_idx = list(df[df['id'].apply(lambda x: x if x.isnumeric() else 'none') == 'none']['id'].index)
    df = df.drop(unwanted_idx)
    df = df.set_index('id', verify_integrity=True)
    df = df.sort_index()
    return df

def assign_datatypes(df):
    df['int_rate'] = df['int_rate'].apply(lambda x: x.replace('%', '')).astype('float64')
    df['revol_util'] = df['revol_util'].apply(lambda x: str(x).replace('%', '')).astype('float64')
    df['term'] = df['term'].apply(lambda x: x.replace(' months', '')).astype('int64')
    df['earliest_cr_line'] = pd.to_datetime(df['earliest_cr_line'])
    df['sec_app_earliest_cr_line'] = pd.to_datetime(df['sec_app_earliest_cr_line'])
    df['zip_code'] = df['zip_code'].apply(lambda x: str(x).replace('x', '')).astype('category').cat.codes
    df['addr_state'] = df['addr_state'].apply(lambda x: x.strip()).astype('category').cat.codes
    df['application_type'] = df['application_type'].astype('category').cat.codes
    df['emp_length'] = df['emp_length'].astype('category').cat.codes
    df['grade'] = df['grade'].astype('category').cat.codes
    df['home_ownership'] = df['home_ownership'].astype('category').cat.codes
    df['initial_list_status'] = df['initial_list_status'].astype('category').cat.codes
    df['purpose'] = df['purpose'].astype('category').cat.codes
    df['sub_grade'] = df['sub_grade'].astype('category').cat.codes
    return df

def impute_nulls(df):
    # take mean value for imputing
    nulls_mean = ['all_util',
                  'annual_inc_joint',
                  'avg_cur_bal',
                  'bc_open_to_buy',
                  'bc_util',
                  'dti',
                  'dti_joint',
                  'il_util',
                  'num_tl_120dpd_2m',
                  'pct_tl_nvr_dlq',
                  'percent_bc_gt_75',
                  'revol_bal_joint',
                  'revol_util',
                  'sec_app_open_acc',
                  'sec_app_open_act_il']
    # take most frequent value
    null_freq_list = ['mo_sin_old_il_acct',
                      'mths_since_last_delinq',
                      'mths_since_last_major_derog',
                      'mths_since_last_record',
                      'mths_since_rcnt_il',
                      'mths_since_recent_bc',
                      'mths_since_recent_bc_dlq',
                      'mths_since_recent_inq',
                      'mths_since_recent_revol_delinq',
                      'sec_app_chargeoff_within_12_mths',
                      'sec_app_collections_12_mths_ex_med',
                      'sec_app_fico_range_high',
                      'sec_app_fico_range_low',
                      'sec_app_inq_last_6mths',
                      'sec_app_mort_acc',
                      'sec_app_mths_since_last_major_derog',
                      'sec_app_num_rev_accts',
                      'sec_app_revol_util',
                      'zip_code']
    # Datetimes to be imputed
    null_datetimes = ['sec_app_earliest_cr_line']

    for cols in nulls_mean:
        mean_val = df[cols].mean()
        df[cols] = df[cols].apply(lambda x: mean_val if pd.isna(x) else x)

    for cols in null_freq_list:
        freq_val = df.mode()[cols][0]
        df[cols] = df[cols].apply(lambda x: freq_val if pd.isna(x) else x)

    for cols in null_datetimes:
        date_val = pd.to_datetime('2099-01-01')
        df[cols] = df[cols].apply(lambda x: date_val if pd.isna(x) else x)

    df['desc'] = df['desc'].apply(lambda x: 'Unknown' if pd.isna(x) else x)
    df['member_id'] = df['member_id'].apply(lambda x: -1 if pd.isna(x) else x)
    return df