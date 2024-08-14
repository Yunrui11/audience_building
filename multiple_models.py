import pandas as pd


categ = ['age_range', 'sex', 'ethnicity', 'party', 'education_level', 'homeowner_or_renter', 'geography_classification', 'income_level', 'maritalstatus', 'pd_state_abbreviation']
groups = {
    'age_range': {
        1: '18-24',
        2: '25-34',
        3: '35-44',
        4: '45-54',
        5: '55-64',
        6: '65-74',
        7: '75+'
    },
    'sex': {
        1: 'Male',
        2: 'Female'
    },
    'ethnicity': {
        1: 'White',
        2: 'Black',
        3: 'Hispanic',
        4: 'Asian'
    },
    'party': {
        1: 'Strong GOP',
        2: 'Steady GOP',
        3: 'Volatile GOP',
        4: 'Volatile IND',
        5: 'Steady IND',
        6: 'Volatile DEM',
        7: 'Steady DEM',
        8: 'Strong DEM'
    },
    'broad_political_labels' : {
        1: 'Republican',
        2: 'Republican',
        3: 'Republican',
        4: 'Independent',
        5: 'Independent',
        6: 'Democrat',
        7: 'Democrat',
        8: 'Democrat'
    },
    'education_level': {
        1: 'High School',
        2: 'Some College / Technical',
        3: 'College Degree',
        4: 'Post Grad'
    },
    'homeowner_or_renter' : {
        1: 'Owner',
        2: 'Renter'
    },
    'geography_classification': {
        1: 'Rural',
        2: 'Suburban',
        3: 'Urban'
    },
    'income_level': {
        1: 'Under $30K',
        2: '$30K-$50K',
        3: '$50K-$75K',
        4: '$75k-$100K',
        5: '$100K+'
    },
    'maritalstatus': {
        1: 'Married',
        2: 'Single'
    },
    'pd_state_abbreviation' : {
        'Northeast': ['CT', 'ME', 'MA', 'NH', 'RI', 'VT', 'NJ', 'NY', 'PA'],
        'Midwest': ['IL', 'IN', 'MI', 'OH', 'WI', 'IA', 'KS', 'MN', 'MO', 'NE', 'ND', 'SD'],
        'Fringe South': ['DC', 'DE', 'VA', 'WV', 'MD', 'OK', 'FL'],
        'Deep South': ['SC', 'GA', 'NC', 'TX', 'AR', 'LA', 'MS', 'AL', 'TN', 'KY'],
        'West': ['MT', 'WY', 'CO', 'NM', 'ID', 'UT', 'AZ', 'NV', 'WA', 'OR', 'CA', 'AK', 'HI']
    }
}

def input_info(pairs, file_path, name, Voter = True, default_threshold=None, Joint = False, patterns = "tunnl"):
    df = pd.read_csv(file_path)
    for i, pair in enumerate(pairs):
        filtered_df, total_count, default = building_audience(df, pair[0], pair[1], Voter, default_threshold, Joint)
        final_df = demographics_data(categ, groups, filtered_df, total_count, default)
        model_df = calculate_ave_modelscore(filtered_df, patterns)
        combined_df = pd.concat([final_df, model_df], axis=1)
        combined_df.to_excel(f"{name[i]}.xlsx", index=False)
        
 
## user input functions
def building_audience(df, model_1, model_2, Voter, default_threshold, Joint):
    if Voter:
        df_full = df.dropna(subset=['dt_regid']).copy()  # Make a copy to avoid SettingWithCopyWarning
    else:
        df_full = df.copy()
    if Joint:
        df_full['Net'] = df_full[model_1] * df_full[model_2]
    else: 
        df_full['Net'] = df_full[model_1] - df_full[model_2]
    if default_threshold is None: 
        # Calculate the average and standard deviation of the Net column
        avg_net = df_full['Net'].mean()
        std_net = df_full['Net'].std()
        default = round(avg_net + std_net, 4)

    # Apply the threshold to filter the audience
    filtered_df = df_full[df_full['Net'] >= default]
    total_count = len(filtered_df)
    
    return filtered_df, total_count, default

def audience_insight(var_name, label,  df, total_count):
    df = df[df[var_name].isin(label.keys())]
    count_by_group = df.groupby(var_name).size()
    count_by_group.index = count_by_group.index.map(label)
    percent = count_by_group / total_count
    
    return percent

def demographics_data(categ, groups, filtered_df, total_count, default_threshold):
    # count total people
    num = len(filtered_df)
    # count voter/nonvoter
    num_nan = filtered_df['dt_regid'].isna().sum()/num
    num_with_values = filtered_df['dt_regid'].notna().sum()/num
    # Create an empty DataFrame with the specified columns
    final_df = pd.DataFrame({
    'Features': ['Count', 'Voter', 'NotVoter'],
    'Percentage':  [num, num_with_values, num_nan]})

    for i in categ[:-1]:
        percent = audience_insight(i, groups[i], filtered_df, total_count)
        # Convert the Series to a DataFrame
        percent_df = percent.reset_index()
        # Rename the columns
        percent_df.columns = ['Features', 'Percentage']
        final_df = pd.concat([final_df, percent_df], ignore_index=True)
        if i == 'party':
            label = 'broad_political_labels'
            extra_percent = audience_insight(i, groups[label], filtered_df, total_count)
            extra_percent = extra_percent.groupby(level=0).sum()
            # Convert the Series to a DataFrame
            percent_df = extra_percent.reset_index()
            percent_df.columns = ['Features', 'Percentage']
            final_df = pd.concat([final_df, percent_df], ignore_index=True)
    var = categ[-1]
    # Create a reverse mapping for quick lookup
    state_to_region = {state: region for region, states in groups[var].items() for state in states}
    percent = audience_insight(var, state_to_region, filtered_df, total_count)
    percent = percent.groupby(level=0).sum()
    # Convert the Series to a DataFrame
    percent_df = percent.reset_index()
    percent_df.columns = ['Features', 'Percentage']
    final_df = pd.concat([final_df, percent_df], ignore_index=True)
    final_df['Percentage'] = final_df['Percentage'].round(3)
    final_df['cutpoint'] = default_threshold
    
    return final_df


def selected_columns(columns, patterns):
    selected_columns = [col for col in columns if patterns in col]
    return selected_columns

def calculate_ave_modelscore(filtered_df, patterns):
    column_names = filtered_df.columns.tolist()
    selected = selected_columns(column_names, patterns)
    models_ave = []
    for i in selected: 
        models_ave.append(round(filtered_df[i].mean(), 3))
    model_df = pd.DataFrame({
    'model_name': selected,
    'average':  models_ave})

    return model_df