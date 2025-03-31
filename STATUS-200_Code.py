# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import os
import re
from datetime import datetime

print("Starting Viksit Bharat Data Analysis...")
print("Team: STATUS-200")
print("Team Lead: Pratik Rana")

# %%
# --- Configuration ---
# Set file paths relative to where the script is run
# Assume data files are in a 'data' subdirectory
DATA_DIR = 'data'
OUTPUT_DIR = 'visualizations'

# Create output directory if it doesn't exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# %%
# Set plot style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = (12, 6)
plt.rcParams['figure.dpi'] = 100 # Lower DPI for potentially faster rendering if needed

# %%
# --- Helper Functions ---
def save_plot(filename, fig=plt):
    """Saves the current plot to the output directory."""
    filepath = os.path.join(OUTPUT_DIR, filename)
    fig.savefig(filepath, bbox_inches='tight', dpi=150)
    print(f"Saved plot: {filepath}")
    if fig == plt:
        plt.close() # Close the plot to free memory
    else:
        plt.close(fig) # Close specific figure object

def format_crore(x, pos):
    'The two args are the value and tick position'
    return '{:,.0f} Cr'.format(x / 1e7) # Assuming input is in Rupees, format to Crores

crore_formatter = mticker.FuncFormatter(format_crore)


def clean_numeric_column(series):
    """Cleans common issues in numeric columns (commas, '₹ crore')"""
    if series.dtype == 'object':
        # Remove commas and any non-numeric characters except decimal point and minus sign
        series = series.str.replace(',', '', regex=False)
        series = series.str.replace(r'[^\d.-]', '', regex=True)
        # Handle potential empty strings after cleaning
        series = series.replace('', np.nan)
    # Convert to numeric, coercing errors to NaN
    return pd.to_numeric(series, errors='coerce')


def clean_year_col(col_name):
    """Cleans year column names by extracting the fiscal year (e.g., '2012-13')."""
    if isinstance(col_name, str):
        col_name = col_name.replace('\n', ' ').strip()
        # Extract year part like '2012-13'
        import re
        match = re.search(r'(\d{4}-\d{2})', col_name)
        if match:
            return match.group(1)
    return col_name

# %% [markdown]
# # --- Data Loading and Preprocessing ---
# 

# %%
# 1. IIP Data
print("\n--- Processing IIP Data ---")
try:
    iip_file = os.path.join(DATA_DIR, 'IIP_data.xlsx')
    iip_df = pd.read_excel(iip_file, sheet_name=0) # Assuming data is on the first sheet

    # Rename columns for easier access
    iip_df.rename(columns={'NIC 2008': 'NIC_Code', 'Description': 'Sector'}, inplace=True)

    # Melt the dataframe to long format
    id_vars = ['NIC_Code', 'Sector', 'Weights']
    value_vars = [col for col in iip_df.columns if col not in id_vars]
    iip_long = pd.melt(iip_df, id_vars=id_vars, value_vars=value_vars, var_name='Month_Year', value_name='IIP')

    # Convert Month_Year to datetime objects
    # Handle potential errors during conversion, assuming 'MM/YY' format
    def parse_date(date_str):
        try:
            # Try parsing MM/YY format first
            return pd.to_datetime(date_str, format='%m/%y')
        except ValueError:
             # If that fails, try MM/YYYY if that's possible (less likely here but good practice)
             try:
                 return pd.to_datetime(date_str, format='%m/%Y')
             except ValueError:
                 # Try Day/Month/Year (e.g., from Excel date numbers converted wrongly)
                 try:
                     # Check if it might be an Excel serial date number misinterpreted
                     if isinstance(date_str, (int, float)):
                          # Excel base date is 1899-12-30 for Windows
                          base_date = datetime(1899, 12, 30)
                          delta = pd.Timedelta(days=date_str)
                          dt = base_date + delta
                          # Format to first of the month
                          return dt.replace(day=1)
                     else:
                         # Try other common formats if needed
                         return pd.to_datetime(date_str)
                 except:
                     return pd.NaT # Return Not a Time if all parsing fails

    iip_long['Date'] = iip_long['Month_Year'].apply(parse_date)

    # Drop rows where date parsing failed
    iip_long.dropna(subset=['Date'], inplace=True)

    # Convert IIP to numeric
    iip_long['IIP'] = pd.to_numeric(iip_long['IIP'], errors='coerce')
    iip_long.dropna(subset=['IIP'], inplace=True)

    # Sort by Sector and Date
    iip_long.sort_values(by=['Sector', 'Date'], inplace=True)

    # Calculate weighted contribution (optional, good for seeing impact)
    # iip_long['Weighted_IIP'] = iip_long['IIP'] * iip_long['Weights'] / 100

    print(f"IIP data loaded: {iip_long.shape[0]} rows")
    print(f"IIP Date range: {iip_long['Date'].min()} to {iip_long['Date'].max()}")
    #print(iip_long.head())
    #print(iip_long.info())
    #print(iip_long[iip_long['Sector'] == 'General'].tail()) # Check latest General IIP data

except FileNotFoundError:
    print(f"Error: IIP file not found at {iip_file}")
    iip_long = pd.DataFrame() # Create empty df to avoid later errors
except Exception as e:
    print(f"Error processing IIP data: {e}")
    iip_long = pd.DataFrame()

# %%
# 2. GCF Data

print("\n--- Processing GCF Data ---")
try:
    gcf_file = os.path.join(DATA_DIR, '1.10.xlsx')
    
    # First inspect the file structure
    preview_df = pd.read_excel(gcf_file, header=None, nrows=10)
    
    # Try to identify the correct header row and data start
    # This is more flexible than hard-coded row numbers
    header_row = None
    for i in range(10):
        row = preview_df.iloc[i].astype(str).str.lower()
        if row.str.contains('item').any():
            header_row = i
            print(f"Found header row at index {i}")
            break
    
    if header_row is not None:
        # Read with the detected header row
        gcf_df = pd.read_excel(gcf_file, header=header_row)
        
        # Find the item column
        item_col = [col for col in gcf_df.columns if 'item' in str(col).lower()]
        if item_col:
            item_col = item_col[0]
            print(f"Item column identified: '{item_col}'")
            
            # Find constant price columns (usually contains years like 2011-12)
            constant_price_cols = [col for col in gcf_df.columns 
                                  if any(f"20{i:02d}-{(i+1)%100:02d}" in str(col) 
                                        for i in range(11, 25))]
            
            if constant_price_cols:
                print(f"Found {len(constant_price_cols)} constant price year columns")
                
                # Create subset with relevant columns
                gcf_constant = gcf_df[[item_col] + constant_price_cols].copy()
                gcf_constant.rename(columns={item_col: 'Sector'}, inplace=True)
                
                # Clean sector names
                gcf_constant['Sector'] = gcf_constant['Sector'].str.replace(r'^\d+(\.\d+)?\s*', '', regex=True).str.strip()
                
                # Convert columns to numeric
                for col in constant_price_cols:
                    gcf_constant[col] = clean_numeric_column(gcf_constant[col])
                
                # Melt for easier analysis
                gcf_long = pd.melt(
                    gcf_constant, 
                    id_vars=['Sector'], 
                    var_name='Year', 
                    value_name='GCF_Constant_Crore'
                )
                
                # Extract year information
                gcf_long['Year_Start'] = gcf_long['Year'].astype(str).str.extract(r'(\d{4})').astype(int)
                gcf_long.sort_values(by=['Sector', 'Year_Start'], inplace=True)
                
                print(f"GCF data loaded: {gcf_long.shape[0]} rows")
            else:
                raise ValueError("Could not identify constant price columns")
        else:
            raise ValueError("Could not identify Item column")
    else:
        raise ValueError("Could not identify header row")
        
except FileNotFoundError:
    print(f"Error: GCF file not found at {gcf_file}")
    gcf_constant = pd.DataFrame()
    gcf_long = pd.DataFrame()
except Exception as e:
    print(f"Error processing GCF data: {e}")
    import traceback
    traceback.print_exc()
    gcf_constant = pd.DataFrame()
    gcf_long = pd.DataFrame()

# %%
# 3. NVA Data
print("\n--- Processing NVA Data ---")
try:
    # Assuming the data is already loaded into a DataFrame from the document
    # If you're reading from an Excel file, uncomment and adjust the path
    nva_file = os.path.join(DATA_DIR, '1.7.xlsx')
    # Read the Excel file with the correct header row
    nva_df = pd.read_excel(nva_file, header=4, skiprows=[0, 1])

    # Identify constant price columns (second set of years, starting from 2011-12 constant prices)
    constant_price_cols_nva = nva_df.columns[14:].tolist()  # Columns 14-25 are constant prices (2011-12 to 2022-23)
    item_col_nva = 'Item'  # Assuming 'Item' is the sector column

    # Select relevant columns for constant prices
    nva_constant = nva_df[[item_col_nva] + constant_price_cols_nva].copy()

    # Clean column names to extract years
    nva_constant.columns = [clean_year_col(col) if col != item_col_nva else col for col in nva_constant.columns]
    nva_constant.rename(columns={'Item': 'Sector'}, inplace=True)

    # Clean Sector names (remove numbering like '1.1')
    nva_constant['Sector'] = nva_constant['Sector'].str.replace(r'^\d+(\.\d+)?\s*', '', regex=True).str.strip()

    # Convert year columns to numeric
    year_cols_nva = [col for col in nva_constant.columns if col != 'Sector']
    for col in year_cols_nva:
        nva_constant[col] = clean_numeric_column(nva_constant[col])

    # Remove rows with NaN in Sector and exclude summary rows
    nva_constant = nva_constant[~nva_constant['Sector'].str.contains('TOTAL NVA', na=False, case=True)]
    nva_constant.dropna(subset=['Sector'], inplace=True)

    # Separate Total NVA for overall trend plot
    total_nva_row = nva_df[nva_df[item_col_nva].str.contains('TOTAL NVA', na=False, case=True)]
    total_nva_constant = total_nva_row[[item_col_nva] + constant_price_cols_nva].copy()
    total_nva_constant.columns = [clean_year_col(col) if col != item_col_nva else col for col in total_nva_constant.columns]
    total_nva_constant.rename(columns={'Item': 'Sector'}, inplace=True)
    for col in year_cols_nva:
        total_nva_constant[col] = clean_numeric_column(total_nva_constant[col])

    # Melt Total NVA for plotting
    total_nva_long = pd.melt(total_nva_constant, id_vars=['Sector'], var_name='Year', value_name='NVA_Constant_Crore')
    total_nva_long['Year_Start'] = total_nva_long['Year'].str.split('-').str[0].astype(int)

    # Melt sector data for plotting
    nva_long = pd.melt(nva_constant, id_vars=['Sector'], var_name='Year', value_name='NVA_Constant_Crore')
    nva_long['Year_Start'] = nva_long['Year'].str.split('-').str[0].astype(int)
    nva_long.sort_values(by=['Sector', 'Year_Start'], inplace=True)

    print(f"NVA data loaded: {nva_long.shape[0]} rows")

except Exception as e:
    print(f"Error processing NVA data: {e}")
    nva_constant = pd.DataFrame()
    nva_long = pd.DataFrame()
    total_nva_long = pd.DataFrame()

# %%

# 4. Quarterly GVA data
print("\n--- Processing Quarterly GVA Data ---")
try:
    q_gva_file = os.path.join(DATA_DIR, '8.18.1.xlsx')
    
    # The file has a complex structure with sections for current and constant prices
    # Since we want constant prices, we'll load the second table which starts around row 21
    # First, let's get a full preview to find the constant prices section
    full_preview = pd.read_excel(q_gva_file, header=None, nrows=30)
    print("First 30 rows preview:")
    
    # Find the row that contains "Statement- 8.18.1: Quarterly Estimates of GVA at Constant Prices"
    constant_prices_row = None
    for i, row in full_preview.iterrows():
        if isinstance(row[1], str) and "Constant Prices" in row[1]:
            constant_prices_row = i
            print(f"Found constant prices section at row {i}")
            break
    
    # If constant prices section not found, adjust to a sensible default
    if constant_prices_row is None:
        constant_prices_row = 16  # Based on the file excerpt provided
        print(f"Using default constant prices section at row {constant_prices_row}")
    
    # Now load the quarters row (5 rows after constant_prices_row header)
    quarters_row = constant_prices_row + 4  # Adjust based on file structure
    quarters_df = pd.read_excel(
        q_gva_file,
        header=None,
        skiprows=quarters_row,
        nrows=1
    )
    
    # Load the data (2 rows after the quarters row)
    data_df = pd.read_excel(
        q_gva_file,
        header=None,
        skiprows=quarters_row + 2  # Skip to the actual data
    )
    
    # Extract the first two columns (S.No. and Item/Sector)
    data_df.rename(columns={0: 'S.No.', 1: 'Sector'}, inplace=True)
    
    # Now create the year-quarter mapping for each column
    year_quarter_pairs = []
    current_year = None
    
    # The year headers appear every 4 columns (2, 6, 10, 14, etc.)
    # The quarters (Q1, Q2, Q3, Q4) are always in sequence
    for col_idx in range(2, len(quarters_df.columns)):
        year_val = quarters_df.iloc[0, col_idx]
        
        # If this is a year column (typically has format like "2011-12")
        if isinstance(year_val, str) and re.match(r'\d{4}-\d{2}', year_val):
            current_year = year_val
            # The next 4 columns correspond to Q1, Q2, Q3, Q4
            for q, offset in enumerate(range(4)):
                if col_idx + offset < len(quarters_df.columns):
                    quarter_val = f"Q{q+1}"
                    year_quarter = f"{current_year}_{quarter_val}"
                    year_quarter_pairs.append((col_idx + offset, year_quarter))
    
    print(f"Found {len(year_quarter_pairs)} year-quarter pairs")
    
    # Create a dataframe with mapped columns
    gva_data = pd.DataFrame()
    gva_data['Sector'] = data_df['Sector']
    
    # Add each column with its proper year-quarter label
    for col_idx, year_quarter in year_quarter_pairs:
        if col_idx < len(data_df.columns):
            gva_data[year_quarter] = data_df[col_idx]
    
    # Filter out rows where Sector is NaN or contains "GVA at Basic Price" (total row)
    gva_data = gva_data[~gva_data['Sector'].isnull()]
    gva_data = gva_data[~gva_data['Sector'].astype(str).str.contains('GVA at Basic Price', na=False)]
    
    # Melt the dataframe to get data in long format
    q_gva_long = pd.melt(
        gva_data,
        id_vars=['Sector'],
        var_name='Year_Quarter',
        value_name='GVA_Constant_Crore'
    )
    
    # Only keep rows where Year_Quarter is a string (not NaN)
    q_gva_long = q_gva_long[q_gva_long['Year_Quarter'].notna()]
    
    # Now parse the Year_Quarter column
    q_gva_long['Year'] = q_gva_long['Year_Quarter'].str.split('_').str[0]
    q_gva_long['Quarter'] = q_gva_long['Year_Quarter'].str.split('_').str[1]
    
    # Parse dates correctly for fiscal years
    def parse_fiscal_year_quarter(row):
        try:
            year = row['Year']
            quarter = row['Quarter']
            
            if pd.isna(year) or pd.isna(quarter):
                return pd.NaT
                
            # Extract start year from fiscal year (e.g., 2011 from 2011-12)
            year_start = int(year.split('-')[0])
            quarter_num = int(quarter.replace('Q', ''))
            
            # Convert fiscal quarter to date
            if quarter_num == 1:
                return pd.Timestamp(f"{year_start}-04-01")  # Q1: Apr-Jun
            elif quarter_num == 2:
                return pd.Timestamp(f"{year_start}-07-01")  # Q2: Jul-Sep
            elif quarter_num == 3:
                return pd.Timestamp(f"{year_start}-10-01")  # Q3: Oct-Dec
            elif quarter_num == 4:
                return pd.Timestamp(f"{year_start+1}-01-01")  # Q4: Jan-Mar (next year)
        except:
            return pd.NaT
        
    # Apply date parsing
    q_gva_long['Date'] = q_gva_long.apply(parse_fiscal_year_quarter, axis=1)
    
    # Clean data
    q_gva_long['GVA_Constant_Crore'] = pd.to_numeric(q_gva_long['GVA_Constant_Crore'], errors='coerce')
    q_gva_long = q_gva_long.dropna(subset=['Date', 'GVA_Constant_Crore'])
    
    # Clean sector names - remove S.No. if present
    q_gva_long['Sector'] = q_gva_long['Sector'].astype(str).str.replace(r'^\d+\s*', '', regex=True).str.strip()
    
    # Sort and calculate growth
    q_gva_long.sort_values(['Sector', 'Date'], inplace=True)
    q_gva_long['GVA_YoY_Growth'] = q_gva_long.groupby('Sector')['GVA_Constant_Crore'].pct_change(4) * 100
    
    print(f"GVA quarterly data processed: {len(q_gva_long)} rows")
    print("Sample of processed data:")
    print(q_gva_long[['Sector', 'Year_Quarter', 'Date', 'GVA_Constant_Crore']].head())
    
except FileNotFoundError:
    print(f"Error: File not found at {q_gva_file}")
    q_gva_long = pd.DataFrame()
except Exception as e:
    print(f"Error processing quarterly GVA data: {str(e)}")
    print(f"Error details: {type(e).__name__}")
    import traceback
    traceback.print_exc()
    q_gva_long = pd.DataFrame()

# %%
# 5. GVA Growth Data
print("\n--- Processing GVA Growth Data ---")
try:
    gva_growth_file = os.path.join(DATA_DIR, '1.6B.xlsx')
    
    # First, let's preview the data to understand its structure
    preview = pd.read_excel(gva_growth_file, header=None, nrows=6)
    print("File structure preview:")
    print(preview.head(6))

    # Read the actual data - based on the preview
    # The real headers are around rows 2-3, and data starts at row 4
    gva_growth_df = pd.read_excel(
        gva_growth_file,
        header=None,
        skiprows=3  # Skip the title and header rows
    )
    
    print(f"Raw data shape: {gva_growth_df.shape}")
    print("First few columns:", gva_growth_df.columns[:5].tolist())
    
    # The first row may contain header information
    header_row = gva_growth_df.iloc[0].tolist()
    print("Header row:", header_row[:5])
    
    # Set proper column names based on the file structure
    # Column 0 = S.No, Column 1 = Item/Sector, Columns 2-12 = Current prices years, 13+ = Constant prices
    gva_growth_df.columns = range(gva_growth_df.shape[1])
    gva_growth_df = gva_growth_df.iloc[1:]  # Skip the header row if it contains metadata
    
    # Rename key columns
    gva_growth_df.rename(columns={0: 'S.No.', 1: 'Sector'}, inplace=True)
    
    # Get year labels from header_row or use fixed years if not available
    # From the file structure, we know columns 2-12 are current prices (2012-13 to 2022-23)
    # and columns 13+ are constant prices
    current_price_cols = list(range(2, 13))  # Columns for current prices
    constant_price_cols = list(range(13, gva_growth_df.shape[1]))  # Columns for constant prices
    
    # Create mappings for current and constant price columns
    current_years = [f"{2012+i}-{(13+i)%100:02d}" for i in range(11)]  # 2012-13 through 2022-23
    constant_years = [f"{2012+i}-{(13+i)%100:02d}" for i in range(len(constant_price_cols))]  # Starting from 2012-13
    
    # Create a mapping dictionary for current price columns
    current_price_mapping = {
        col_idx: f"{year}_current" 
        for col_idx, year in zip(current_price_cols, current_years)
    }
    
    # Create a mapping dictionary for constant price columns
    constant_price_mapping = {
        col_idx: f"{year}_constant" 
        for col_idx, year in zip(constant_price_cols, constant_years)
    }
    
    # Combine the mappings and update column names
    column_mapping = {**current_price_mapping, **constant_price_mapping}
    gva_growth_df = gva_growth_df.rename(columns=column_mapping)
    
    print("Column names after mapping:")
    print(gva_growth_df.columns.tolist()[:5])
    
    # Extract constant price columns
    constant_growth_cols = [col for col in gva_growth_df.columns if "_constant" in str(col)]
    
    print(f"Found {len(constant_growth_cols)} constant price columns")
    
    # Select "Sector" + constant price columns
    gva_growth_constant = gva_growth_df[["Sector"] + constant_growth_cols].copy()
    
    # Clean column names to just show years
    gva_growth_constant.columns = [
        'Sector' if col == 'Sector' else col.replace("_constant", "")
        for col in gva_growth_constant.columns
    ]
    
    # Clean sector names (remove numbering prefixes)
    gva_growth_constant['Sector'] = (
        gva_growth_constant['Sector']
        .astype(str)
        .str.replace(r'^\d+\.?\d*\s*', '', regex=True)
        .str.strip()
    )
    
    print("Sectors found:", gva_growth_constant['Sector'].tolist()[:5])
    
    # Convert growth rate columns to numeric
    for col in gva_growth_constant.columns:
        if col != 'Sector':
            gva_growth_constant[col] = pd.to_numeric(
                gva_growth_constant[col].astype(str).str.replace(r'[^\d.-]', '', regex=True),
                errors='coerce'
            )
    
    # Remove summary rows
    gva_growth_constant = gva_growth_constant[
        ~gva_growth_constant['Sector'].str.contains('TOTAL GVA|^1\s*$|^2\s*$|^3\s*$', case=False, na=False)
    ]
    
    print(f"Filtered data shape: {gva_growth_constant.shape}")
    
    # Melt to long format
    gva_growth_long = pd.melt(
        gva_growth_constant,
        id_vars=['Sector'],
        var_name='Year',
        value_name='GVA_Growth_Percent'
    )
    
    # Add start year for easier analysis
    gva_growth_long['Year_Start'] = gva_growth_long['Year'].str.split('-').str[0]
    gva_growth_long['Year_Start'] = pd.to_numeric(gva_growth_long['Year_Start'], errors='coerce')
    
    # Sort by sector and year
    gva_growth_long.sort_values(['Sector', 'Year_Start'], inplace=True)
    
    print(f"Final processed data shape: {gva_growth_long.shape}")

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
    gva_growth_long = pd.DataFrame()

# %%
gva_growth_long

# %% [markdown]
# # --- Analysis and Visualization ---

# %%
print("\n--- Generating Visualizations ---")

# %%

# Viz 1: Overall IIP Trend (General Index)
if not iip_long.empty:
    general_iip = iip_long[iip_long['Sector'] == 'General']
    if not general_iip.empty:
        plt.figure(figsize=(14, 7))
        sns.lineplot(data=general_iip, x='Date', y='IIP', marker='o', markersize=4)

        # Highlight COVID dip
        covid_start = pd.Timestamp('2020-03-01')
        covid_end = pd.Timestamp('2020-06-01')
        plt.axvspan(covid_start, covid_end, color='red', alpha=0.2, label='COVID Lockdown Impact (Approx)')

        plt.title('Overall Index of Industrial Production (IIP) Trend (Base: 2011-12=100)', fontsize=16)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('IIP Index', fontsize=12)
        plt.legend()
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)
        save_plot('1_overall_iip_trend.png')

# %%
# Viz 2: IIP Trends for Major Sectors (Mining, Manufacturing, Electricity)
if not iip_long.empty:
    major_sectors = ['Mining', 'Manufacturing', 'Electricity']
    iip_major = iip_long[iip_long['Sector'].isin(major_sectors)]
    if not iip_major.empty:
        plt.figure(figsize=(14, 7))
        sns.lineplot(data=iip_major, x='Date', y='IIP', hue='Sector', marker='.', markersize=5)
        plt.title('IIP Trends for Major Sectors (Base: 2011-12=100)', fontsize=16)
        plt.xlabel('Date', fontsize=12)
        plt.ylabel('IIP Index', fontsize=12)
        plt.legend(title='Sector')
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)
        save_plot('2_major_sector_iip_trends.png')

# %%
# Viz 3: Top 5 and Bottom 5 Manufacturing Sub-sectors (Average Growth post-COVID)
if not iip_long.empty:
    mfg_iip = iip_long[(iip_long['NIC_Code'] >= 10) & (iip_long['NIC_Code'] <= 32)]
    # Calculate average annual growth rate post-COVID (e.g., FY21-22 onwards)
    post_covid_start = pd.Timestamp('2021-04-01')
    mfg_post_covid = mfg_iip[mfg_iip['Date'] >= post_covid_start].copy()

    if not mfg_post_covid.empty:
        # Calculate simple average IIP for the period as a proxy for level
        avg_iip_post_covid = mfg_post_covid.groupby('Sector')['IIP'].mean().sort_values(ascending=False)

        # Or calculate YoY growth rate and average that
        mfg_post_covid['IIP_YoY_Growth'] = mfg_post_covid.groupby('Sector')['IIP'].pct_change(periods=12) * 100
        avg_growth_post_covid = mfg_post_covid.groupby('Sector')['IIP_YoY_Growth'].mean().sort_values(ascending=False).dropna()

        if not avg_growth_post_covid.empty:
            top5 = avg_growth_post_covid.head(5)
            bottom5 = avg_growth_post_covid.tail(5)
            combined = pd.concat([top5, bottom5])

            plt.figure(figsize=(12, 8))
            colors = ['green' if x > 0 else 'red' for x in combined.values]
            sns.barplot(x=combined.values, y=combined.index, palette=colors, orient='h')
            plt.title('Top 5 & Bottom 5 Mfg Sectors by Avg. IIP Growth (Apr 2021 - Mar 2024)', fontsize=14)
            plt.xlabel('Average Monthly YoY Growth (%)', fontsize=12)
            plt.ylabel('Manufacturing Sector', fontsize=12)
            plt.axvline(0, color='grey', linewidth=0.8)
            plt.tight_layout()
            save_plot('3_mfg_sector_growth_post_covid.png')

# %%
# Fixed code for Viz 4: Total GCF plot

# Viz 4: Total Gross Capital Formation (Constant Prices) Trend - FIXED
if not gcf_long.empty:
    # Need the total GCF - let's reload GCF data to get the total row easily
    try:
        # First preview the file to understand its structure
        gcf_preview = pd.read_excel(gcf_file, header=None, nrows=10)
        print("GCF file structure preview:")
        
        # Find the header row containing "Item" column
        header_row = None
        for i in range(10):
            row = gcf_preview.iloc[i].astype(str).str.lower()
            if row.str.contains('item').any():
                header_row = i
                print(f"Found header row at index {i}")
                break
        
        if header_row is None:
            header_row = 4  # Default fallback if not found
        
        # Load with the detected header row
        gcf_df_total = pd.read_excel(gcf_file, header=header_row)
        
        # Find the item/sector column more robustly
        possible_item_cols = [col for col in gcf_df_total.columns 
                             if isinstance(col, str) and ('item' in col.lower() or 'sector' in col.lower())]
        
        if possible_item_cols:
            item_col_total = possible_item_cols[0]
            print(f"Found item/sector column: {item_col_total}")
        else:
            # Try the first column as a fallback
            item_col_total = gcf_df_total.columns[1] if len(gcf_df_total.columns) > 1 else None
            print(f"Using column 1 as item/sector: {item_col_total}")
        
        if item_col_total is not None:
            # Find year columns more robustly
            year_pattern = re.compile(r'20\d{2}-\d{2}')
            constant_price_cols_total = [
                col for col in gcf_df_total.columns 
                if isinstance(col, str) and year_pattern.search(str(col))
            ]
            
            if not constant_price_cols_total and len(gcf_df_total.columns) > 2:
                # If no year columns found by pattern, try using numeric columns
                constant_price_cols_total = [col for col in gcf_df_total.columns[2:] 
                                          if col != item_col_total]
                print(f"Using columns 2+ as year columns: {len(constant_price_cols_total)} columns")
            else:
                print(f"Found {len(constant_price_cols_total)} year columns")
                
            # Find total GCF row - search more flexibly
            total_gcf_mask = gcf_df_total[item_col_total].astype(str).str.contains('Total|GCF', case=False, na=False)
            total_gcf_row = gcf_df_total[total_gcf_mask]
            
            if total_gcf_row.empty:
                # Try finding it by row position - often the last row
                total_gcf_row = gcf_df_total.tail(1)
                print("Using last row as Total GCF row")
            
            if not total_gcf_row.empty:
                # Extract row data and create a clean DataFrame for plotting
                row_data = {}
                row_data['Sector'] = total_gcf_row[item_col_total].values[0]
                
                # Process each year column individually
                for col in constant_price_cols_total:
                    year_label = clean_year_col(col)
                    # Extract the value and clean it
                    value = total_gcf_row[col].values[0]
                    if isinstance(value, str):
                        value = value.replace(',', '').strip()
                        try:
                            value = float(value)
                        except:
                            value = np.nan
                    row_data[year_label] = value
                
                # Create a clean DataFrame
                total_gcf_constant = pd.DataFrame([row_data])
                
                # Melt for plotting
                id_vars = ['Sector']
                value_vars = [col for col in total_gcf_constant.columns if col != 'Sector']
                total_gcf_long = pd.melt(
                    total_gcf_constant, 
                    id_vars=id_vars, 
                    value_vars=value_vars, 
                    var_name='Year', 
                    value_name='GCF_Constant_Crore'
                )
                
                # Extract year for sorting
                total_gcf_long['Year_Start'] = total_gcf_long['Year'].str.extract(r'(\d{4})').astype(int)
                total_gcf_long.sort_values(by='Year_Start', inplace=True)
                
                # Create plot
                plt.figure(figsize=(14, 7))
                
                # Add bar plot for values
                sns.barplot(
                    data=total_gcf_long, 
                    x='Year', 
                    y='GCF_Constant_Crore', 
                    alpha=0.6,
                    color='lightblue'
                )
                
                # Add line plot for trend
                sns.lineplot(
                    data=total_gcf_long, 
                    x='Year', 
                    y='GCF_Constant_Crore', 
                    marker='o',
                    linewidth=2,
                    color='darkblue'
                )
                
                # Add growth rate labels
                for i in range(1, len(total_gcf_long)):
                    current = total_gcf_long.iloc[i]['GCF_Constant_Crore']
                    previous = total_gcf_long.iloc[i-1]['GCF_Constant_Crore']
                    if not (pd.isna(current) or pd.isna(previous) or previous == 0):
                        growth = ((current / previous) - 1) * 100
                        color = 'green' if growth >= 0 else 'red'
                        plt.annotate(
                            f"{growth:.1f}%", 
                            (i, current), 
                            textcoords="offset points",
                            xytext=(0,10), 
                            ha='center',
                            color=color,
                            fontweight='bold'
                        )
                
                plt.title('Total Gross Capital Formation (Investment) Trend (Constant 2011-12 Prices)', fontsize=16)
                plt.xlabel('Fiscal Year', fontsize=12)
                plt.ylabel('GCF (₹ Crore)', fontsize=12)
                plt.xticks(rotation=45)
                plt.gca().yaxis.set_major_formatter(mticker.EngFormatter(unit=' Cr'))
                plt.grid(True, axis='y', linestyle='--', linewidth=0.5)
                plt.tight_layout()
                
                save_plot('4_total_gcf_trend.png')
                print("Total GCF trend plot generated successfully")
            else:
                print("Could not find Total GCF row in the data")
        else:
            print("Could not identify an Item/Sector column in the GCF data")
            
    except Exception as e:
        print(f"Could not generate Total GCF plot: {e}")
        import traceback
        traceback.print_exc()  # Print detailed error info

# %%
# Viz 5: Sectoral Share of GCF (Constant Prices) - Average over last 3 years
if not gcf_constant.empty:
    # Calculate average GCF for the last 3 available years (e.g., 2020-21, 2021-22, 2022-23)
    last_3_years = gcf_constant.columns[-3:]
    if len(last_3_years) == 3:
        gcf_constant['Avg_GCF_Last3Y'] = gcf_constant[last_3_years].mean(axis=1)

        # Select major sectors for pie chart clarity (Top 7 + Others)
        gcf_avg_sorted = gcf_constant[['Sector', 'Avg_GCF_Last3Y']].dropna().sort_values('Avg_GCF_Last3Y', ascending=False)

        # Define key sectors explicitly or dynamically take top N
        # Let's define based on typical NAS categories
        major_gcf_sectors = [
            'Manufacturing',
            'Real estate, ownership of dwelling & professional services',
            'Agriculture, forestry and fishing',
            'Trade, repair, hotels and restaurants',
            'Transport, storage, communication & services related to broadcasting',
            'Construction',
            'Electricity, gas, water supply & other utility services',
            'Public administration and defence'
        ]

        gcf_plot_data = gcf_avg_sorted[gcf_avg_sorted['Sector'].isin(major_gcf_sectors)].copy()

        # Calculate 'Others'
        total_avg_gcf = gcf_avg_sorted['Avg_GCF_Last3Y'].sum()
        major_sum = gcf_plot_data['Avg_GCF_Last3Y'].sum()
        others_gcf = total_avg_gcf - major_sum

        if others_gcf > 0:
             others_row = pd.DataFrame([{'Sector': 'Others', 'Avg_GCF_Last3Y': others_gcf}])
             gcf_plot_data = pd.concat([gcf_plot_data, others_row], ignore_index=True)

        # Filter out very small slices for clarity if needed
        gcf_plot_data = gcf_plot_data[gcf_plot_data['Avg_GCF_Last3Y'] > 0]

        if not gcf_plot_data.empty:
            plt.figure(figsize=(10, 10))
            plt.pie(gcf_plot_data['Avg_GCF_Last3Y'], labels=gcf_plot_data['Sector'], autopct='%1.1f%%', startangle=140, pctdistance=0.85)
            plt.title('Average Sectoral Share of Gross Capital Formation (Investment)\n(Constant 2011-12 Prices, Avg. 2020-21 to 2022-23)', fontsize=16, pad=20)
            centre_circle = plt.Circle((0,0),0.70,fc='white')
            fig = plt.gcf()
            fig.gca().add_artist(centre_circle)
            plt.axis('equal') # Equal aspect ratio ensures that pie is drawn as a circle.
            plt.tight_layout()
            save_plot('5_sectoral_gcf_share_avg_last3y.png', fig=fig)
        else:
             print("No data to plot for GCF sectoral share.")

    else:
        print("Could not find the last 3 years columns for GCF average.")


# %%
# Viz 6: Total Net Value Added (NVA) Trend (Constant Prices)
if not total_nva_long.empty:
    plt.figure(figsize=(12, 6))
    sns.lineplot(data=total_nva_long, x='Year', y='NVA_Constant_Crore', marker='o')
    sns.barplot(data=total_nva_long, x='Year', y='NVA_Constant_Crore', alpha=0.6, color='lightblue')
    plt.title('Total Net Value Added (NVA) Trend (Constant 2011-12 Prices)', fontsize=16)
    plt.xlabel('Fiscal Year', fontsize=12)
    plt.ylabel('NVA (₹ Crore)', fontsize=12)
    plt.xticks(rotation=45)
    plt.gca().yaxis.set_major_formatter(mticker.EngFormatter(unit=' Cr'))
    plt.grid(True, axis='y', linestyle='--', linewidth=0.5)
    save_plot('6_total_nva_trend.png')

# %%
# Viz 7: Sectoral Share of NVA (Constant Prices) - Average over last 3 years
if not nva_constant.empty:
    last_3_years_nva = nva_constant.columns[-3:]
    if len(last_3_years_nva) == 3:
        nva_constant['Avg_NVA_Last3Y'] = nva_constant[last_3_years_nva].mean(axis=1)
        nva_avg_sorted = nva_constant[['Sector', 'Avg_NVA_Last3Y']].dropna().sort_values('Avg_NVA_Last3Y', ascending=False)

        # Select major sectors (similar to GCF or based on NVA contribution)
        major_nva_sectors = [
            'Real estate, ownership of dwelling & professional services',
            'Manufacturing',
            'Agriculture, forestry and fishing',
            'Trade, repair, hotels and restaurants',
            'Financial services',
            'Public administration and defence',
            'Construction',
            'Transport, storage, communication & services related to broadcasting'
            #'Other services' # Often large, keep or group into others
        ]

        nva_plot_data = nva_avg_sorted[nva_avg_sorted['Sector'].isin(major_nva_sectors)].copy()
        total_avg_nva = nva_avg_sorted['Avg_NVA_Last3Y'].sum()
        major_sum_nva = nva_plot_data['Avg_NVA_Last3Y'].sum()
        others_nva = total_avg_nva - major_sum_nva

        if others_nva > 0:
            others_row_nva = pd.DataFrame([{'Sector': 'Others', 'Avg_NVA_Last3Y': others_nva}])
            nva_plot_data = pd.concat([nva_plot_data, others_row_nva], ignore_index=True)

        nva_plot_data = nva_plot_data[nva_plot_data['Avg_NVA_Last3Y'] > 0] # Ensure positive values for pie

        if not nva_plot_data.empty:
            plt.figure(figsize=(10, 10))
            plt.pie(nva_plot_data['Avg_NVA_Last3Y'], labels=nva_plot_data['Sector'], autopct='%1.1f%%', startangle=140, pctdistance=0.85)
            plt.title('Average Sectoral Share of Net Value Added (NVA)\n(Constant 2011-12 Prices, Avg. 2020-21 to 2022-23)', fontsize=16, pad=20)
            centre_circle = plt.Circle((0,0),0.70,fc='white')
            fig = plt.gcf()
            fig.gca().add_artist(centre_circle)
            plt.axis('equal')
            plt.tight_layout()
            save_plot('7_sectoral_nva_share_avg_last3y.png', fig=fig)
        else:
             print("No data to plot for NVA sectoral share.")
    else:
        print("Could not find the last 3 years columns for NVA average.")

# %%
# Viz 8: Quarterly GVA YoY Growth for Key Sectors
if not q_gva_long.empty:
    q_gva_key_sectors = [
        'Manufacturing',
        'Construction',
        'Trade, hotels, transport, communication and services related to broadcasting',
        'Financial,  real estate  &  professional  services',
        'Agriculture, Livestock, Forestry and Fishing' # Renaming to match quarterly data's sector name
        ]
    # Adjust sector names based on actual names in q_gva_long['Sector'].unique()
    q_gva_plot_data = q_gva_long[q_gva_long['Sector'].isin(q_gva_key_sectors)].copy()

    if not q_gva_plot_data.empty:
        plt.figure(figsize=(15, 8))
        sns.lineplot(data=q_gva_plot_data, x='Date', y='GVA_YoY_Growth', hue='Sector', marker='o', markersize=3)

        # Highlight COVID period
        covid_start_q = pd.Timestamp('2020-03-01')
        covid_end_q = pd.Timestamp('2021-03-31') # Wider span for quarterly impact visibility
        plt.axvspan(covid_start_q, covid_end_q, color='grey', alpha=0.15, label='COVID Impact Period')

        plt.title('Quarterly GVA Growth (Year-on-Year) for Key Sectors (Constant Prices)', fontsize=16)
        plt.xlabel('Quarter Ending', fontsize=12)
        plt.ylabel('YoY Growth (%)', fontsize=12)
        plt.axhline(0, color='black', linestyle='--', linewidth=0.8)
        plt.legend(title='Sector', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True, which='both', linestyle='--', linewidth=0.5)
        plt.tight_layout(rect=[0, 0, 0.85, 1]) # Adjust layout for legend
        save_plot('8_quarterly_gva_yoy_growth.png')

# %%
# Viz 9: Average Annual GVA Growth (Constant Prices) - Top & Bottom Sectors
if not gva_growth_long.empty:
    # Calculate average growth over the period (e.g., last 5 years: 2018-19 to 2022-23)
    growth_period_start_year = 2018
    avg_growth_period = gva_growth_long[gva_growth_long['Year_Start'] >= growth_period_start_year]

    if not avg_growth_period.empty:
        avg_growth = avg_growth_period.groupby('Sector')['GVA_Growth_Percent'].mean().sort_values(ascending=False).dropna()

        if not avg_growth.empty:
            top7 = avg_growth.head(7)
            bottom7 = avg_growth.tail(7)
            # Avoid showing same sector in top and bottom if only few sectors exist
            bottom7 = bottom7[~bottom7.index.isin(top7.index)]

            combined_growth = pd.concat([top7, bottom7])

            plt.figure(figsize=(12, 9))
            colors = ['#2ca02c' if x > avg_growth.median() else '#d62728' for x in combined_growth.values] # Green/Red based on median
            sns.barplot(x=combined_growth.values, y=combined_growth.index, palette=colors, hue=combined_growth.index , orient='h', legend=False)
            plt.title(f'Top & Bottom Sectors by Avg. Annual GVA Growth ({growth_period_start_year}-{(growth_period_start_year+4)%100} to 2022-23)', fontsize=14)
            plt.xlabel('Average Annual Growth (%) (Constant Prices)', fontsize=12)
            plt.ylabel('Sector', fontsize=12)
            plt.axvline(0, color='grey', linewidth=0.8)
            plt.axvline(avg_growth.median(), color='blue', linestyle=':', linewidth=1, label=f'Median Growth ({avg_growth.median():.1f}%)')
            plt.legend()
            plt.tight_layout()
            save_plot('9_avg_annual_gva_growth_sectors.png')

# %%
# Viz 10: Manufacturing - IIP vs GVA Growth vs GCF Trend

# Requires aligning data: Annual GVA Growth, Annual GCF, and Annualized IIP
if not iip_long.empty and not gcf_long.empty and not gva_growth_long.empty:
    
    # 1. Check for manufacturing in each dataset (with flexible name matching)
    print("Available sectors in GVA Growth data:", gva_growth_long['Sector'].unique())
    print("Available sectors in GCF data:", gcf_long['Sector'].unique())
    print("Available sectors in IIP data:", iip_long['Sector'].unique())
    
    # Find Manufacturing sector with flexible matching - handle NaN values safely
    mfg_sectors_gva = [s for s in gva_growth_long['Sector'].unique() 
                      if isinstance(s, str) and 'manufac' in s.lower()]
    
    # Handle NaN values in gcf_long
    mfg_sectors_gcf = [s for s in gcf_long['Sector'].unique() 
                      if isinstance(s, str) and 'manufac' in s.lower()]
    
    mfg_sectors_iip = [s for s in iip_long['Sector'].unique() 
                      if isinstance(s, str) and 'manufac' in s.lower()]
    
    print(f"Manufacturing sectors in GVA: {mfg_sectors_gva}")
    print(f"Manufacturing sectors in GCF: {mfg_sectors_gcf}")
    print(f"Manufacturing sectors in IIP: {mfg_sectors_iip}")
    
    if mfg_sectors_gva and mfg_sectors_gcf and mfg_sectors_iip:
        # 1. Get Manufacturing GVA Growth
        mfg_gva_growth = gva_growth_long[gva_growth_long['Sector'].isin(mfg_sectors_gva)].copy()
        print(f"GVA Growth data points: {len(mfg_gva_growth)}")
        mfg_gva_by_year = mfg_gva_growth.groupby('Year_Start')['GVA_Growth_Percent'].mean()
        print(f"GVA Growth years: {sorted(mfg_gva_by_year.index.tolist())}")
        
        # 2. Get Manufacturing GCF
        mfg_gcf = gcf_long[gcf_long['Sector'].isin(mfg_sectors_gcf)].copy()
        print(f"GCF data points: {len(mfg_gcf)}")
        mfg_gcf_by_year = mfg_gcf.groupby('Year_Start')['GCF_Constant_Crore'].mean()
        # Calculate year-over-year growth
        mfg_gcf_growth = mfg_gcf_by_year.pct_change() * 100
        print(f"GCF Growth years: {sorted(mfg_gcf_growth.index.tolist())}")
        
        # 3. Calculate Annual Average IIP for Manufacturing
        mfg_iip = iip_long[iip_long['Sector'].isin(mfg_sectors_iip)].copy()
        print(f"IIP data points: {len(mfg_iip)}")
        
        # Convert date to fiscal year (April-March)
        mfg_iip['Year_Start'] = mfg_iip['Date'].dt.year - (mfg_iip['Date'].dt.month < 4)
        
        # Calculate annual average IIP by fiscal year
        annual_mfg_iip = mfg_iip.groupby('Year_Start')['IIP'].mean()
        # Calculate year-over-year growth
        iip_growth = annual_mfg_iip.pct_change() * 100
        print(f"IIP Growth years: {sorted(iip_growth.index.tolist())}")
        
        # 4. Create comprehensive DataFrame for all years in any dataset
        all_years = sorted(set(mfg_gva_by_year.index) | 
                         set(mfg_gcf_growth.index) | 
                         set(iip_growth.index))
        
        print(f"All years found across datasets: {all_years}")
        
        # Create DataFrame with all years, allowing NaN for missing data
        combined_mfg = pd.DataFrame(index=all_years)
        
        # Assign Series objects to DataFrame columns
        combined_mfg['GVA Growth (%)'] = mfg_gva_by_year
        combined_mfg['GCF Growth (%)'] = mfg_gcf_growth
        combined_mfg['IIP Growth (%)'] = iip_growth
        
        print("Combined data before filtering:")
        print(combined_mfg)
        
        # Filter to years where we have at least two metrics (more flexible)
        combined_mfg['Data_Points'] = combined_mfg.notna().sum(axis=1)
        combined_mfg = combined_mfg[combined_mfg['Data_Points'] >= 2].drop(columns=['Data_Points'])
        
        print("Combined data after requiring at least 2 metrics:")
        print(combined_mfg)
        
        if not combined_mfg.empty:
            fig, ax1 = plt.subplots(figsize=(14, 7))
            
            ax1.set_xlabel('Fiscal Year Start')
            ax1.set_ylabel('Growth Rate (%)')
            
            # Plot each metric, handling potential missing values
            if 'GVA Growth (%)' in combined_mfg.columns:
                sns.lineplot(data=combined_mfg.reset_index(), x='index', 
                             y='GVA Growth (%)', ax=ax1, 
                             color='tab:red', label='GVA Growth', marker='o')
            
            if 'IIP Growth (%)' in combined_mfg.columns:
                sns.lineplot(data=combined_mfg.reset_index(), x='index', 
                             y='IIP Growth (%)', ax=ax1,
                             color='tab:blue', label='IIP Growth', marker='s')
            
            if 'GCF Growth (%)' in combined_mfg.columns:
                sns.lineplot(data=combined_mfg.reset_index(), x='index', 
                             y='GCF Growth (%)', ax=ax1,
                             color='tab:green', label='GCF Growth', marker='^')
            
            ax1.axhline(0, color='grey', linestyle=':', linewidth=0.8)
            
            # Set labels for all years
            ax1.set_xticks(combined_mfg.index)
            ax1.set_xticklabels([f"{yr}-{(yr+1)%100}" for yr in combined_mfg.index], rotation=45)
            
            plt.title('Manufacturing Sector: Growth Indicators Comparison (Constant Prices)', fontsize=16)
            plt.legend(loc='upper left')
            plt.tight_layout()
            save_plot('10_manufacturing_growth_comparison.png', fig=fig)
        else:
            print("ERROR: No overlapping years with sufficient data found for Manufacturing visualization.")
    else:
        print("ERROR: Manufacturing sector not found in one or more datasets.")
        missing_datasets = []
        if not mfg_sectors_gva:
            missing_datasets.append("GVA Growth")
        if not mfg_sectors_gcf:
            missing_datasets.append("GCF")
        if not mfg_sectors_iip:
            missing_datasets.append("IIP")
        print(f"Manufacturing not found in: {', '.join(missing_datasets)}")

# %%
# Viz 11: Heatmap of GVA Growth by Sector and Year

plt.figure(figsize=(16, 10))

# Pivot data for heatmap
heatmap_data = gva_growth_long.pivot_table(
    values='GVA_Growth_Percent', 
    index='Sector', 
    columns='Year_Start'
)

# Filter out empty sectors and limit to sectors with sufficient data
filtered_heatmap = heatmap_data.dropna(thresh=5).iloc[:20]  # Show top 20 sectors with at least 5 years of data

# Create heatmap with diverging color palette
cmap = sns.diverging_palette(220, 10, as_cmap=True)
ax = sns.heatmap(
    filtered_heatmap,
    cmap=cmap,
    center=0,  # Center color map at 0
    annot=True,  # Show values
    fmt=".1f",  # Format to 1 decimal place
    linewidths=.5,
    vmin=-15,  # Min value for color scale
    vmax=15    # Max value for color scale
)

plt.title('Sectoral GVA Growth Heat Map (%) - 2012-2022', fontsize=18, pad=20)
plt.xlabel('Year', fontsize=14)
plt.ylabel('Sector', fontsize=14)
plt.xticks(rotation=45)
plt.tight_layout()
save_plot('11_gva_growth_heatmap.png')

# %%
# Viz 12: IIP Seasonal Decomposition

if not iip_long.empty:
    general_iip = iip_long[iip_long['Sector'] == 'General'].copy()
    if not general_iip.empty:
        # Set date as index for time series analysis
        general_iip.set_index('Date', inplace=True)
        monthly_iip = general_iip['IIP'].resample('M').mean()
        
        # Fill any missing months with interpolation
        monthly_iip = monthly_iip.interpolate()
        
        # Perform seasonal decomposition
        from statsmodels.tsa.seasonal import seasonal_decompose
        
        # Decompose time series (adjust model type and period as needed)
        decomposition = seasonal_decompose(monthly_iip, model='additive', period=12)
        
        # Create plot
        fig, axes = plt.subplots(4, 1, figsize=(14, 12), sharex=True)
        
        # Original IIP
        axes[0].plot(decomposition.observed, color='#3366CC')
        axes[0].set_title('Original IIP Index', fontsize=14)
        axes[0].grid(True, linestyle='--', alpha=0.6)
        
        # Trend component
        axes[1].plot(decomposition.trend, color='#109618')
        axes[1].set_title('Trend Component', fontsize=14)
        axes[1].grid(True, linestyle='--', alpha=0.6)
        
        # Seasonal component
        axes[2].plot(decomposition.seasonal, color='#FF9900')
        axes[2].set_title('Seasonal Component', fontsize=14)
        axes[2].grid(True, linestyle='--', alpha=0.6)
        
        # Residual component
        axes[3].plot(decomposition.resid, color='#DC3912')
        axes[3].set_title('Residual Component', fontsize=14)
        axes[3].grid(True, linestyle='--', alpha=0.6)
        
        # COVID annotation
        for i in range(4):
            axes[i].axvspan(pd.Timestamp('2020-03-01'), pd.Timestamp('2020-06-01'), 
                         color='red', alpha=0.2, label='COVID Lockdown')
        
        plt.xlabel('Date', fontsize=12)
        fig.suptitle('IIP Time Series Decomposition', fontsize=18, y=0.92)
        plt.tight_layout()
        save_plot('12_iip_seasonal_decomposition.png', fig=fig)

# %%
# Viz 12: COVID Recovery Index by Sector

# COVID Recovery Index by Sector
if not q_gva_long.empty:
    # Define key sectors to track
    key_sectors = [
        'Manufacturing',
        'Construction',
        'Trade, hotels, transport, communication and services related to broadcasting',
        'Financial, real estate & professional services',
        'Agriculture, Livestock, Forestry and Fishing'
    ]
    
    # Filter data for key sectors
    covid_recovery = q_gva_long[q_gva_long['Sector'].isin(key_sectors)].copy()
    
    # Define pre-COVID reference quarter (Q4 2019 or closest available)
    pre_covid_date = pd.Timestamp('2019-12-31')
    
    # Find closest date to pre-COVID reference for each sector
    baseline_values = {}
    for sector in key_sectors:
        sector_data = covid_recovery[covid_recovery['Sector'] == sector]
        if not sector_data.empty:
            # Find closest date to reference
            closest_date = sector_data.iloc[(sector_data['Date'] - pre_covid_date).abs().argsort()[:1]]['Date'].values[0]
            baseline_value = sector_data[sector_data['Date'] == closest_date]['GVA_Constant_Crore'].values[0]
            baseline_values[sector] = (closest_date, baseline_value)
    
    # Calculate recovery index (current value / pre-COVID value * 100)
    for sector in key_sectors:
        if sector in baseline_values:
            mask = covid_recovery['Sector'] == sector
            baseline_date, baseline_val = baseline_values[sector]
            covid_recovery.loc[mask, 'Recovery_Index'] = covid_recovery.loc[mask, 'GVA_Constant_Crore'] / baseline_val * 100
    
    # Filter to post-COVID period
    post_covid_recovery = covid_recovery[covid_recovery['Date'] >= pd.Timestamp('2020-01-01')].copy()
    
    # Create visualization
    plt.figure(figsize=(15, 8))
    
    # Plot recovery index lines
    sns.lineplot(
        data=post_covid_recovery, 
        x='Date', 
        y='Recovery_Index', 
        hue='Sector',
        marker='o',
        markersize=8,
        linewidth=2.5
    )
    
    # Add reference line at 100% (pre-COVID level)
    plt.axhline(y=100, color='gray', linestyle='--', label='Pre-COVID Level')
    
    # Highlight important periods
    plt.axvspan(pd.Timestamp('2020-03-01'), pd.Timestamp('2020-06-30'), 
                color='red', alpha=0.15, label='Initial COVID Lockdown')
    plt.axvspan(pd.Timestamp('2021-04-01'), pd.Timestamp('2021-06-30'), 
                color='orange', alpha=0.15, label='Second COVID Wave')
    
    plt.title('Sectoral Recovery Index Post-COVID\n(Q4 2019 = 100)', fontsize=18)
    plt.xlabel('Quarter', fontsize=14)
    plt.ylabel('Recovery Index (Pre-COVID Level = 100)', fontsize=14)
    plt.grid(True, alpha=0.3)
    plt.legend(title='Sector', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    # Format y-axis to show percentage
    plt.gca().yaxis.set_major_formatter(mticker.PercentFormatter(decimals=0))
    
    plt.tight_layout()
    save_plot('13_covid_recovery_index.png')

# %%
# Viz 14: Bubble chart showing relationship between GCF, GVA growth and sector size
if not gcf_long.empty and not gva_growth_long.empty:
    # Calculate latest year data
    max_year = min(gcf_long['Year_Start'].max(), gva_growth_long['Year_Start'].max())
    
    # Get GCF for latest year
    latest_gcf = gcf_long[gcf_long['Year_Start'] == max_year]
    
    # Get GVA growth for latest year
    latest_gva_growth = gva_growth_long[gva_growth_long['Year_Start'] == max_year]
    
    # Calculate total GCF to determine sector size
    sector_size = latest_gcf.groupby('Sector')['GCF_Constant_Crore'].sum().reset_index()
    
    # Create dataframe for bubble chart by merging datasets
    # First standardize sector names between datasets
    sector_size['Sector_Std'] = sector_size['Sector'].str.lower().str.strip()
    latest_gva_growth['Sector_Std'] = latest_gva_growth['Sector'].str.lower().str.strip()
    
    # Merge data
    bubble_data = pd.merge(
        sector_size, 
        latest_gva_growth[['Sector', 'Sector_Std', 'GVA_Growth_Percent']], 
        on='Sector_Std', 
        how='inner',
        suffixes=('_GCF', '_GVA')
    )
    
    # Filter out very small sectors for clarity
    bubble_data = bubble_data[bubble_data['GCF_Constant_Crore'] > bubble_data['GCF_Constant_Crore'].quantile(0.1)]
    
    try:
        # Try to import plotly
        import plotly.express as px
        import plotly.io as pio
        
        # Create bubble chart
        fig = px.scatter(
            bubble_data,
            x='GVA_Growth_Percent',
            y='GCF_Constant_Crore',
            size='GCF_Constant_Crore',
            color='GVA_Growth_Percent',
            hover_name='Sector_GCF',
            size_max=60,
            color_continuous_scale=px.colors.sequential.Viridis,
        )
        
        # Customize layout
        fig.update_layout(
            title=f'Investment (GCF) vs Growth (GVA) by Sector - FY {max_year}-{(max_year+1)%100}',
            xaxis_title='GVA Growth (%)',
            yaxis_title='Gross Capital Formation (₹ Crore)',
            height=800,
            width=1000,
            template='plotly_white',
            coloraxis_colorbar=dict(title='GVA Growth (%)'),
        )
        
        # Save as interactive HTML
        pio.write_html(fig, os.path.join(OUTPUT_DIR, '14_investment_growth_bubble.html'))
        print(f"Saved interactive plot: {os.path.join(OUTPUT_DIR, '14_investment_growth_bubble.html')}")
        
        try:
            # Try to save as image
            pio.write_image(fig, os.path.join(OUTPUT_DIR, '14_investment_growth_bubble.png'))
            print(f"Saved static image: {os.path.join(OUTPUT_DIR, '14_investment_growth_bubble.png')}")
        except ValueError:
            print("To save as static image, install the kaleido package: pip install -U kaleido")
            
            # Create a matplotlib fallback version
            plt.figure(figsize=(12, 8))
            sizes = bubble_data['GCF_Constant_Crore'] / bubble_data['GCF_Constant_Crore'].max() * 1000
            
            scatter = plt.scatter(
                bubble_data['GVA_Growth_Percent'],
                bubble_data['GCF_Constant_Crore'],
                s=sizes,
                c=bubble_data['GVA_Growth_Percent'],
                cmap='viridis',
                alpha=0.7,
            )
            
            # Add colorbar
            cbar = plt.colorbar(scatter)
            cbar.set_label('GVA Growth (%)')
            
            # Label key points
            for i, row in bubble_data.iterrows():
                if row['GCF_Constant_Crore'] > bubble_data['GCF_Constant_Crore'].quantile(0.75):
                    plt.annotate(
                        row['Sector_GCF'],
                        (row['GVA_Growth_Percent'], row['GCF_Constant_Crore']),
                        xytext=(5, 5),
                        textcoords='offset points',
                        fontsize=8,
                        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8)
                    )
            
            plt.title(f'Investment (GCF) vs Growth (GVA) by Sector - FY {max_year}-{(max_year+1)%100}')
            plt.xlabel('GVA Growth (%)')
            plt.ylabel('Gross Capital Formation (₹ Crore)')
            plt.grid(True, alpha=0.3)
            plt.tight_layout()
            
            # Save matplotlib version
            save_plot('14_investment_growth_bubble_matplotlib.png')
            
    except ImportError:
        print("Plotly not installed. Using matplotlib instead.")
        # Create scatter plot with matplotlib
        plt.figure(figsize=(12, 8))
        plt.scatter(
            bubble_data['GVA_Growth_Percent'],
            bubble_data['GCF_Constant_Crore'],
            s=bubble_data['GCF_Constant_Crore'] / bubble_data['GCF_Constant_Crore'].max() * 1000,
            c=bubble_data['GVA_Growth_Percent'],
            cmap='viridis',
            alpha=0.7,
        )
        
        plt.title(f'Investment (GCF) vs Growth (GVA) by Sector - FY {max_year}-{(max_year+1)%100}')
        plt.xlabel('GVA Growth (%)')
        plt.ylabel('Gross Capital Formation (₹ Crore)')
        plt.grid(True, alpha=0.3)
        plt.colorbar(label='GVA Growth (%)')
        plt.tight_layout()
        
        save_plot('14_investment_growth_bubble_matplotlib.png')

# %%
# Viz 15:  Stacked area chart showing evolution of sectoral composition

if not nva_long.empty:
    # Group sectors into major categories for clarity
    sector_mapping = {
        'Agriculture, forestry and fishing': 'Agriculture',
        'Mining and quarrying': 'Industry',
        'Manufacturing': 'Industry',
        'Electricity, gas, water supply & other utility services': 'Industry',
        'Construction': 'Construction',
        'Trade, repair, hotels and restaurants': 'Services',
        'Transport, storage, communication & services related to broadcasting': 'Services',
        'Financial services': 'Financial & Real Estate',
        'Real estate, ownership of dwelling & professional services': 'Financial & Real Estate',
        'Public administration and defence': 'Public Admin & Others',
        'Other services': 'Public Admin & Others'
    }
    
    # Apply mapping (with fallback to original name)
    nva_long['Major_Sector'] = nva_long['Sector'].map(lambda x: sector_mapping.get(x, 'Others'))
    
    # Aggregate by Major Sector and Year
    gva_composition = nva_long.groupby(['Major_Sector', 'Year_Start'])['NVA_Constant_Crore'].sum().reset_index()
    
    # Pivot for stacked area chart
    gva_pivot = gva_composition.pivot(index='Year_Start', columns='Major_Sector', values='NVA_Constant_Crore')
    
    # Fill missing values with 0
    gva_pivot = gva_pivot.fillna(0)
    
    # Calculate percentage share
    gva_pct = gva_pivot.div(gva_pivot.sum(axis=1), axis=0) * 100
    
    # Custom color palette
    colors = plt.cm.viridis(np.linspace(0, 1, len(gva_pivot.columns)))
    
    # Create stacked area chart
    plt.figure(figsize=(14, 8))
    plt.stackplot(gva_pivot.index, 
                 [gva_pct[col] for col in gva_pct.columns],
                 labels=gva_pct.columns,
                 colors=colors,
                 alpha=0.8)
    
    plt.title('Evolution of Sectoral Composition in the Economy (2011-2023)', fontsize=18)
    plt.xlabel('Year', fontsize=14)
    plt.ylabel('Share of GVA (%)', fontsize=14)
    plt.xlim(gva_pivot.index.min(), gva_pivot.index.max())
    plt.ylim(0, 100)
    plt.grid(True, alpha=0.3)
    plt.legend(loc='upper left', bbox_to_anchor=(1, 1))
    
    # Add percentage labels at the right side
    for i, col in enumerate(gva_pct.columns):
        # Get last valid value
        last_valid = gva_pct[col].iloc[-1]
        plt.text(gva_pivot.index.max() + 0.5, 
                gva_pct[col].iloc[-1] + sum([gva_pct[c].iloc[-1] for c in gva_pct.columns[:i]]) - gva_pct[col].iloc[-1]/2,
                f"{col}: {last_valid:.1f}%",
                fontsize=10,
                ha='left',
                va='center')
    
    plt.tight_layout()
    save_plot('15_sectoral_composition_evolution.png')

# %%
# Viz 16:  Violin plots showing growth distribution in different time periods
if not gva_growth_long.empty:
    # Define time periods
    gva_growth_long['Period'] = pd.cut(
        gva_growth_long['Year_Start'],
        bins=[2011, 2014, 2019, 2022, 2025],
        labels=['Pre-2014', '2014-2019', '2019-2022', 'Post-2022']
    )
    
    # Remove empty sectors and outliers
    clean_gva_data = gva_growth_long[
        (~gva_growth_long['Sector'].isin(['', 'Item'])) & 
        (gva_growth_long['GVA_Growth_Percent'].between(-30, 30))  # Remove extreme outliers
    ].copy()
    
    # Create violin plot
    plt.figure(figsize=(14, 8))
    
    # Plot with swarmplot overlay
    sns.violinplot(
        x='Period', 
        y='GVA_Growth_Percent', 
        data=clean_gva_data,
        palette='viridis',
        inner='quartile',
        cut=0,
        linewidth=1
    )
    
    # Add individual points for sectors
    sns.swarmplot(
        x='Period', 
        y='GVA_Growth_Percent', 
        data=clean_gva_data,
        size=4,
        color='white',
        edgecolor='gray',
        alpha=0.7
    )
    
    # Add median line
    plt.axhline(y=0, color='red', linestyle='--', alpha=0.7, label='Zero Growth')
    
    plt.title('Distribution of Sectoral Growth Rates Across Time Periods', fontsize=18)
    plt.xlabel('Time Period', fontsize=14)
    plt.ylabel('GVA Growth Rate (%)', fontsize=14)
    plt.grid(True, axis='y', alpha=0.3, linestyle='--')
    
    # Annotate with key statistics
    for i, period in enumerate(clean_gva_data['Period'].unique()):
        period_data = clean_gva_data[clean_gva_data['Period'] == period]['GVA_Growth_Percent']
        if not period_data.empty:
            median = period_data.median()
            plt.annotate(
                f"Median: {median:.1f}%",
                xy=(i, median),
                xytext=(i, median + 1),
                ha='center',
                fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8)
            )
    
    plt.tight_layout()
    save_plot('16_growth_distribution_by_period.png')

# %%
# Viz 17: Radar chart comparing key sectors across multiple metrics

if not iip_long.empty and not gva_growth_long.empty and not gcf_long.empty:
    # Define common sectors across datasets
    common_sectors = ['Manufacturing', 'Construction', 'Agriculture, forestry and fishing']
    
    # Collect metrics for these sectors (latest available data)
    metrics = {}
    
    # 1. IIP Growth (latest year)
    if not iip_long.empty:
        # Calculate year-over-year growth for the latest complete year
        latest_year = iip_long['Date'].dt.year.max() - 1  # Use previous year for complete data
        iip_filtered = iip_long[iip_long['Date'].dt.year.isin([latest_year, latest_year-1])]
        
        # Group by sector and year, get average IIP
        iip_annual = iip_filtered.groupby(['Sector', iip_filtered['Date'].dt.year])['IIP'].mean().reset_index()
        iip_annual.rename(columns={'Date': 'Year'}, inplace=True)
        
        # Calculate growth rates
        for sector in common_sectors:
            sector_data = iip_annual[iip_annual['Sector'] == sector]
            if len(sector_data) >= 2:
                prev_year = sector_data[sector_data['Year'] == latest_year-1]['IIP'].values[0]
                curr_year = sector_data[sector_data['Year'] == latest_year]['IIP'].values[0]
                growth = (curr_year / prev_year - 1) * 100
                if 'IIP_Growth' not in metrics:
                    metrics['IIP_Growth'] = {}
                metrics['IIP_Growth'][sector] = growth
    
    # 2. GVA Growth (latest year)
    if not gva_growth_long.empty:
        latest_year_gva = gva_growth_long['Year_Start'].max()
        for sector in common_sectors:
            # Find closest match in GVA sectors
            matching_sectors = [s for s in gva_growth_long['Sector'].unique() 
                               if isinstance(s, str) and sector.lower() in s.lower()]
            
            if matching_sectors:
                sector_match = matching_sectors[0]
                latest_growth = gva_growth_long[(gva_growth_long['Sector'] == sector_match) & 
                                               (gva_growth_long['Year_Start'] == latest_year_gva)]['GVA_Growth_Percent'].values
                
                if len(latest_growth) > 0:
                    if 'GVA_Growth' not in metrics:
                        metrics['GVA_Growth'] = {}
                    metrics['GVA_Growth'][sector] = latest_growth[0]
    
    # 3. GCF Share (latest year)
    if not gcf_long.empty:
        latest_year_gcf = gcf_long['Year_Start'].max()
        latest_gcf = gcf_long[gcf_long['Year_Start'] == latest_year_gcf]
        
        # Calculate total GCF
        total_gcf = latest_gcf['GCF_Constant_Crore'].sum()
        
        # First ensure all sectors are strings and clean the data
        latest_gcf = latest_gcf.copy()
        latest_gcf['Sector'] = latest_gcf['Sector'].apply(
            lambda x: str(x) if not pd.isna(x) else "Unknown"
        )
        
        for sector in common_sectors:
            # Find closest match with type checking
            matching_sectors = [s for s in latest_gcf['Sector'].unique() 
                               if isinstance(s, str) and sector.lower() in s.lower()]
            
            if matching_sectors:
                sector_match = matching_sectors[0]
                sector_gcf = latest_gcf[latest_gcf['Sector'] == sector_match]['GCF_Constant_Crore'].sum()
                share = (sector_gcf / total_gcf) * 100
                
                if 'GCF_Share' not in metrics:
                    metrics['GCF_Share'] = {}
                metrics['GCF_Share'][sector] = share
    
    # Continue with the radar chart creation only if we have metrics
    if any(metrics.values()):
        # Normalize metrics to 0-1 scale for radar chart
        normalized_metrics = {}
        for metric, values in metrics.items():
            if values:  # Check if dict is not empty
                min_val = min(values.values())
                max_val = max(values.values())
                range_val = max_val - min_val
                
                if range_val > 0:
                    normalized_metrics[metric] = {sector: (val - min_val) / range_val 
                                                for sector, val in values.items()}
                else:
                    normalized_metrics[metric] = values
    
    # Create radar chart
    import matplotlib.pyplot as plt
    from matplotlib.patches import Circle, RegularPolygon
    from matplotlib.path import Path
    from matplotlib.projections.polar import PolarAxes
    from matplotlib.projections import register_projection
    from matplotlib.spines import Spine
    from matplotlib.transforms import Affine2D
    
    def radar_factory(num_vars, frame='circle'):
        """Create a radar chart with `num_vars` axes."""
        # Calculate evenly-spaced axis angles
        theta = np.linspace(0, 2*np.pi, num_vars, endpoint=False)
        
        class RadarAxes(PolarAxes):
            name = 'radar'
            
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.set_theta_zero_location('N')
                
            def fill(self, *args, closed=True, **kwargs):
                """Override fill so that line is closed by default"""
                return super().fill(closed=closed, *args, **kwargs)
                
            def plot(self, *args, **kwargs):
                """Override plot so that line is closed by default"""
                lines = super().plot(*args, **kwargs)
                for line in lines:
                    self._close_line(line)
                return lines
                
            def _close_line(self, line):
                x, y = line.get_data()
                # FIXME: markers at x[0], y[0] get doubled-up
                if x[0] != x[-1]:
                    x = np.concatenate((x, [x[0]]))
                    y = np.concatenate((y, [y[0]]))
                    line.set_data(x, y)
                    
            def set_varlabels(self, labels):
                self.set_thetagrids(np.degrees(theta), labels)
                
            def _gen_axes_patch(self):
                if frame == 'circle':
                    return Circle((0.5, 0.5), 0.5)
                elif frame == 'polygon':
                    return RegularPolygon((0.5, 0.5), num_vars,
                                         radius=.5, edgecolor="k")
                else:
                    raise ValueError("unknown value for 'frame': %s" % frame)
                    
            def draw(self, renderer):
                """ Draw. If frame is polygon, make gridlines polygon-shaped """
                if frame == 'polygon':
                    gridlines = self.yaxis.get_gridlines()
                    for gl in gridlines:
                        gl.get_path()._interpolation_steps = num_vars
                super().draw(renderer)
                
            def _gen_axes_spines(self):
                if frame == 'circle':
                    return super()._gen_axes_spines()
                elif frame == 'polygon':
                    # spine_type must be 'left'/'right'/'top'/'bottom'/'circle'.
                    spine = Spine(axes=self,
                                 spine_type='circle',
                                 path=Path.unit_regular_polygon(num_vars))
                    # unit_regular_polygon returns a polygon with radius 1 at the origin
                    spine.set_transform(Affine2D().scale(.5).translate(.5, .5) + self.transAxes)
                    return {'polar': spine}
                else:
                    raise ValueError("unknown value for 'frame': %s" % frame)
        
        register_projection(RadarAxes)
        return theta
    
    # Prepare data for radar chart
    metrics_names = list(normalized_metrics.keys())
    theta = radar_factory(len(metrics_names), frame='polygon')
    
    # Create figure
    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='radar'))
    
    # Plot each sector
    colors = ['b', 'g', 'r', 'c', 'm']
    for i, sector in enumerate(common_sectors):
        values = [normalized_metrics[metric].get(sector, 0) for metric in metrics_names]
        ax.plot(theta, values, color=colors[i % len(colors)], linewidth=2, label=sector)
        ax.fill(theta, values, color=colors[i % len(colors)], alpha=0.1)
    
    # Customize plot
    ax.set_varlabels(metrics_names)
    ax.set_title('Multi-Dimensional Sector Performance Comparison', fontsize=16, y=1.1)
    
    # Add original values in the legend
    legend_labels = []
    for sector in common_sectors:
        metrics_text = []
        for metric in metrics_names:
            if metric in metrics and sector in metrics[metric]:
                metrics_text.append(f"{metric}: {metrics[metric][sector]:.1f}")
        legend_labels.append(f"{sector}\n" + "\n".join(metrics_text))
        
    plt.legend(legend_labels, loc='upper right', bbox_to_anchor=(0.1, 0.1))
    
    plt.tight_layout()
    save_plot('17_sector_radar_comparison.png', fig=fig)

# %%
# Viz 18: Investment Intensity Analysis (GCF to GVA ratio by sector)
if not gcf_long.empty and not nva_long.empty:
    # Get latest year common to both datasets
    max_gcf_year = gcf_long['Year_Start'].max()
    max_nva_year = nva_long['Year_Start'].max()
    common_year = min(max_gcf_year, max_nva_year)
    
    # Filter data for the common year
    gcf_latest = gcf_long[gcf_long['Year_Start'] == common_year]
    nva_latest = nva_long[nva_long['Year_Start'] == common_year]
    
    # Standardize sector names for merging
    gcf_latest['Sector_Std'] = gcf_latest['Sector'].str.lower().str.strip()
    nva_latest['Sector_Std'] = nva_latest['Sector'].str.lower().str.strip()
    
    # Aggregate by standardized sectors
    gcf_agg = gcf_latest.groupby('Sector_Std')['GCF_Constant_Crore'].sum().reset_index()
    nva_agg = nva_latest.groupby('Sector_Std')['NVA_Constant_Crore'].sum().reset_index()
    
    # Merge datasets
    intensity_df = pd.merge(
        gcf_agg, 
        nva_agg, 
        on='Sector_Std',
        how='inner'
    )
    
    # Calculate investment intensity ratio
    intensity_df['Investment_Intensity'] = intensity_df['GCF_Constant_Crore'] / intensity_df['NVA_Constant_Crore'] * 100
    
    # Sort by intensity
    intensity_df.sort_values('Investment_Intensity', ascending=False, inplace=True)
    
    # Filter to top 15 sectors for readability
    plot_df = intensity_df.head(15)
    
    # Create horizontal bar chart
    plt.figure(figsize=(14, 10))
    bars = plt.barh(
        plot_df['Sector_Std'], 
        plot_df['Investment_Intensity'],
        color=plt.cm.viridis(np.linspace(0, 0.9, len(plot_df))),
        height=0.7
    )
    
    # Add value labels
    for bar in bars:
        width = bar.get_width()
        label_x_pos = width + 1
        plt.text(label_x_pos, bar.get_y() + bar.get_height()/2, f'{width:.1f}%',
                 va='center', fontsize=9)
    
    plt.title(f'Investment Intensity Ratio by Sector (FY {common_year}-{(common_year+1)%100})\nGCF as % of Sector GVA', 
              fontsize=16, pad=20)
    plt.xlabel('Investment Intensity (%)', fontsize=12)
    plt.grid(axis='x', linestyle='--', alpha=0.7)
    plt.xlim(0, plot_df['Investment_Intensity'].max() * 1.15)  # Add extra space for labels
    plt.tight_layout()
    
    save_plot('18_investment_intensity_ratio.png')

# %%
# 19: Moving Average Trend Analysis of Key Sectors
if not nva_long.empty:
    # Define key sectors to analyze
    key_sectors = [
        'Manufacturing', 
        'Agriculture, forestry and fishing',
        'Construction',
        'Trade, repair, hotels and restaurants',
        'Financial services'
    ]
    
    # Filter data for these sectors
    key_sectors_data = nva_long[nva_long['Sector'].isin(key_sectors)].copy()
    
    # Calculate growth rates for each sector and year
    growth_data = []
    
    for sector in key_sectors:
        sector_data = key_sectors_data[key_sectors_data['Sector'] == sector].sort_values('Year_Start')
        
        # Calculate year-over-year growth
        for i in range(1, len(sector_data)):
            prev_value = sector_data.iloc[i-1]['NVA_Constant_Crore']
            curr_value = sector_data.iloc[i]['NVA_Constant_Crore']
            
            if prev_value > 0:
                growth_pct = (curr_value / prev_value - 1) * 100
                
                growth_data.append({
                    'Sector': sector,
                    'Year_Start': sector_data.iloc[i]['Year_Start'],
                    'Growth_Pct': growth_pct
                })
    
    growth_df = pd.DataFrame(growth_data)
    
    # Calculate 3-year moving average
    sectors_with_ma = []
    
    for sector in key_sectors:
        sector_growth = growth_df[growth_df['Sector'] == sector].sort_values('Year_Start')
        
        if len(sector_growth) >= 3:
            sector_growth['MA_3Y'] = sector_growth['Growth_Pct'].rolling(window=3).mean()
            sectors_with_ma.append(sector_growth)
    
    if sectors_with_ma:
        ma_df = pd.concat(sectors_with_ma)
        
        # Create trend plot with both actual and moving average
        plt.figure(figsize=(14, 8))
        
        # Plot each sector
        for i, sector in enumerate(key_sectors):
            sector_data = ma_df[ma_df['Sector'] == sector].sort_values('Year_Start')
            
            if not sector_data.empty:
                color = plt.cm.tab10(i)
                
                # Plot actual growth as scatter points
                plt.plot(
                    sector_data['Year_Start'],
                    sector_data['Growth_Pct'],
                    'o-',
                    color=color,
                    alpha=0.3,
                    label=f"{sector} (Actual)"
                )
                
                # Plot moving average as solid line
                plt.plot(
                    sector_data['Year_Start'],
                    sector_data['MA_3Y'],
                    '-',
                    color=color,
                    linewidth=2.5,
                    label=f"{sector} (3Y Moving Avg)"
                )
        
        # Add horizontal line at zero
        plt.axhline(y=0, color='black', linestyle='-', alpha=0.2)
        
        # Add vertical lines at significant events if applicable
        plt.axvline(x=2014, color='gray', linestyle='--', alpha=0.5, label='2014 Election')
        plt.axvline(x=2016, color='gray', linestyle=':', alpha=0.5, label='Demonetization')
        plt.axvline(x=2020, color='red', linestyle='--', alpha=0.5, label='COVID-19')
        
        plt.title('Sectoral Growth Trends with 3-Year Moving Average', fontsize=16)
        plt.xlabel('Year', fontsize=12)
        plt.ylabel('Growth Rate (%)', fontsize=12)
        plt.grid(True, alpha=0.3)
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        
        save_plot('19_sectoral_growth_ma_trends.png')

# %%
# Viz 20: Sector Performance Heat Grid - Robust against data type issues
if not gva_growth_long.empty and not gcf_long.empty:
    # Define key sectors to look for
    key_sectors = [
        'Manufacturing', 
        'Agriculture, forestry and fishing',
        'Construction', 
        'Financial services',
        'Real estate'
    ]
    
    # Get latest year data for each dataset
    latest_year_gva = gva_growth_long['Year_Start'].max()
    latest_year_gcf = gcf_long['Year_Start'].max()
    
    # Create dictionary to store metrics for each sector
    sector_metrics = {}
    
    # Extract GVA growth for key sectors
    for sector_keyword in key_sectors:
        # Find sectors that contain the keyword (case insensitive)
        gva_matches = gva_growth_long[
            gva_growth_long['Sector'].astype(str).str.contains(
                sector_keyword, case=False, na=False
            )
        ]
        
        # Filter for latest year
        latest_gva = gva_matches[gva_matches['Year_Start'] == latest_year_gva]
        
        if not latest_gva.empty:
            # Get growth rate
            growth = latest_gva['GVA_Growth_Percent'].mean()
            
            # Initialize sector in dictionary if not exists
            if sector_keyword not in sector_metrics:
                sector_metrics[sector_keyword] = {}
            
            sector_metrics[sector_keyword]['GVA_Growth'] = growth
    
    # Extract GCF share for key sectors
    total_gcf = gcf_long[gcf_long['Year_Start'] == latest_year_gcf]['GCF_Constant_Crore'].sum()
    
    for sector_keyword in key_sectors:
        # Find sectors that contain the keyword
        gcf_matches = gcf_long[
            gcf_long['Sector'].astype(str).str.contains(
                sector_keyword, case=False, na=False
            )
        ]
        
        # Filter for latest year
        latest_gcf = gcf_matches[gcf_matches['Year_Start'] == latest_year_gcf]
        
        if not latest_gcf.empty and total_gcf > 0:
            # Calculate share
            sector_gcf = latest_gcf['GCF_Constant_Crore'].sum()
            share = (sector_gcf / total_gcf) * 100
            
            # Add to dictionary
            if sector_keyword not in sector_metrics:
                sector_metrics[sector_keyword] = {}
            
            sector_metrics[sector_keyword]['GCF_Share'] = share
    
    # Create a dataframe for visualization
    metrics_data = []
    
    for sector, metrics in sector_metrics.items():
        row = {'Sector': sector}
        row.update(metrics)
        metrics_data.append(row)
    
    df_metrics = pd.DataFrame(metrics_data)
    
    # Create visualization if we have data
    if not df_metrics.empty:
        # Set up plot
        plt.figure(figsize=(14, 9))
        
        # Define color mapping for growth
        cmap_growth = plt.cm.RdYlGn  # Red (negative) to Green (positive)
        
        # Plot sectors as rows with metrics as colored cells
        sectors = df_metrics['Sector']
        metrics_cols = [col for col in df_metrics.columns if col != 'Sector']
        
        # Set up grid
        n_sectors = len(sectors)
        n_metrics = len(metrics_cols)
        
        # Create grid of rectangles
        for i, sector in enumerate(sectors):
            for j, metric in enumerate(metrics_cols):
                if metric in df_metrics.loc[df_metrics['Sector'] == sector].iloc[0]:
                    value = df_metrics.loc[df_metrics['Sector'] == sector, metric].iloc[0]
                    
                    # Determine color based on metric type
                    if 'Growth' in metric:
                        # Normalize growth between -10 and +10 for color mapping
                        norm_value = max(min(value, 10), -10) / 10  # Scale to [-1, 1]
                        color = cmap_growth(0.5 + norm_value/2)  # Map to [0, 1]
                    else:
                        # For share metrics, use Blues colormap
                        norm_value = min(value / 50, 1)  # Cap at 50% for full color
                        color = plt.cm.Blues(norm_value)
                    
                    # Draw rectangle
                    rect = plt.Rectangle((j, i), 0.8, 0.8, color=color, alpha=0.8)
                    plt.gca().add_patch(rect)
                    
                    # Add value text
                    plt.text(j + 0.4, i + 0.4, f'{value:.1f}%', 
                             ha='center', va='center',
                             color='white' if abs(value) > 5 else 'black',
                             fontweight='bold')
        
        # Set up axis
        plt.xlim(-0.2, n_metrics - 0.2)
        plt.ylim(-0.2, n_sectors - 0.2)
        plt.gca().set_xticks(range(n_metrics))
        plt.gca().set_yticks(range(n_sectors))
        plt.gca().set_xticklabels(metrics_cols, fontsize=12)
        plt.gca().set_yticklabels(sectors, fontsize=12)
        plt.gca().set_aspect('equal')
        
        plt.title('Sector Performance Metrics (Latest Available Year)', fontsize=16, pad=20)
        
        # Add legend for growth coloring
        sm_growth = plt.cm.ScalarMappable(cmap=cmap_growth, 
                                         norm=plt.Normalize(-10, 10))
        sm_growth.set_array([])
        cbar_growth = plt.colorbar(sm_growth, ax=plt.gca(), 
                                 orientation='horizontal', 
                                 pad=0.05, aspect=40)
        cbar_growth.set_label('Growth Rate (%)')
        
        plt.tight_layout()
        save_plot('20_sector_performance_grid.png')

# %%
print("\n--- Analysis and Visualization Complete ---")
print(f"Visualizations saved in '{OUTPUT_DIR}' directory.")


