import pandas as pd
import numpy as np
from faker import Faker
import random
import json
from datetime import datetime, timedelta
import os

fake = Faker()

# --- Configuration ---
num_new_clients = 10
date_range_days = 90 # Generate data for 90 days
start_date = datetime(2024, 12, 1)

# --- File Paths (Constants) ---

# Get the directory where the current script is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Construct the path to the data/raw directory relative to the script's directory
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data', 'raw') # Go up one level ('..') then down into 'data/raw'

CLIENTS_FILE = os.path.join(DATA_DIR, 'clients.csv')
GOOGLE_ADS_FILE = os.path.join(DATA_DIR, 'google_ads.csv')
FACEBOOK_ADS_FILE = os.path.join(DATA_DIR, 'facebook_ads.json')
EMAIL_CAMPAIGNS_FILE = os.path.join(DATA_DIR, 'email_campaigns.csv')
WEB_TRAFFIC_FILE = os.path.join(DATA_DIR, 'web_traffic.csv')
REVENUE_FILE = os.path.join(DATA_DIR, 'revenue.csv')

# --- Ensure Data Directory Exists ---
# Using exist_ok=True prevents an error if the directory already exists
os.makedirs(DATA_DIR, exist_ok=True)
print(f"Ensured directory exists: {DATA_DIR}")


# --- Helper function to read existing data safely ---
def read_csv_safely(filepath, columns=None):
    try:
        df = pd.read_csv(filepath)
        print(f"Successfully read {len(df)} records from {filepath}")
        return df
    except FileNotFoundError:
        print(f"Warning: {filepath} not found. Starting with an empty DataFrame.")
        # If columns are provided, create an empty DataFrame with those columns
        if columns is not None:
             return pd.DataFrame(columns=columns)
        else:
             return pd.DataFrame() # Return a completely empty DataFrame if columns aren't known yet
    except pd.errors.EmptyDataError:
        print(f"Warning: {filepath} is empty. Starting with an empty DataFrame.")
        if columns is not None:
             return pd.DataFrame(columns=columns)
        else:
             return pd.DataFrame()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        if columns is not None:
             return pd.DataFrame(columns=columns)
        else:
             return pd.DataFrame()


def read_json_safely(filepath):
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
        print(f"Successfully read {len(data)} records from {filepath}")
        return data
    except FileNotFoundError:
        print(f"Warning: {filepath} not found. Starting with an empty list.")
        return []
    except json.JSONDecodeError:
        print(f"Warning: {filepath} is empty or invalid JSON. Starting with an empty list.")
        return []
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return []

# --- 1. Generate Clients ---
# Provide expected columns for the first read in case the file is empty
existing_clients_df = read_csv_safely(CLIENTS_FILE, columns=['client_id', 'name', 'industry', 'account_manager', 'signup_date'])

# Handle case where existing_clients_df is empty (first run)
if existing_clients_df.empty:
     print("No existing clients found. Generating initial clients.")
     start_client_id_num = 101 # Start numbering from C101
     # If starting fresh, we don't need to find max_client_id
else:
    # Find the maximum client ID number to continue numbering
    # Assumes client IDs are in the format CXXX and are all numbers after 'C'
    try:
        max_client_id = existing_clients_df['client_id'].str.replace('C', '').astype(int).max()
        start_client_id_num = max_client_id + 1
    except (AttributeError, ValueError, KeyError):
        print("Could not determine max client ID from existing data. Starting client numbering from 101.")
        start_client_id_num = 101 # Fallback in case of unexpected format


new_clients_data = []
for i in range(num_new_clients):
    client_id = f'C{start_client_id_num + i}'
    name = fake.company()
    industry = random.choice(['Tech', 'Retail', 'Finance', 'Healthcare', 'Hospitality', 'SaaS', 'E-commerce', 'Manufacturing', 'Education', 'Marketing', 'Consulting']) # Added more industries
    account_manager = fake.name()
    signup_date = fake.date_between(start_date - timedelta(days=365*4), start_date - timedelta(days=60)) # Signup up to 4 years prior, at least 2 months before data

    new_clients_data.append([client_id, name, industry, account_manager, signup_date.strftime('%Y-%m-%d')]) # Format date


new_clients_df = pd.DataFrame(new_clients_data, columns=existing_clients_df.columns)
all_clients_df = pd.concat([existing_clients_df, new_clients_df], ignore_index=True)

# Save updated clients.csv
all_clients_df.to_csv(CLIENTS_FILE, index=False)
print(f"Generated {len(new_clients_df)} new clients. Total clients: {len(all_clients_df)}")

client_ids = all_clients_df['client_id'].tolist()

# --- 2. Define Date Range ---
dates = [start_date + timedelta(days=i) for i in range(date_range_days)]

# --- 3 & 4. Generate Time-Series Data ---

google_ads_data = []
facebook_ads_data = []
email_campaigns_data = []
web_traffic_data = []
revenue_data = []

# Track campaign/email IDs per client to potentially reuse them
client_campaigns = {cid: {'google': set(), 'facebook': set(), 'email': set()} for cid in client_ids} # Use sets for faster checking

# Load existing campaign/email IDs to potentially reuse them
existing_google_ads_df = read_csv_safely(GOOGLE_ADS_FILE, columns=['campaign_id', 'client_id', 'date', 'clicks', 'impressions', 'cost_usd', 'device_type', 'geo'])
if not existing_google_ads_df.empty:
    for index, row in existing_google_ads_df.iterrows():
         if row['client_id'] in client_campaigns:
              client_campaigns[row['client_id']]['google'].add(row['campaign_id'])

existing_facebook_ads_data = read_json_safely(FACEBOOK_ADS_FILE)
if existing_facebook_ads_data:
    for record in existing_facebook_ads_data:
         client_id = record.get('client')
         fb_campaign_id = record.get('fb_campaign_id')
         if client_id and fb_campaign_id and client_id in client_campaigns:
             client_campaigns[client_id]['facebook'].add(fb_campaign_id)


existing_email_campaigns_df = read_csv_safely(EMAIL_CAMPAIGNS_FILE, columns=['email_id', 'client_id', 'date', 'emails_sent', 'open_rate', 'click_rate', 'subject_line'])
if not existing_email_campaigns_df.empty:
     for index, row in existing_email_campaigns_df.iterrows():
         if row['client_id'] in client_campaigns:
              client_campaigns[row['client_id']]['email'].add(row['email_id'])


for current_date in dates:
    # Select a subset of clients for this date to make it more realistic
    # More active clients as time progresses or based on other factors
    proportion_active = 0.6 + (dates.index(current_date) / date_range_days) * 0.3 # Increase from 60% to 90% over the date range
    k_active = max(1, int(len(client_ids) * proportion_active))
    active_clients_today = random.sample(client_ids, k=k_active)

    # Dictionaries to store daily aggregates needed for revenue attribution
    daily_ga_clicks = {}
    daily_fb_clicks = {}
    daily_email_clicks = {}
    daily_organic_sessions = {}

    for client_id in active_clients_today:
        # Ensure data is generated after client signup
        signup_dt_str = all_clients_df[all_clients_df['client_id'] == client_id]['signup_date'].iloc[0]
        signup_dt = datetime.strptime(signup_dt_str, '%Y-%m-%d').date()
        if current_date.date() < signup_dt:
            continue # Skip if date is before signup

        # Initialize daily aggregates for this client
        daily_ga_clicks[client_id] = 0
        daily_fb_clicks[client_id] = 0
        daily_email_clicks[client_id] = 0
        daily_organic_sessions[client_id] = 0


        # --- Generate Google Ads Data ---
        if random.random() < 0.85: # Increased chance of activity
             num_ga_campaigns = random.randint(1, 4) # More potential campaigns
             for i in range(num_ga_campaigns):
                 campaign_id = random.choice(list(client_campaigns[client_id]['google'])) if client_campaigns[client_id]['google'] and random.random() < 0.7 else f'G{random.randint(10000, 99999)}' # Reuse or new
                 client_campaigns[client_id]['google'].add(campaign_id) # Add to set in case it was new

                 clicks = random.randint(20, 1000) # Increased range
                 impressions = clicks * random.randint(12, 70)
                 cost_usd = clicks * random.uniform(0.6, 3.0)
                 device_type = random.choice(['mobile', 'desktop', 'tablet'])
                 geo = random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'AU', 'NZ', 'IE', 'ES', 'IT']) # More geos
                 google_ads_data.append([campaign_id, client_id, current_date.strftime('%Y-%m-%d'), clicks, impressions, round(cost_usd, 2), device_type, geo])
                 daily_ga_clicks[client_id] += clicks


        # --- Generate Facebook Ads Data ---
        if random.random() < 0.75: # Increased chance of activity
             num_fb_campaigns = random.randint(1, 3)
             for i in range(num_fb_campaigns):
                 fb_campaign_id = random.choice(list(client_campaigns[client_id]['facebook'])) if client_campaigns[client_id]['facebook'] and random.random() < 0.6 else f'FB{random.randint(10000, 99999)}'
                 client_campaigns[client_id]['facebook'].add(fb_campaign_id)

                 clicks = random.randint(30, 800)
                 reach = clicks * random.randint(20, 80)
                 spend = clicks * random.uniform(0.9, 3.5)
                 platform = random.choice(['Facebook', 'Instagram', 'Audience Network', 'Messenger'])
                 geo = random.choice(['US', 'CA', 'UK', 'DE', 'FR', 'AU', 'NZ', 'IE', 'ES', 'IT'])
                 facebook_ads_data.append({
                     "fb_campaign_id": fb_campaign_id,
                     "client": client_id,
                     "date": current_date.strftime('%Y-%m-%d'),
                     "clicks": clicks,
                     "reach": reach,
                     "spend": round(spend, 2),
                     "platform": platform,
                     "geo": geo
                 })
                 daily_fb_clicks[client_id] += clicks


        # --- Generate Email Campaign Data ---
        if random.random() < 0.6: # Increased chance of activity
             num_emails = random.randint(1, 3) # More potential emails
             for i in range(num_emails):
                 email_id = random.choice(list(client_campaigns[client_id]['email'])) if client_campaigns[client_id]['email'] and random.random() < 0.5 else f'E{random.randint(10000, 99999)}'
                 client_campaigns[client_id]['email'].add(email_id)

                 emails_sent = random.randint(10000, 50000)
                 open_rate = round(random.uniform(0.20, 0.50), 2)
                 click_rate = round(random.uniform(0.05, open_rate * random.uniform(0.45, 0.7)), 2)
                 subject_line = fake.catch_phrase() + " " + fake.word() # More varied subject lines
                 email_campaigns_data.append([email_id, client_id, current_date.strftime('%Y-%m-%d'), emails_sent, open_rate, click_rate, subject_line])
                 daily_email_clicks[client_id] += int(emails_sent * click_rate) # Calculate clicks from rate


        # --- Generate Web Traffic Data (Organic) ---
        if random.random() < 0.98: # Very high chance of organic traffic
            sessions = random.randint(1000, 10000) # Increased range
            pageviews = sessions * random.uniform(1.3, 3.5)
            bounce_rate = round(random.uniform(0.10, 0.50), 2)
            total_seconds = random.randint(120, 600) # Between 2 and 10 minutes
            minutes, seconds = divmod(total_seconds, 60)
            hours, minutes = divmod(minutes, 60)
            avg_session_duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            web_traffic_data.append([client_id, current_date.strftime('%Y-%m-%d'), int(pageviews), sessions, bounce_rate, avg_session_duration])
            daily_organic_sessions[client_id] += sessions


    # --- Generate Revenue Attribution Data (After all daily activity is generated) ---
    for client_id in active_clients_today:
        if current_date.date() < datetime.strptime(all_clients_df[all_clients_df['client_id'] == client_id]['signup_date'].iloc[0], '%Y-%m-%d').date():
             continue # Skip if date is before signup

        # Only consider channels where there was activity generated
        potential_channels = []
        if daily_ga_clicks.get(client_id, 0) > 0: potential_channels.append('google_ads')
        if daily_fb_clicks.get(client_id, 0) > 0: potential_channels.append('facebook')
        if daily_email_clicks.get(client_id, 0) > 0: potential_channels.append('email')
        if daily_organic_sessions.get(client_id, 0) > 0: potential_channels.append('organic')


        for channel in potential_channels:
             revenue = 0
             if channel == 'google_ads':
                 revenue = daily_ga_clicks[client_id] * random.uniform(4, 18)
             elif channel == 'facebook':
                 revenue = daily_fb_clicks[client_id] * random.uniform(5, 22)
             elif channel == 'email':
                 revenue = daily_email_clicks[client_id] * random.uniform(10, 40)
             elif channel == 'organic':
                 revenue = daily_organic_sessions[client_id] * random.uniform(1, 6)

             if revenue > 0:
                # Add some final daily randomness and round
                revenue = revenue * random.uniform(0.85, 1.15)
                revenue_data.append([client_id, current_date.strftime('%Y-%m-%d'), channel, round(revenue, 2)])


# --- 5. Save Generated Data ---

# Google Ads
google_ads_df = pd.DataFrame(google_ads_data, columns=['campaign_id', 'client_id', 'date', 'clicks', 'impressions', 'cost_usd', 'device_type', 'geo'])
# existing_google_ads_df was already loaded at the start for campaign ID tracking
all_google_ads_df = pd.concat([existing_google_ads_df, google_ads_df], ignore_index=True)
all_google_ads_df.to_csv(GOOGLE_ADS_FILE, index=False)
print(f"Generated {len(google_ads_data)} Google Ads records. Total: {len(all_google_ads_df)}")

# Facebook Ads (Handle JSON)
# existing_facebook_ads_data was already loaded at the start
all_facebook_ads_data = existing_facebook_ads_data + facebook_ads_data
with open(FACEBOOK_ADS_FILE, 'w') as f:
    json.dump(all_facebook_ads_data, f, indent=2)
print(f"Generated {len(facebook_ads_data)} Facebook Ads records. Total: {len(all_facebook_ads_data)}")


# Email Campaigns
email_campaigns_df = pd.DataFrame(email_campaigns_data, columns=['email_id', 'client_id', 'date', 'emails_sent', 'open_rate', 'click_rate', 'subject_line'])
# existing_email_campaigns_df was already loaded at the start
all_email_campaigns_df = pd.concat([existing_email_campaigns_df, email_campaigns_df], ignore_index=True)
all_email_campaigns_df.to_csv(EMAIL_CAMPAIGNS_FILE, index=False)
print(f"Generated {len(email_campaigns_data)} Email Campaign records. Total: {len(all_email_campaigns_df)}")

# Web Traffic
web_traffic_df = pd.DataFrame(web_traffic_data, columns=['client_id', 'date', 'pageviews', 'sessions', 'bounce_rate', 'avg_session_duration'])
existing_web_traffic_df = read_csv_safely(WEB_TRAFFIC_FILE, columns=['client_id', 'date', 'pageviews', 'sessions', 'bounce_rate', 'avg_session_duration']) # Read again to ensure columns if first time
all_web_traffic_df = pd.concat([existing_web_traffic_df, web_traffic_df], ignore_index=True)
all_web_traffic_df.to_csv(WEB_TRAFFIC_FILE, index=False)
print(f"Generated {len(web_traffic_data)} Web Traffic records. Total: {len(all_web_traffic_df)}")

# Revenue Attribution
revenue_df = pd.DataFrame(revenue_data, columns=['client_id', 'date', 'channel', 'attributed_revenue'])
existing_revenue_df = read_csv_safely(REVENUE_FILE, columns=['client_id', 'date', 'channel', 'attributed_revenue']) # Read again to ensure columns if first time
all_revenue_df = pd.concat([existing_revenue_df, revenue_df], ignore_index=True)
all_revenue_df.to_csv(REVENUE_FILE, index=False)
print(f"Generated {len(revenue_data)} Revenue records. Total: {len(all_revenue_df)}")