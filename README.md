# Crop Irrigation and Growth Tracker

A Streamlit-based application for tracking crop irrigation needs and growth stages based on weather and soil data.

## Features

- User authentication (login/register functionality)
- Field data input and storage
- Weather and soil data retrieval from agricultural APIs
- Irrigation calculations based on crop type and growth stage
- Historical data visualization with charts
- Data persistence using JSON files in your GitHub repository

## Setup and Installation

1. Clone this repository
2. Install requirements: `pip install -r requirements.txt`
3. Run the app: `streamlit run app.py`

## Data Persistence

The application stores data using:
- JSON files in a `data/` directory within your repository
- Session state for temporary caching during active sessions

**Benefits of this approach:**
- No external database setup required
- Data is stored directly in your Git repository
- Easy to back up and version control
- Simple to deploy to Streamlit Cloud

## For Deployment on Streamlit Cloud

1. Create a fork of this repository on GitHub
2. Create or verify that these files exist in your repository:
   - `data/users.json`
   - `data/field_data.json`
   - `data/irrigation_records.json`
3. Connect your GitHub repo to Streamlit Cloud
4. Deploy the application
5. Add the following secrets in the Streamlit Cloud dashboard:
   - `AGROMONITORING_API_KEY`: Your API key from agromonitoring.com

### Managing Data

When you make changes in the application:
1. The data is automatically saved to JSON files in the `data/` directory
2. To persist these changes to your GitHub repository:
   - Commit and push the data files to your repository periodically
   - This will preserve your data between deployments

## Configuration

You will need to obtain your own API key from [Agromonitoring API](https://agromonitoring.com/) and add it to your Streamlit secrets or environment variables.

## Technology Stack

- Streamlit for the web interface
- JSON files for data persistence
- Python for backend logic
- Pandas for data manipulation
- Streamlit-geolocation for location input