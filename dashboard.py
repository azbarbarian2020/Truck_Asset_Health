import streamlit as st
import pandas as pd
import altair as alt
import datetime
from datetime import timedelta
from snowflake.snowpark.context import get_active_session

# Set page config with initial sidebar state (expanded but collapsible)
st.set_page_config(
    page_title="Truck Fleet Monitoring", 
    layout="wide", 
    initial_sidebar_state="expanded"  # This makes it collapsible but default to open
)

# Add CSS for styling, highlighting, and responsive layouts
st.markdown("""
<style>
    /* Overall page styling */
    .main {
        background-color: #f9f9fb;
        padding: 1rem;
    }
    
    /* Dashboard title styling */
    .dashboard-title {
        color: #2c3e50;
        text-align: center;
        margin-bottom: 1.5rem;
        padding-bottom: 0.75rem;
        border-bottom: 2px solid #3498db;
    }
    
    /* Section headers */
    .section-header {
        color: #2c3e50;
        margin-top: 2rem;
        margin-bottom: 1rem;
        padding-bottom: 0.5rem;
        border-bottom: 1px solid #e0e0e0;
        font-weight: bold;
    }
    
    /* Card styling for content sections */
    .content-card {
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        padding: 1.5rem;
        margin-bottom: 1.5rem;
    }
    
    /* Alert styling */
    .alert-card {
        border-left: 4px solid #e74c3c;
        background-color: #fdf3f2;
        padding: 1rem;
        margin-bottom: 1rem;
        border-radius: 4px;
    }
    
    .alert-card-success {
        border-left: 4px solid #2ecc71;
        background-color: #eafaf1;
    }
    
    /* Exception highlighting */
    .red-highlight {
        background-color: #ffcccc !important;
        color: #cc0000 !important;
        font-weight: bold !important;
    }
    
    .yellow-highlight {
        background-color: #fff3cd !important;
        color: #856404 !important;
        font-weight: bold !important;
    }
    
    .blue-highlight {
        background-color: #cce5ff !important;
        color: #0066cc !important;
        font-weight: bold !important;
    }
    
    /* Table styling with scrolling */
    .scrollable-table {
        max-height: 400px;
        overflow-y: auto;
        margin-bottom: 20px;
        border-radius: 5px;
        border: 1px solid #e0e0e0;
    }
    
    table {
        width: 100%;
        border-collapse: collapse;
    }
    
    th {
        background-color: #f0f2f6;
        padding: 10px;
        border: 1px solid #ddd;
        text-align: center;
        position: sticky;
        top: 0;
        z-index: 10;
        font-weight: bold;
        color: #2c3e50;
    }
    
    td {
        padding: 10px;
        border: 1px solid #ddd;
        text-align: center;
    }
    
    tr:nth-child(even) {
        background-color: #f8f9fa;
    }
    
    tr:hover {
        background-color: #f1f2f6;
    }
    
    /* Key metrics styling */
    .metric-container {
        display: flex;
        flex-wrap: wrap;
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    
    .metric-card {
        flex: 1;
        min-width: 150px;
        background-color: white;
        border-radius: 8px;
        padding: 1rem;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        text-align: center;
    }
    
    .metric-title {
        font-size: 0.9rem;
        color: #7f8c8d;
        margin-bottom: 0.5rem;
    }
    
    .metric-value {
        font-size: 1.8rem;
        font-weight: bold;
        color: #2c3e50;
    }
    
    /* Improve sidebar styling */
    .sidebar .sidebar-content {
        background-color: #f0f2f6;
        padding: 1.5rem !important;
    }
    
    /* Better button styling */
    .stButton button {
        border-radius: 4px;
        font-weight: 500;
        transition: all 0.2s ease;
    }
    
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
    }
    
    /* Footer styling */
    .footer {
        text-align: center;
        color: #7f8c8d;
        margin-top: 2rem;
        padding-top: 1rem;
        border-top: 1px solid #e0e0e0;
        font-size: 0.9rem;
    }
    
    /* Chart legend */
    .chart-legend {
        font-size: 0.9rem;
        display: flex;
        gap: 1rem;
        margin: 1rem 0;
        flex-wrap: wrap;
    }
    
    .legend-item {
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .legend-color {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        display: inline-block;
    }
    
    /* Tooltip icon */
    .tooltip-icon {
        color: #7f8c8d;
        margin-left: 0.3rem;
        cursor: help;
    }
    
    /* Responsive layout adjustments */
    @media screen and (min-width: 1200px) {
        /* For large screens */
        .dashboard-title {
            font-size: 2.5rem;
        }
        
        .section-header {
            font-size: 1.8rem;
        }
        
        .content-card {
            padding: 2rem;
        }
        
        /* Two-column layout for wide screens */
        .wide-layout {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 1.5rem;
        }
    }
    
    @media screen and (max-width: 1199px) {
        /* For medium screens */
        .dashboard-title {
            font-size: 2rem;
        }
        
        .section-header {
            font-size: 1.5rem;
        }
        
        .content-card {
            padding: 1.5rem;
        }
        
        /* Single column layout */
        .wide-layout {
            display: grid;
            grid-template-columns: 1fr;
        }
    }
    
    @media screen and (max-width: 768px) {
        /* For small screens (tablets) */
        .dashboard-title {
            font-size: 1.8rem;
        }
        
        .section-header {
            font-size: 1.3rem;
        }
        
        .content-card {
            padding: 1rem;
        }
        
        /* Adjust chart height for smaller screens */
        .chart-container {
            height: 200px;
        }
        
        /* Reduce padding in tables */
        td, th {
            padding: 6px;
        }
    }
    
    @media screen and (max-width: 576px) {
        /* For very small screens (phones) */
        .dashboard-title {
            font-size: 1.5rem;
        }
        
        .section-header {
            font-size: 1.2rem;
        }
        
        .metric-container {
            flex-direction: column;
        }
        
        .metric-card {
            margin-bottom: 0.5rem;
        }
        
        /* Further reduce table padding */
        td, th {
            padding: 4px;
            font-size: 0.8rem;
        }
        
        /* Stack buttons vertically on very small screens */
        .button-container {
            flex-direction: column;
        }
    }
</style>
""", unsafe_allow_html=True)

# Function to optimize chart data for better performance
def optimize_chart_data(df, max_points_per_series=200):
    """
    Optimize chart data by sampling if there are too many points.
    This significantly improves rendering performance for large datasets.
    
    Args:
        df: DataFrame with timestamp and multiple series
        max_points_per_series: Maximum number of points to display per truck
        
    Returns:
        Optimized DataFrame with appropriate sampling
    """
    if df.empty:
        return df
        
    # Group by truck ID
    grouped = df.groupby('TRUCK_ID')
    
    # Initialize an empty list to store optimized dataframes
    optimized_dfs = []
    
    # Process each truck's data
    for truck_id, group_df in grouped:
        group_size = len(group_df)
        
        # If the group is already small enough, keep all points
        if group_size <= max_points_per_series:
            optimized_dfs.append(group_df)
        else:
            # Calculate sampling rate to get desired number of points
            sample_rate = max(1, int(group_size / max_points_per_series))
            
            # Always include the first and last points for accurate boundaries
            first_point = group_df.iloc[[0]]
            last_point = group_df.iloc[[-1]]
            
            # Sample the middle points
            middle_points = group_df.iloc[1:-1:sample_rate]
            
            # Combine the three parts
            sampled_df = pd.concat([first_point, middle_points, last_point])
            
            # Ensure we include any points where a threshold was crossed
            # (especially important for failure probability)
            threshold_points = group_df[group_df['FAILURE_PROB'] > 0.5].copy()
            
            # Merge the sampled points with important threshold points
            combined_df = pd.concat([sampled_df, threshold_points]).drop_duplicates()
            
            # Sort by timestamp to ensure correct line plotting
            combined_df = combined_df.sort_values('TIMESTAMP')
            
            optimized_dfs.append(combined_df)
    
    # Combine all optimized dataframes
    if optimized_dfs:
        result_df = pd.concat(optimized_dfs)
        return result_df
    else:
        return df  # Return original if something went wrong

# Connect to Snowflake with enhanced error handling
try:
    session = get_active_session()

    # Display the dashboard title with better styling
    st.markdown('<h1 class="dashboard-title">🚚 Truck Fleet Monitoring Dashboard</h1>', unsafe_allow_html=True)

    # Define the query to get the sensor data
    try:
        # Execute the query with timeout handling
        with st.spinner('Loading data from Snowflake...'):
            query = """
            select 
                TIMESTAMP, 
                TRUCK_ID, 
                EXHAUST_GAS_TEMP, 
                OIL_PRESSURE, 
                BOOST_PRESSURE, 
                OIL_CONTAMINATION, 
                ENGINE_BOOST_RATIO, 
                round(predict_proba_1, 4) as FAILURE_PROB
            from TURBO_DATA_PREDICTIONS_NEW
            Where TIMESTAMP <= '2025-04-08 00:00:00.000' and TIMESTAMP >= '2025-04-07 00:00:00.000' 
            order by TRUCK_ID, TIMESTAMP
            """
            
            df = session.sql(query).to_pandas()
            
            if df.empty:
                st.error("No data was returned from the database. Please check your query parameters.")
                st.stop()
    
    except Exception as query_error:
        st.error(f"Error executing database query: {str(query_error)}")
        st.info("Detailed error information has been logged. Please contact IT support if this issue persists.")
        st.stop()
    
    # Data preprocessing with error handling
    try:
        # Convert timestamp to datetime
        df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
        
        # Get min/max timestamps for time filter
        min_date = df['TIMESTAMP'].min().date()
        max_date = df['TIMESTAMP'].max().date()
        
        # Define the fail probability column name
        fail_prob_col = 'FAILURE_PROB'
        
        # Get the max timestamp from the data for reference time
        max_timestamp = df['TIMESTAMP'].max()
    
    except Exception as preprocessing_error:
        st.error(f"Error preprocessing data: {str(preprocessing_error)}")
        st.write("Debugging information:")
        st.write("DataFrame columns:", df.columns.tolist())
        st.write("DataFrame sample:", df.head(2))
        st.stop()
    
    # ---- SIDEBAR CONTROLS ----
    with st.sidebar:
        st.title("Dashboard Controls")
        
        # Time Range Controls
        st.header("⏰ Time Range")
        
        # Define time window duration options
        st.subheader("Time Window")
        
        # Initialize time window to default 12 hours 
        if 'time_window' not in st.session_state:
            st.session_state.time_window = "12 hours"
            
        # Initialize reference time to 15:00 on 2025-04-07
        if 'reference_time' not in st.session_state:
            # Find the date from the data (should be 2025-04-07)
            default_date = datetime.date(2025, 4, 7)
            # Create a timestamp for 15:00 on that date
            default_end_time = datetime.datetime.combine(default_date, datetime.time(15, 0, 0))
            # Convert to pandas timestamp for consistency
            st.session_state.reference_time = pd.Timestamp(default_end_time)
            
        # Time window selection with buttons in a row
        time_window_cols = st.columns(3)
        with time_window_cols[0]:
            if st.button("3 Hours", 
                       key="btn_3h", 
                       type="primary" if st.session_state.time_window == "3 hours" else "secondary",
                       use_container_width=True):
                st.session_state.time_window = "3 hours"
                st.rerun()
                
        with time_window_cols[1]:
            if st.button("6 Hours", 
                       key="btn_6h", 
                       type="primary" if st.session_state.time_window == "6 hours" else "secondary",
                       use_container_width=True):
                st.session_state.time_window = "6 hours"
                st.rerun()
                
        with time_window_cols[2]:
            if st.button("12 Hours", 
                       key="btn_12h", 
                       type="primary" if st.session_state.time_window == "12 hours" else "secondary",
                       use_container_width=True):
                st.session_state.time_window = "12 hours"
                st.rerun()
        
        # Calculate start and end times based on selected window
        time_window = st.session_state.time_window
            
        # Get window duration in hours
        window_hours = int(time_window.split()[0])
        
        # Calculate end time (reference time) and start time (reference - window)
        end_datetime = st.session_state.reference_time
        start_datetime = end_datetime - timedelta(hours=window_hours)
        
        # Display the currently selected time range
        st.subheader("Selected Time Range")
        st.write(f"**From:** {start_datetime.strftime('%Y-%m-%d %H:%M')}")
        st.write(f"**To:** {end_datetime.strftime('%Y-%m-%d %H:%M')}")
        
        # Time navigation controls
        st.subheader("Time Navigation")
        st.write("**Move time window:**")
        
        # First row of time navigation - larger time jumps
        nav_cols1 = st.columns(2)
        with nav_cols1[0]:
            if st.button("◀◀ -1 Hour", key="nav_hour_back", use_container_width=True):
                st.session_state.reference_time -= timedelta(hours=1)
                st.rerun()
                
        with nav_cols1[1]:
            if st.button("+1 Hour ▶▶", key="nav_hour_forward", use_container_width=True):
                st.session_state.reference_time += timedelta(hours=1)
                st.rerun()
        
        # Second row - medium time jumps
        nav_cols2 = st.columns(2)
        with nav_cols2[0]:
            if st.button("◀ -30 Min", key="nav_30min_back", use_container_width=True):
                st.session_state.reference_time -= timedelta(minutes=30)
                st.rerun()
                
        with nav_cols2[1]:
            if st.button("+30 Min ▶", key="nav_30min_forward", use_container_width=True):
                st.session_state.reference_time += timedelta(minutes=30)
                st.rerun()
        
        # Third row - fine time jumps
        nav_cols3 = st.columns(2)
        with nav_cols3[0]:
            if st.button("◀ -15 Min", key="nav_15min_back", use_container_width=True):
                st.session_state.reference_time -= timedelta(minutes=15)
                st.rerun()
                
        with nav_cols3[1]:
            if st.button("+15 Min ▶", key="nav_15min_forward", use_container_width=True):
                st.session_state.reference_time += timedelta(minutes=15)
                st.rerun()
        
        # Fourth row - finest time jumps
        nav_cols4 = st.columns(2)
        with nav_cols4[0]:
            if st.button("◀ -5 Min", key="nav_5min_back", use_container_width=True):
                st.session_state.reference_time -= timedelta(minutes=5)
                st.rerun()
                
        with nav_cols4[1]:
            if st.button("+5 Min ▶", key="nav_5min_forward", use_container_width=True):
                st.session_state.reference_time += timedelta(minutes=5)
                st.rerun()
        
        # Reset to latest data
        if st.button("⟳ Show Latest Data", type="primary", use_container_width=True):
            st.session_state.reference_time = max_timestamp
            st.rerun()
        
        # Number of rows selection
        st.header("📊 Table Display")
        st.write("**Number of rows to show initially:**")
        
        # Initialize rows_to_display if not already set
        if 'rows_to_display' not in st.session_state:
            st.session_state.rows_to_display = "10"  # Default to 10 rows
        
        # Use columns to ensure equal spacing and size
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("10 rows", 
                         key="btn_10", 
                         type="primary" if st.session_state.rows_to_display == "10" else "secondary",
                         use_container_width=True):
                st.session_state.rows_to_display = "10"
                st.rerun()
                
        with col2:
            if st.button("25 rows", 
                         key="btn_25", 
                         type="primary" if st.session_state.rows_to_display == "25" else "secondary",
                         use_container_width=True):
                st.session_state.rows_to_display = "25"
                st.rerun()
                
        with col3:
            if st.button("50 rows", 
                         key="btn_50", 
                         type="primary" if st.session_state.rows_to_display == "50" else "secondary",
                         use_container_width=True):
                st.session_state.rows_to_display = "50"
                st.rerun()
                
        with col4:
            if st.button("All rows", 
                         key="btn_all", 
                         type="primary" if st.session_state.rows_to_display == "All" else "secondary",
                         use_container_width=True):
                st.session_state.rows_to_display = "All"
                st.rerun()
        
        # Chart Filters - persist selections in session state
        st.header("📈 Chart Filters")
        trucks = sorted(df['TRUCK_ID'].unique())
        try:
            trucks = [int(t) for t in trucks]
        except:
            pass
        
        # Initialize selected_trucks if not already set
        if 'selected_trucks' not in st.session_state:
            st.session_state.selected_trucks = []  # Default to empty (no selection)
        
        # Use multiselect with clear key and store values explicitly
        selected_trucks = st.multiselect(
            "Select Trucks for Charts:", 
            options=trucks, 
            default=st.session_state.selected_trucks,
            key="truck_multiselect"
        )
        
        # Immediately update session state with current selections
        st.session_state.selected_trucks = selected_trucks.copy()
    
    # Filter data based on time range
    filtered_df = df[(df['TIMESTAMP'] >= start_datetime) & (df['TIMESTAMP'] <= end_datetime)]
    
    # Find the FIRST time each truck exceeds the failure probability threshold
    high_failure_alerts = []
    
    # Group by truck_id and find the first occurrence of high failure probability
    first_alerts = {}
    first_failure_times = {}
    for _, row in filtered_df.sort_values('TIMESTAMP').iterrows():
        truck_id = row['TRUCK_ID']
        if row[fail_prob_col] > 0.5 and truck_id not in first_alerts:
            first_alerts[truck_id] = {
                'truck_id': truck_id,
                'timestamp': row['TIMESTAMP'],
                'failure_prob': row[fail_prob_col]
            }
            first_failure_times[truck_id] = row['TIMESTAMP']
    
    # Convert dictionary to list
    high_failure_alerts = list(first_alerts.values())
    
    # Create Fleet Overview Section
    st.markdown('<h2 class="section-header">📊 Fleet Overview</h2>', unsafe_allow_html=True)
    
    # Create metrics summary
    if not filtered_df.empty:
        # Get key metrics
        total_trucks = filtered_df['TRUCK_ID'].nunique()
        avg_failure_prob = filtered_df[fail_prob_col].mean() * 100
        trucks_above_threshold = len(first_failure_times)
        trucks_normal = total_trucks - trucks_above_threshold
        
        # Display metrics in a row
        st.markdown('<div class="content-card metric-container">', unsafe_allow_html=True)
        
        metric_cols = st.columns(4)
        
        with metric_cols[0]:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Total Trucks</div>
                <div class="metric-value">{total_trucks}</div>
            </div>
            """, unsafe_allow_html=True)
            
        with metric_cols[1]:
            st.markdown(f"""
            <div class="metric-card">
                <div class="metric-title">Avg. Failure Probability</div>
                <div class="metric-value">{avg_failure_prob:.1f}%</div>
            </div>
            """, unsafe_allow_html=True)
            
        with metric_cols[2]:
            st.markdown(f"""
            <div class="metric-card" style="background-color: #eafaf1;">
                <div class="metric-title">Normal Status</div>
                <div class="metric-value" style="color: #27ae60;">{trucks_normal}</div>
            </div>
            """, unsafe_allow_html=True)
            
        with metric_cols[3]:
            if trucks_above_threshold > 0:
                alert_color = "#e74c3c"
                alert_bg = "#fdf3f2"
            else:
                alert_color = "#27ae60"
                alert_bg = "#eafaf1"
                
            st.markdown(f"""
            <div class="metric-card" style="background-color: {alert_bg};">
                <div class="metric-title">Alarms Active</div>
                <div class="metric-value" style="color: {alert_color};">{trucks_above_threshold}</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Alerts section with enhanced styling
    st.markdown('<h2 class="section-header">⚠️ Failure Alerts</h2>', unsafe_allow_html=True)
    
    if high_failure_alerts:
        st.markdown('<div class="content-card">', unsafe_allow_html=True)
        st.warning("⚠️ **IMPORTANT:** Failure probability over 50% typically indicates failure is likely to occur within 12 hours.")
        
        # Get the maximum timestamp from the data to calculate time remaining
        max_timestamp = filtered_df['TIMESTAMP'].max()
        
        # Sort alerts by failure probability descending, then by timestamp
        sorted_alerts = sorted(high_failure_alerts, 
                              key=lambda x: (x['failure_prob'], x['timestamp']), 
                              reverse=True)
        
        for alert in sorted_alerts:
            # Calculate time difference between max timestamp and alert time
            alert_time = alert['timestamp']
            time_difference = max_timestamp - alert_time
            
            # Convert to hours and minutes
            hours_difference = time_difference.total_seconds() / 3600
            hours = int(hours_difference)
            minutes = int((hours_difference - hours) * 60)
            
            # Calculate remaining time (from 12 hour prediction window)
            if hours == 0 and minutes == 0:
                # If this is the exact alarm time, show full 12 hours
                remaining_hours = 12
                remaining_minutes = 0
            else:
                # Otherwise, subtract the elapsed time from 12 hours
                total_remaining_minutes = (12 * 60) - ((hours * 60) + minutes)
                remaining_hours = max(0, total_remaining_minutes // 60)
                remaining_minutes = max(0, total_remaining_minutes % 60)
            
            # Format the message with the time remaining
            if hours >= 12:
                time_message = "⚠️ URGENT: Failure may be imminent!"
                urgency_class = "high-urgency"
            elif remaining_hours < 4:
                time_message = f"⚠️ CRITICAL: Failure predicted in less than {remaining_hours}h {remaining_minutes}m"
                urgency_class = "high-urgency"
            else:
                time_message = f"⚠️ Failure predicted in {remaining_hours}h {remaining_minutes}m"
                urgency_class = "medium-urgency"
            
            # Calculate a color intensity based on failure probability
            # Higher probability = more intense red
            intensity = min(255, int(150 + (alert['failure_prob'] * 100)))
            red_intensity = f"rgba({intensity}, 0, 0, 0.15)"
            
            # Format probability for display
            prob_pct = f"{alert['failure_prob']:.1%}"
            
            st.markdown(f"""
            <div class="alert-card" style="background-color: {red_intensity}; border-left-width: 6px;">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <h4>Truck {int(alert['truck_id'])}</h4>
                    <span style="font-weight: bold; color: #e74c3c; font-size: 1.2rem;">{prob_pct} probability</span>
                </div>
                <p><strong>{time_message}</strong></p>
                <p>First exceeded 50% threshold at {alert['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p>Elapsed time since first alert: {hours}h {minutes}m</p>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.markdown("""
        <div class="content-card">
            <div class="alert-card alert-card-success">
                <h4>✅ All systems normal</h4>
                <p>No trucks have exceeded 50% failure probability in the selected time range</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
    
    # Table section with scrolling
    st.markdown('<h2 class="section-header">📋 Latest Sensor Readings</h2>', unsafe_allow_html=True)
    
    try:
        # Get latest reading for each truck
        latest_df = filtered_df.sort_values('TIMESTAMP').groupby('TRUCK_ID').last().reset_index()
        
        # Dynamic row display section
        if not latest_df.empty:
            # Determine number of rows to show initially based on session state
            display_option = st.session_state.rows_to_display
            if display_option == "All":
                rows_to_show = len(latest_df)
            else:
                rows_to_show = min(int(display_option), len(latest_df))
                
            # More precise row height calculation:
            # Each row is exactly 41px high (40px content + 1px border)
            # Header is 41px high
            # We need to add 2px for the container borders
            table_height = rows_to_show * 41 + 41 + 2
            
            # Apply the exact height to make sure we show exactly the number of rows requested
            st.markdown(f"""
            <style>
                .scrollable-table {{
                    height: {table_height}px !important;
                    max-height: none !important;
                    overflow-y: auto;
                }}
            </style>
            """, unsafe_allow_html=True)
            
            # Create content card for the table
            st.markdown('<div class="content-card">', unsafe_allow_html=True)
            
            # Create HTML for the table with careful attention to headers
            table_html = """
            <div class="scrollable-table">
                <table>
                    <thead>
                        <tr>
                            <th style="text-align: center; font-weight: bold;">Truck ID</th>
                            <th style="text-align: center; font-weight: bold;">Timestamp</th>
                            <th style="text-align: center; font-weight: bold;">Failure Probability</th>
                            <th style="text-align: center; font-weight: bold;">Exhaust Gas Temp (°C)</th>
                            <th style="text-align: center; font-weight: bold;">Oil Pressure (kPa)</th>
                            <th style="text-align: center; font-weight: bold;">Boost Pressure (kPa)</th>
                            <th style="text-align: center; font-weight: bold;">Oil Contamination (ppm)</th>
                            <th style="text-align: center; font-weight: bold;">Engine Boost Ratio</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            # Add all rows with exception highlighting
            for _, row in latest_df.iterrows():
                table_html += "<tr>"
                
                # Truck ID
                table_html += f"<td>{int(row['TRUCK_ID'])}</td>"
                
                # Timestamp
                formatted_time = row['TIMESTAMP'].strftime('%Y-%m-%d %H:%M:%S')
                table_html += f"<td>{formatted_time}</td>"
                
                # Failure Probability
                failure_class = 'red-highlight' if row[fail_prob_col] > 0.5 else ''
                table_html += f"<td class='{failure_class}'>{row[fail_prob_col]:.2%}</td>"
                
                # Exhaust Gas Temp - 290 to 400 degrees C
                exhaust_class = 'red-highlight' if row['EXHAUST_GAS_TEMP'] > 400 else ('blue-highlight' if row['EXHAUST_GAS_TEMP'] < 290 else '')
                table_html += f"<td class='{exhaust_class}'>{row['EXHAUST_GAS_TEMP']:.1f}</td>"
                
                # Oil Pressure - 275 to 480 kPa
                oil_pressure_class = 'red-highlight' if row['OIL_PRESSURE'] > 480 else ('blue-highlight' if row['OIL_PRESSURE'] < 275 else '')
                table_html += f"<td class='{oil_pressure_class}'>{row['OIL_PRESSURE']:.1f}</td>"
                
                # Boost Pressure - 100 to 205 kPa
                boost_class = 'red-highlight' if row['BOOST_PRESSURE'] > 205 else ('blue-highlight' if row['BOOST_PRESSURE'] < 100 else '')
                table_html += f"<td class='{boost_class}'>{row['BOOST_PRESSURE']:.1f}</td>"
                
                # Oil Contamination - 0 to 25 ppm
                contamination_class = 'red-highlight' if row['OIL_CONTAMINATION'] > 25 else ('blue-highlight' if row['OIL_CONTAMINATION'] < 0 else '')
                table_html += f"<td class='{contamination_class}'>{row['OIL_CONTAMINATION']:.1f}</td>"
                
                # Engine Boost Ratio - 0.85 to 1.15
                boost_ratio_class = 'red-highlight' if row['ENGINE_BOOST_RATIO'] > 1.15 else ('blue-highlight' if row['ENGINE_BOOST_RATIO'] < 0.85 else '')
                table_html += f"<td class='{boost_ratio_class}'>{row['ENGINE_BOOST_RATIO']:.3f}</td>"
                
                table_html += "</tr>"
            
            table_html += """
                </tbody>
            </table>
            </div>
            """
            
            # Display table
            st.markdown(table_html, unsafe_allow_html=True)
            st.markdown('</div>', unsafe_allow_html=True)
        else:
            st.info("No data available for the selected time range.")
    
    except Exception as table_error:
        st.error(f"Error generating table: {str(table_error)}")
        st.warning("Unable to display the table due to an error. Other dashboard features should still work.")
    
    # # REPLACE YOUR ENTIRE CHART SECTION WITH THIS CODE

    # REPLACE YOUR ENTIRE CHART SECTION WITH THIS CODE

    # REPLACE YOUR ENTIRE CHART SECTION WITH THIS CODE

    # Charts section
    st.markdown('<h2 class="section-header">📈 Sensor Readings Time Series</h2>', unsafe_allow_html=True)
    
    try:
        # Create a simplified chart interpretation guide
        with st.expander("Chart Interpretation Guide"):
            st.markdown("""
            - **Black Line**: Average of all trucks with no alerts
            - **Colored Lines**: Individual trucks (if selected)
            - **Blue Line**: Minimum threshold - values below may indicate issues
            - **Red Line**: Maximum threshold - values above may indicate issues
            - **Green Band**: Acceptable operating range
            - **Dashed Vertical Lines**: When a truck first exceeded 50% failure probability
            
            **Sensor Acceptable Ranges:**
            - Exhaust Gas Temp: 290°C to 400°C
            - Oil Pressure: 275 kPa to 480 kPa
            - Boost Pressure: 100 kPa to 205 kPa
            - Oil Contamination: 0 ppm to 25 ppm
            - Engine Boost Ratio: 0.85 to 1.15
            """)
        
        # Define constants for chart domains
        CHART_DOMAINS = {
            fail_prob_col: [0, 1.2],
            'EXHAUST_GAS_TEMP': [240, 450],
            'OIL_PRESSURE': [200, 550],
            'BOOST_PRESSURE': [50, 250], 
            'OIL_CONTAMINATION': [0, 30],
            'ENGINE_BOOST_RATIO': [0.7, 1.3]
        }
        
        # Filter data based on time range
        chart_df = filtered_df.copy()
        
        # Session state to track visibility options
        if 'show_average' not in st.session_state:
            st.session_state.show_average = True
        if 'show_alerts' not in st.session_state:
            st.session_state.show_alerts = False
        
        # Display visibility control buttons
        st.markdown("### Chart Display Options")
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("Show/Hide Average Line", 
                        type="primary" if st.session_state.show_average else "secondary",
                        use_container_width=True):
                st.session_state.show_average = not st.session_state.show_average
                st.rerun()
        
        with col2:
            if st.button("Show/Hide Alert Trucks", 
                        type="primary" if st.session_state.show_alerts else "secondary",
                        use_container_width=True):
                st.session_state.show_alerts = not st.session_state.show_alerts
                st.rerun()
        
        # Get list of trucks with alerts
        alert_trucks = list(first_failure_times.keys())
        
        # Prepare data for charts
        if not chart_df.empty:
            # Calculate average for all non-alert trucks
            non_alert_trucks = [t for t in chart_df['TRUCK_ID'].unique() if t not in alert_trucks]
            avg_df = None
            
            if non_alert_trucks and st.session_state.show_average:
                # Filter for non-alert trucks and calculate average by timestamp
                non_alert_df = chart_df[chart_df['TRUCK_ID'].isin(non_alert_trucks)]
                if not non_alert_df.empty:
                    avg_df = non_alert_df.groupby('TIMESTAMP').mean().reset_index()
                    avg_df['TRUCK_ID'] = 'Average (No Alerts)'
            
            # Get data for alert trucks if option is enabled
            alert_df = None
            if alert_trucks and st.session_state.show_alerts:
                alert_df = chart_df[chart_df['TRUCK_ID'].isin(alert_trucks)]
            
            # Get data for specifically selected trucks
            selected_df = None
            if selected_trucks:
                selected_df = chart_df[chart_df['TRUCK_ID'].isin(selected_trucks)]
            
            # Combine datasets as needed
            display_dfs = []
            if avg_df is not None and not avg_df.empty:
                display_dfs.append(avg_df)
            if alert_df is not None and not alert_df.empty:
                display_dfs.append(alert_df)
            if selected_df is not None and not selected_df.empty:
                # Only include selected trucks that aren't already in alert_df
                if alert_df is not None:
                    selected_df = selected_df[~selected_df['TRUCK_ID'].isin(alert_trucks)]
                if not selected_df.empty:
                    display_dfs.append(selected_df)
            
            # Combine all dataframes
            if display_dfs:
                combined_df = pd.concat(display_dfs)
                
                # Optimize if needed
                if len(combined_df) > 1000:
                    combined_df = optimize_chart_data(combined_df)
                
                # Create chart function using Streamlit native charts
                def create_streamlit_chart(df, field, title, y_label, min_val=None, max_val=None):
                    # Skip if no data
                    if df.empty:
                        st.write(f"No data available for {title}")
                        return
                    
                    # Create figure using matplotlib
                    import matplotlib.pyplot as plt
                    import matplotlib.dates as mdates
                    from matplotlib.lines import Line2D
                    
                    # REDUCED HEIGHT: Change figsize from (10, 6) to (10, 3) - about half as tall
                    fig, ax = plt.subplots(figsize=(10, 3))
                    
                    # Add acceptable range band if both thresholds exist
                    if min_val is not None and max_val is not None:
                        ax.axhspan(min_val, max_val, alpha=0.2, color='green')
                    
                    # Add threshold lines
                    if min_val is not None:
                        ax.axhline(y=min_val, color='blue', linestyle='--', linewidth=1.5)
                        ax.text(df['TIMESTAMP'].min(), min_val, f"Min: {min_val}", 
                               color='blue', fontsize=8, verticalalignment='bottom')
                    
                    if max_val is not None:
                        ax.axhline(y=max_val, color='red', linestyle='--', linewidth=1.5)
                        ax.text(df['TIMESTAMP'].min(), max_val, f"Max: {max_val}", 
                               color='red', fontsize=8, verticalalignment='top')
                    
                    # Plot each truck's data separately
                    unique_trucks = df['TRUCK_ID'].unique()
                    
                    # Create a color map for trucks
                    import matplotlib.cm as cm
                    import numpy as np
                    
                    # Special handling for average line
                    if 'Average (No Alerts)' in unique_trucks:
                        # Plot average line first in black with greater thickness
                        avg_data = df[df['TRUCK_ID'] == 'Average (No Alerts)']
                        ax.plot(avg_data['TIMESTAMP'], avg_data[field], 
                                color='black', linewidth=3, label='Average (No Alerts)')
                        
                        # Remove from the list to plot separately
                        unique_trucks = [t for t in unique_trucks if t != 'Average (No Alerts)']
                    
                    # Create color map for the remaining trucks
                    colors = cm.tab10(np.linspace(0, 1, len(unique_trucks)))
                    
                    # Plot each truck's data
                    for i, truck_id in enumerate(unique_trucks):
                        truck_data = df[df['TRUCK_ID'] == truck_id]
                        color = colors[i]
                        ax.plot(truck_data['TIMESTAMP'], truck_data[field], 
                                marker='o', markersize=4, linestyle='-',  # Smaller markers
                                linewidth=2, alpha=0.8, label=f"Truck {truck_id}")
                    
                    # Add vertical lines for alert times
                    for truck_id, failure_time in first_failure_times.items():
                        if truck_id in df['TRUCK_ID'].values or (truck_id in alert_trucks and st.session_state.show_alerts):
                            # Find color index for this truck
                            try:
                                truck_index = list(unique_trucks).index(truck_id)
                                color = colors[truck_index]
                            except (ValueError, IndexError):
                                color = 'red'  # Default color if not found
                            
                            # Add vertical line
                            ax.axvline(x=failure_time, color=color, linestyle='--', linewidth=1.5, alpha=0.7)
                            
                            # Add text label - adjust position for smaller chart
                            y_range = ax.get_ylim()
                            y_pos = y_range[1] * 0.9
                            ax.text(failure_time, y_pos, f"Truck {truck_id}", 
                                   color=color, fontsize=8, rotation=90, verticalalignment='top')
                    
                    # Set title and labels with smaller fonts for compact display
                    ax.set_title(title, fontsize=12, fontweight='bold')
                    ax.set_xlabel('Time', fontsize=10)
                    ax.set_ylabel(y_label, fontsize=10)
                    
                    # Format x-axis
                    ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
                    plt.xticks(rotation=45, fontsize=8)
                    plt.yticks(fontsize=8)
                    
                    # Set y-axis limits from our constants
                    if field in CHART_DOMAINS:
                        ax.set_ylim(CHART_DOMAINS[field])
                    
                    # Add grid
                    ax.grid(True, linestyle='--', alpha=0.7)
                    
                    # Add legend if multiple trucks - make it smaller and more compact
                    if len(unique_trucks) > 0 or 'Average (No Alerts)' in df['TRUCK_ID'].values:
                        ax.legend(loc='upper left', framealpha=0.9, fontsize=8, ncol=2)
                    
                    # Adjust layout
                    fig.tight_layout()
                    
                    return fig
                
                # Create and display all charts
                # First row of charts
                st.markdown('<div class="wide-layout">', unsafe_allow_html=True)
                
                # Failure Probability chart
                st.markdown('<div class="content-card">', unsafe_allow_html=True)
                fig1 = create_streamlit_chart(combined_df, fail_prob_col, 'Failure Probability', 'Probability', 0, 1)
                if fig1:
                    st.pyplot(fig1)
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Exhaust Gas Temperature chart
                st.markdown('<div class="content-card">', unsafe_allow_html=True)
                fig2 = create_streamlit_chart(combined_df, 'EXHAUST_GAS_TEMP', 'Exhaust Gas Temperature', 'Temperature (°C)', 290, 400)
                if fig2:
                    st.pyplot(fig2)
                st.markdown('</div>', unsafe_allow_html=True)
                
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Second row of charts
                st.markdown('<div class="wide-layout">', unsafe_allow_html=True)
                
                # Oil Pressure chart
                st.markdown('<div class="content-card">', unsafe_allow_html=True)
                fig3 = create_streamlit_chart(combined_df, 'OIL_PRESSURE', 'Oil Pressure', 'Pressure (kPa)', 275, 480)
                if fig3:
                    st.pyplot(fig3)
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Boost Pressure chart
                st.markdown('<div class="content-card">', unsafe_allow_html=True)
                fig4 = create_streamlit_chart(combined_df, 'BOOST_PRESSURE', 'Boost Pressure', 'Pressure (kPa)', 100, 205)
                if fig4:
                    st.pyplot(fig4)
                st.markdown('</div>', unsafe_allow_html=True)
                
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Third row of charts
                st.markdown('<div class="wide-layout">', unsafe_allow_html=True)
                
                # Oil Contamination chart
                st.markdown('<div class="content-card">', unsafe_allow_html=True)
                fig5 = create_streamlit_chart(combined_df, 'OIL_CONTAMINATION', 'Oil Contamination', 'Contamination (ppm)', 0, 25)
                if fig5:
                    st.pyplot(fig5)
                st.markdown('</div>', unsafe_allow_html=True)
                
                # Engine Boost Ratio chart
                st.markdown('<div class="content-card">', unsafe_allow_html=True)
                fig6 = create_streamlit_chart(combined_df, 'ENGINE_BOOST_RATIO', 'Engine Boost Ratio', 'Ratio', 0.85, 1.15)
                if fig6:
                    st.pyplot(fig6)
                st.markdown('</div>', unsafe_allow_html=True)
                
                st.markdown('</div>', unsafe_allow_html=True)
            else:
                st.markdown("""
                <div class="content-card">
                    <p>No data to display with current selections.</p>
                    <p>Try showing the average line, enabling alert trucks, or selecting specific trucks.</p>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.markdown("""
            <div class="content-card">
                <p>No data available for the selected time range.</p>
            </div>
            """, unsafe_allow_html=True)
        
    except Exception as charts_error:
        st.error(f"Error generating charts: {str(charts_error)}")
        st.warning("Unable to display charts due to an error. Try different selections or report this issue to IT support.")
        st.write("Error details:", charts_error)
        import traceback
        st.code(traceback.format_exc(), language="python")

    # Enhanced footer
    st.markdown("""
    <div class="footer">
        <p>
            <strong>Fleet Monitoring Dashboard</strong> | Data from TURBO_DATA_PREDICTIONS_NEW | Last updated: May 12, 2025
            <br>
            <small>Monitoring 24/7 to keep your fleet running safely and efficiently</small>
        </p>
    </div>
    """, unsafe_allow_html=True)

except Exception as e:
    # Global error handler
    st.error("Dashboard encountered an unexpected error")
    
    # Create an expander for technical details
    with st.expander("Technical Details (for IT support)"):
        st.write(f"Error Type: {type(e).__name__}")
        st.write(f"Error Message: {str(e)}")
        
        # Show traceback
        import traceback
        st.code(traceback.format_exc(), language="python")
        
        # Show environment info
        import sys
        st.write(f"Python Version: {sys.version}")
        st.write(f"Streamlit Version: {st.__version__}")
        
        # Show data info if available
        if 'df' in locals():
            st.write("Dataset Info:")
            st.write(f"- Total Rows: {len(df)}")
            st.write(f"- Columns: {list(df.columns)}")
            st.write("Sample data:")
            st.dataframe(df.head(3))
