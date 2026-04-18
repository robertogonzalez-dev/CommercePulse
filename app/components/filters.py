"""Sidebar filter widgets used by all Streamlit pages."""

import datetime
from typing import Optional

import streamlit as st

import app.db as db


def render_date_filters(
    default_start: Optional[datetime.date] = None,
    default_end: Optional[datetime.date] = None,
) -> tuple[Optional[datetime.date], Optional[datetime.date]]:
    """Render start/end date pickers. Returns (start_date, end_date)."""
    try:
        min_date, max_date = db.get_date_bounds()
    except Exception:
        min_date = datetime.date(2023, 1, 1)
        max_date = datetime.date.today()

    col1, col2 = st.sidebar.columns(2)
    start = col1.date_input(
        "From",
        value=default_start or min_date,
        min_value=min_date,
        max_value=max_date,
        key="filter_start_date",
    )
    end = col2.date_input(
        "To",
        value=default_end or max_date,
        min_value=min_date,
        max_value=max_date,
        key="filter_end_date",
    )
    return start, end  # type: ignore[return-value]


def render_channel_filter(key: str = "filter_channel") -> Optional[str]:
    channels = db.get_channels()
    choice = st.sidebar.selectbox("Channel", ["All channels"] + channels, key=key)
    return None if choice == "All channels" else choice


def render_category_filter(key: str = "filter_category") -> Optional[str]:
    cats = db.get_categories()
    choice = st.sidebar.selectbox("Category", ["All categories"] + cats, key=key)
    return None if choice == "All categories" else choice


def render_acq_channel_filter(key: str = "filter_acq_channel") -> Optional[str]:
    channels = db.get_acquisition_channels()
    choice = st.sidebar.selectbox("Acquisition channel", ["All"] + channels, key=key)
    return None if choice == "All" else choice


def render_risk_level_filter(key: str = "filter_risk") -> Optional[str]:
    levels = ["critical", "high", "medium", "low"]
    choice = st.sidebar.selectbox("Risk level", ["All levels"] + levels, key=key)
    return None if choice == "All levels" else choice
