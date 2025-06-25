import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from enum import Enum
from typing import Tuple

# ----------  Data loading ----------------------------------------------------
@st.cache_data(show_spinner=False)
def load_data(path: str) -> pd.DataFrame:
    """Read the CSV once and cache the DataFrame."""
    df = pd.read_csv(path, index_col="timestamp", parse_dates=True)
    return df

DATA_PATH = "processed_data.csv"      # <-- put the CSV in the same folder
df_full = load_data(DATA_PATH)

# ----------  Enum definitions -----------------------------------------------
class CollateralType(str, Enum):
    WETH = "weth"
    USDC = "usdc"

class PositionDirection(str, Enum):
    LONG = "long"
    SHORT = "short"

# ----------  Helper functions (same logic you provided) ----------------------
def get_simulate_df(df: pd.DataFrame, start_date: str, end_date: str):
    df_copy = df.copy()
    df_simulate = df_copy.loc[start_date:end_date]

    df_simulate.reset_index(inplace=True)
    df_simulate["seconds_between_rows"] = (
        df_simulate["timestamp"].diff().dt.total_seconds()
    )
    df_simulate.set_index("timestamp", inplace=True)
    df_simulate.dropna(inplace=True)
    return df_simulate


def compute_net_accrual_pct(
    df_full: pd.DataFrame,
    day: str,
    position_direction: PositionDirection,
    collateral_choice: CollateralType,
):
    df_simulate = get_simulate_df(df_full, day, day)

    cum_net = 0
    for _, row in df_simulate.iterrows():
        if position_direction == PositionDirection.LONG and collateral_choice == CollateralType.WETH:
            cum_net -= row["net_weth_long"] * row["seconds_between_rows"]
        elif position_direction == PositionDirection.LONG and collateral_choice == CollateralType.USDC:
            cum_net -= row["net_usdc_long"] * row["seconds_between_rows"]
        elif position_direction == PositionDirection.SHORT and collateral_choice == CollateralType.WETH:
            cum_net -= row["net_weth_short"] * row["seconds_between_rows"]
        elif position_direction == PositionDirection.SHORT and collateral_choice == CollateralType.USDC:
            cum_net -= row["net_usdc_short"] * row["seconds_between_rows"]

    return cum_net * 100


def compute_net_accrual_pct_daily(
    df_full: pd.DataFrame,
    start_date: str,
    end_date: str,
    position_direction: PositionDirection,
    collateral_choice: CollateralType,
) -> Tuple[pd.DataFrame, float]:
    date_range = pd.date_range(start=start_date, end=end_date, freq="D")
    simple_sum = 0

    cum_net = []
    for day in date_range:
        day_str = day.strftime("%Y-%m-%d")
        net_accrual = compute_net_accrual_pct(
            df_full,
            day_str,
            position_direction,
            collateral_choice,
        )
        simple_sum += net_accrual
        cum_net.append({"day": day_str, "net_accrual": simple_sum})

    return pd.DataFrame(cum_net), simple_sum

# ----------  Streamlit UI ----------------------------------------------------
st.title("Net Accrual Simulator")

col1, col2 = st.columns(2)
with col1:
    start_date = st.date_input(
        "Start date",
        value=df_full.index.min().date(),
        min_value=df_full.index.min().date(),
        max_value=df_full.index.max().date(),
    )
with col2:
    end_date = st.date_input(
        "End date",
        value=df_full.index.max().date(),
        min_value=df_full.index.min().date(),
        max_value=df_full.index.max().date(),
    )

direction = st.selectbox(
    "Position Direction",
    options=list(PositionDirection),
    format_func=lambda x: x.name.title(),
)
collateral = st.selectbox(
    "Collateral",
    options=list(CollateralType),
    format_func=lambda x: x.value.upper(),
)

# ----------  Run simulation --------------------------------------------------
if st.button("Run simulation ⚡"):
    if start_date > end_date:
        st.error("Start date must be before end date.")
    else:
        sim_df, total_accrual = compute_net_accrual_pct_daily(
            df_full,
            str(start_date),
            str(end_date),
            direction,
            collateral,
        )

        # Plot
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(pd.to_datetime(sim_df["day"]), sim_df["net_accrual"], marker="o")
        ax.axhline(0, linestyle="--", linewidth=1)
        ax.set_xlabel("Day")
        ax.set_ylabel("Net Accrual (%)")
        ax.set_title("Daily Net Accrual")
        ax.grid(alpha=0.3)
        st.pyplot(fig)

        # Optional table
        st.subheader("Raw numbers")
        st.dataframe(sim_df, hide_index=True)

        # Aggregate summary
        st.metric("Cumulative net accrual over period (%)", f"{total_accrual:,.4f}")