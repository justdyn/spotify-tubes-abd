from __future__ import annotations

from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st


DATA_PATH = Path(__file__).with_name("spotify_churn_dataset.csv")
SCHEMA_PATH = Path(__file__).with_name("db.sql")


@st.cache_data(show_spinner=False)
def load_data() -> pd.DataFrame:
    """Load and lightly preprocess the churn dataset."""
    df = pd.read_csv(DATA_PATH)
    bool_map = {0: False, 1: True}
    for col in ("offline_listening", "is_churned"):
        if df[col].dtype != bool:
            df[col] = df[col].map(bool_map).fillna(df[col]).astype(bool)
    df["churn_label"] = df["is_churned"].map({True: "Churned", False: "Active"})
    df["listening_hours"] = (df["listening_time"] / 60).round(2)
    return df


@st.cache_data(show_spinner=False)
def read_schema() -> str:
    if not SCHEMA_PATH.exists():
        return "Berkas skema tidak ditemukan."
    return SCHEMA_PATH.read_text(encoding="utf-8").strip()


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Sidebar controls returning a filtered dataframe."""
    st.sidebar.header("Filter")
    country_opt = st.sidebar.multiselect(
        "Negara",
        options=sorted(df["country"].unique()),
    )
    subscription_opt = st.sidebar.multiselect(
        "Jenis langganan",
        options=sorted(df["subscription_type"].unique()),
        default=[],
    )
    gender_opt = st.sidebar.multiselect(
        "Jenis kelamin",
        options=sorted(df["gender"].unique()),
        default=[],
    )
    age_min, age_max = int(df["age"].min()), int(df["age"].max())
    age_range = st.sidebar.slider(
        "Rentang usia",
        min_value=age_min,
        max_value=age_max,
        value=(age_min, age_max),
    )
    churn_choice = st.sidebar.radio(
        "Status churn",
        options=["Semua", "Aktif", "Churn"],
        index=0,
    )

    filtered = df.copy()
    if country_opt:
        filtered = filtered[filtered["country"].isin(country_opt)]
    if subscription_opt:
        filtered = filtered[filtered["subscription_type"].isin(subscription_opt)]
    if gender_opt:
        filtered = filtered[filtered["gender"].isin(gender_opt)]
    filtered = filtered[
        (filtered["age"] >= age_range[0])
        & (filtered["age"] <= age_range[1])
    ]
    if churn_choice != "Semua":
        status_map = {"Aktif": "Active", "Churn": "Churned"}
        filtered = filtered[filtered["churn_label"] == status_map[churn_choice]]
    return filtered


def show_kpis(df: pd.DataFrame) -> None:
    churn_rate = df["is_churned"].mean()
    avg_listening = df["listening_time"].mean()
    avg_songs = df["songs_played_per_day"].mean()
    ads_per_week = df["ads_listened_per_week"].mean()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Pengguna", f"{len(df):,}")
    col2.metric("Tingkat churn", f"{churn_rate:.1%}")
    col3.metric("Rata-rata durasi dengar (menit)", f"{avg_listening:,.0f}")
    col4.metric("Rata-rata iklan/minggu", f"{ads_per_week:,.1f}")

    st.caption(
        f"Pengguna memutar rata-rata {avg_songs:,.1f} lagu per hari untuk segmen ini."
    )


def stacked_churn_chart(df: pd.DataFrame) -> None:
    grouped = (
        df.groupby(["subscription_type", "churn_label"])
        .size()
        .reset_index(name="users")
    )
    if grouped.empty:
        st.info("Sesuaikan filter agar visualisasi terisi.")
        return
    fig = px.bar(
        grouped,
        x="subscription_type",
        y="users",
        color="churn_label",
        barmode="stack",
        text_auto=True,
        title="Komposisi churn per jenis langganan",
        labels={"subscription_type": "Jenis langganan", "users": "Jumlah pengguna"},
    )
    st.plotly_chart(fig, use_container_width=True)


def listening_time_boxplot(df: pd.DataFrame) -> None:
    if df.empty:
        st.info("Tidak ada data untuk box plot.")
        return
    fig = px.box(
        df,
        x="country",
        y="listening_time",
        color="churn_label",
        points="suspectedoutliers",
        title="Distribusi durasi dengar per negara",
        labels={"listening_time": "Durasi dengar (menit)", "country": "Negara"},
    )
    st.plotly_chart(fig, use_container_width=True)


def skip_vs_songs_scatter(df: pd.DataFrame) -> None:
    if df.empty:
        return
    fig = px.scatter(
        df,
        x="songs_played_per_day",
        y="skip_rate",
        size="listening_hours",
        color="churn_label",
        hover_data=[
            "user_id",
            "subscription_type",
            "device_type",
            "listening_time",
        ],
        title="Skip rate vs keterlibatan",
        labels={
            "songs_played_per_day": "Lagu per hari",
            "skip_rate": "Skip rate",
        },
    )
    st.plotly_chart(fig, use_container_width=True)


def offline_ads_chart(df: pd.DataFrame) -> None:
    summary = (
        df.groupby(["offline_listening", "churn_label"])["ads_listened_per_week"]
        .mean()
        .reset_index()
    )
    if summary.empty:
        return
    summary["offline_listening"] = summary["offline_listening"].map(
        {True: "Offline aktif", False: "Hanya streaming"}
    )
    fig = px.bar(
        summary,
        x="offline_listening",
        y="ads_listened_per_week",
        color="churn_label",
        barmode="group",
        text_auto=".1f",
        title="Iklan per minggu vs kapabilitas offline",
        labels={"ads_listened_per_week": "Iklan per minggu", "offline_listening": ""},
    )
    st.plotly_chart(fig, use_container_width=True)


def layout(filtered: pd.DataFrame) -> None:
    st.subheader("Ringkasan engagement")
    show_kpis(filtered)

    stacked_churn_chart(filtered)

    col1, col2 = st.columns(2)
    with col1:
        listening_time_boxplot(filtered)
    with col2:
        skip_vs_songs_scatter(filtered)

    offline_ads_chart(filtered)

    st.subheader("Data detail")
    st.dataframe(
        filtered[
            [
                "user_id",
                "gender",
                "age",
                "country",
                "subscription_type",
                "device_type",
                "songs_played_per_day",
                "skip_rate",
                "offline_listening",
                "ads_listened_per_week",
                "churn_label",
            ]
        ],
        use_container_width=True,
    )
    st.download_button(
        label="Unduh data terfilter (CSV)",
        data=filtered.to_csv(index=False).encode("utf-8"),
        file_name="spotify_churn_filtered.csv",
        mime="text/csv",
    )


def main() -> None:
    st.set_page_config(
        page_title="Ruang Kontrol Churn Spotify",
        page_icon="🎧",
        layout="wide",
    )
    st.title("Dasbor Churn Pendengar Spotify")
    st.write(
        "Jelajahi dataset churn Spotify dari `db.sql` untuk mendukung keputusan "
        "administrasi basis data secara data-driven."
    )
    st.info(
        "Churn menggambarkan pengguna yang berhenti memakai Spotify. "
        "Dengan kata lain, ini adalah jumlah orang yang tidak lagi aktif bekerja "
        "sama dengan layanan sehingga kita perlu memahami alasannya."
    )

    df = load_data()
    filtered = apply_filters(df)

    if filtered.empty:
        st.warning("Tidak ada data yang cocok dengan filter.")
    else:
        layout(filtered)

    with st.expander("Lihat skema tabel `users` dari db.sql"):
        st.code(read_schema())


if __name__ == "__main__":
    main()

