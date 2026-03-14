"""
stock_pipeline.py
=================
Unified, production-ready Stock & Pipeline Analysis tool.

Supports TWO run modes:
  1. LOCAL  – reads files from local/Dropbox paths, schedules daily at 10:30 AM
  2. GITHUB – downloads files from Dropbox via API, processes, uploads output,
              sends email notification. Triggered by GitHub Actions.

Mode is selected by the environment variable RUN_MODE:
  RUN_MODE=github  → GitHub / Cloud mode  (uses Dropbox API + env-var credentials)
  RUN_MODE=local   → Local mode           (uses local file paths + built-in creds)
  (default)        → Local mode

Usage:
  python stock_pipeline.py            # start local scheduler (runs at 10:30 AM daily)
  python stock_pipeline.py run        # run once immediately  (local mode)
  RUN_MODE=github python stock_pipeline.py   # GitHub Actions mode
"""

# ──────────────────────────────────────────────────────────────────────────────
# Imports
# ──────────────────────────────────────────────────────────────────────────────
import logging
import os
import smtplib
import sys
import time
import traceback
import warnings
from datetime import datetime
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import dropbox
import pandas as pd
import requests
import schedule
from dotenv import load_dotenv
from openpyxl import Workbook
from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
from openpyxl.utils import get_column_letter
from openpyxl.utils.dataframe import dataframe_to_rows  # noqa: F401  (kept for reference)

# ──────────────────────────────────────────────────────────────────────────────
# Bootstrap
# ──────────────────────────────────────────────────────────────────────────────
load_dotenv()
warnings.filterwarnings("ignore")

RUN_MODE = os.getenv("RUN_MODE", "local").lower()   # "local" | "github"

# ──────────────────────────────────────────────────────────────────────────────
# Logging  (file handler only in local mode so GitHub logs stay clean)
# ──────────────────────────────────────────────────────────────────────────────
_handlers = [logging.StreamHandler(sys.stdout)]
if RUN_MODE == "local":
    _handlers.append(logging.FileHandler("stock_pipeline.log", encoding="utf-8"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=_handlers,
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# Configuration
# ──────────────────────────────────────────────────────────────────────────────
class LocalConfig:
    """Credentials and paths used when running on a local Windows machine."""

    # ── Gmail ───────────────────────────────────────────────────────
    GMAIL_SENDER: str       = "businesssupport@technosport.in"
    GMAIL_APP_PASSWORD: str = "ctck cvix qafj dpoi"
    GMAIL_RECIPIENTS: list  = [
        "narasimman.s@technosport.in",
        # add more recipients here if needed
    ]

    # ── File paths ──────────────────────────────────────────────────
    STOCK_FILE: str    = r"C:\Users\narasimman.s\Downloads\stock_pipeline.xlsx"
    PIPELINE_FILE: str = r"D:\Dropbox\Dropbox\ODOO B2B REPORT\DATA FILE\Simma\pipeline1.xlsx"
    OUTPUT_FILE: str   = r"C:\Users\narasimman.s\Downloads\Stock_Pipeline_Analysis_Report.xlsx"

    # ── Schedule ─────────────────────────────────────────────────────
    SCHEDULE_TIME: str = "10:30"   # 24-hour HH:MM, IST


class GitHubConfig:
    """
    Credentials and paths for GitHub Actions / Cloud mode.
    Every value is read from environment variables / GitHub Secrets.
    """

    # ── Dropbox OAuth2 ───────────────────────────────────────────────
    APP_KEY:       str = os.getenv("DROPBOX_APP_KEY",       "21xua5rf8nm6ifj")
    APP_SECRET:    str = os.getenv("DROPBOX_APP_SECRET",    "")
    REFRESH_TOKEN: str = os.getenv("DROPBOX_REFRESH_TOKEN", "")

    # ── Dropbox remote paths ─────────────────────────────────────────
    DROPBOX_INPUT_STOCK:    str = os.getenv(
        "DROPBOX_INPUT_STOCK",
        "/ODOO B2B REPORT/DATA FILE/Simma/stock_pipeline.xlsx",
    )
    DROPBOX_INPUT_PIPELINE: str = os.getenv(
        "DROPBOX_INPUT_PIPELINE",
        "/ODOO B2B REPORT/DATA FILE/Simma/pipeline1.xlsx",
    )
    DROPBOX_OUTPUT_PATH:    str = os.getenv(
        "DROPBOX_OUTPUT_PATH",
        "/ODOO B2B REPORT/DATA FILE/Simma/Stock_Pipeline_Analysis_Report.xlsx",
    )

    # ── Local temp paths inside the Actions runner ──────────────────
    LOCAL_STOCK:    str = "/tmp/stock_pipeline.xlsx"
    LOCAL_PIPELINE: str = "/tmp/pipeline1.xlsx"
    LOCAL_OUTPUT:   str = "/tmp/Stock_Pipeline_Analysis_Report.xlsx"

    # ── Gmail ────────────────────────────────────────────────────────
    GMAIL_SENDER:       str = os.getenv("GMAIL_SENDER",       "")
    GMAIL_APP_PASSWORD: str = os.getenv("GMAIL_APP_PASSWORD", "")
    GMAIL_RECIPIENT:    str = os.getenv("GMAIL_RECIPIENT",    "")


# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────
BLANK_STYLES_TO_REMOVE = [
    "OR23", "OR55", "OR69", "OR56", "OR66", "OR24", "OR97", "OR03",
    "OR01A", "OR05", "OR07", "OR07B", "OR09", "OR09A", "OR12", "OR1A",
    "OR35", "OR51", "OR57", "OR96", "SWL01", "OR81", "CR86",
]

SIZE_COLUMNS = [
    "06Y/S", "08Y/M", "10Y/L", "12Y/XL", "14Y/2XL",
    "06UK", "07UK", "08UK", "09UK", "10UK", "11UK",
    "3XL", "4XL", "5XL", "STOCK",
]

COLUMN_ORDER = [
    "SERIES", "STYLE", "CATEGORY",
    "06Y/S", "08Y/M", "10Y/L", "12Y/XL", "14Y/2XL",
    "06UK", "07UK", "08UK", "09UK", "10UK", "11UK",
    "3XL", "4XL", "5XL", "STOCK", "MONTH",
]


# ──────────────────────────────────────────────────────────────────────────────
# ① Dropbox helpers  (GitHub mode only)
# ──────────────────────────────────────────────────────────────────────────────
def get_dropbox_access_token() -> str:
    """Exchange the stored refresh token for a short-lived access token."""
    logger.info("🔑 Requesting Dropbox access token …")
    resp = requests.post(
        "https://api.dropbox.com/oauth2/token",
        data={
            "grant_type":    "refresh_token",
            "refresh_token": GitHubConfig.REFRESH_TOKEN,
            "client_id":     GitHubConfig.APP_KEY,
            "client_secret": GitHubConfig.APP_SECRET,
        },
        timeout=30,
    )
    resp.raise_for_status()
    token = resp.json()["access_token"]
    logger.info("✅ Dropbox access token obtained.")
    return token


def download_from_dropbox(dbx: dropbox.Dropbox, dropbox_path: str, local_path: str) -> bool:
    """Download a single file from Dropbox. Returns True on success."""
    try:
        logger.info("⬇️  Downloading '%s' …", dropbox_path)
        _, res = dbx.files_download(dropbox_path)
        with open(local_path, "wb") as fh:
            fh.write(res.content)
        logger.info("   Saved → '%s'  (%.1f KB)", local_path, len(res.content) / 1024)
        return True
    except dropbox.exceptions.ApiError as exc:
        logger.warning("⚠️  Could not download '%s': %s", dropbox_path, exc)
        return False


def upload_to_dropbox(dbx: dropbox.Dropbox, local_path: str, dropbox_path: str) -> None:
    """Upload local_path to Dropbox, overwriting any existing file."""
    logger.info("⬆️  Uploading '%s' → '%s' …", local_path, dropbox_path)
    with open(local_path, "rb") as fh:
        data = fh.read()
    dbx.files_upload(data, dropbox_path, mode=dropbox.files.WriteMode("overwrite"))
    logger.info("   Upload complete (%.1f KB).", len(data) / 1024)


# ──────────────────────────────────────────────────────────────────────────────
# ② Data processing
# ──────────────────────────────────────────────────────────────────────────────
def filter_blank_styles(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows whose STYLE value is in the known-blank list."""
    if df.empty or "STYLE" not in df.columns:
        return df
    before = len(df)
    bad = {s.strip() for s in BLANK_STYLES_TO_REMOVE}
    df["STYLE"] = df["STYLE"].astype(str).str.strip()
    df = df[~df["STYLE"].isin(bad)].copy()
    removed = before - len(df)
    if removed:
        logger.info("🗑️  Removed %d blank-style rows.", removed)
    else:
        logger.info("ℹ️  No blank styles found to remove.")
    return df


def process_stock_data(stock_file_path: str) -> pd.DataFrame:
    """Load, normalise, filter, and aggregate the stock Excel file."""
    logger.info("📊 Processing stock data: %s", stock_file_path)

    if not os.path.exists(stock_file_path):
        logger.error("❌ Stock file not found: %s", stock_file_path)
        return pd.DataFrame()

    df = pd.read_excel(stock_file_path, engine="openpyxl")
    if df.empty:
        logger.error("❌ Stock file is empty.")
        return pd.DataFrame()

    logger.info("   Raw columns: %s", list(df.columns))

    # ── Remove PACK/LFR location rows ───────────────────────────────
    if "Location" in df.columns:
        before = len(df)
        df = df[~df["Location"].astype(str).str.contains("PACK/LFR", case=False, na=False)]
        logger.info("🗑️  Removed %d PACK/LFR rows. Remaining: %d", before - len(df), len(df))
    else:
        logger.warning("⚠️  'Location' column not found – skipping PACK/LFR filter.")

    if df.empty:
        logger.error("❌ No data after PACK/LFR filter.")
        return pd.DataFrame()

    # ── Column rename map (lowercase key → target name) ─────────────
    _rmap = {
        "s":           "06Y/S",
        "m":           "08Y/M",
        "l":           "10Y/L",
        "xl":          "12Y/XL",
        "2xl":         "14Y/2XL",
        "3xl":         "3XL",
        "4xl":         "4XL",
        "5xl":         "5XL",
        "6uk":         "06UK",
        "7uk":         "07UK",
        "8uk":         "08UK",
        "9uk":         "09UK",
        "10uk":        "10UK",
        "11uk":        "11UK",
        "grand total": "STOCK",
        "cat sales":   "CATEGORY",
    }
    rename_dict = {}
    for col in df.columns:
        key = str(col).strip().lower()
        if key in _rmap:
            rename_dict[col] = _rmap[key]
            logger.info("   Rename  '%s' → '%s'", col, _rmap[key])
    df = df.rename(columns=rename_dict)

    if "STYLE" not in df.columns:
        logger.error("❌ 'STYLE' column not found after renaming.")
        logger.info("   Available columns: %s", list(df.columns))
        return pd.DataFrame()

    if "CATEGORY" not in df.columns:
        logger.warning("⚠️  'CAT SALES' / 'CATEGORY' column not found – no category grouping.")

    # ── Combine paired youth size columns (e.g. 06Y + 06Y/S) ────────
    for old, new in [("06Y","06Y/S"),("08Y","08Y/M"),("10Y","10Y/L"),("12Y","12Y/XL"),("14Y","14Y/2XL")]:
        if old in df.columns and new in df.columns:
            df[new] = df[old].fillna(0) + df[new].fillna(0)
            df.drop(columns=[old], inplace=True)
            logger.info("   Combined %s + %s → %s", old, new, new)
        elif old in df.columns:
            df.rename(columns={old: new}, inplace=True)
            logger.info("   Renamed %s → %s", old, new)

    # ── Coerce numeric size/stock columns ───────────────────────────
    for col in SIZE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ── Drop empty STYLE rows ────────────────────────────────────────
    df = df.dropna(subset=["STYLE"])
    df = df[df["STYLE"].astype(str).str.strip() != ""]
    if df.empty:
        logger.error("❌ No valid STYLE data found after cleaning.")
        return pd.DataFrame()

    # ── Aggregate by STYLE (+ CATEGORY if present) ──────────────────
    group_cols = ["STYLE"] + (["CATEGORY"] if "CATEGORY" in df.columns else [])
    agg_cols   = [c for c in SIZE_COLUMNS if c in df.columns]
    df = df.groupby(group_cols)[agg_cols].sum().reset_index()
    df = df.sort_values("STYLE", ascending=True).reset_index(drop=True)

    logger.info("✅ Stock data processed. Shape: %s", df.shape)
    return df


def process_pipeline_data(pipeline_file_path: str) -> pd.DataFrame:
    """Load and summarise the pipeline Excel file."""
    logger.info("📈 Processing pipeline data: %s", pipeline_file_path)
    _empty = pd.DataFrame(columns=["STYLE", "MONTH"])

    if not pipeline_file_path or not os.path.exists(pipeline_file_path):
        logger.warning("⚠️  Pipeline file not found: %s", pipeline_file_path)
        return _empty

    df = pd.read_excel(pipeline_file_path, engine="openpyxl")
    if df.empty:
        logger.warning("⚠️  Pipeline file is empty.")
        return _empty

    logger.info("   Pipeline columns: %s", list(df.columns))

    # ── Normalise STYLE column name ──────────────────────────────────
    if "STYLE NO" in df.columns and "STYLE" not in df.columns:
        df = df.rename(columns={"STYLE NO": "STYLE"})
        logger.info("   Renamed 'STYLE NO' → 'STYLE'")
    elif "STYLE NO" in df.columns:
        df.drop(columns=["STYLE NO"], inplace=True)

    if "STYLE" not in df.columns:
        logger.error("❌ 'STYLE' column not found in pipeline data.")
        return _empty

    # ── Filter to CATALOGUE SHEET rows only ─────────────────────────
    if "From" in df.columns:
        df = df[df["From"].str.contains("CATALOGUE SHEET", case=False, na=False)]
        logger.info("   Rows after CATALOGUE SHEET filter: %d", len(df))
    else:
        logger.warning("⚠️  'From' column not found – skipping CATALOGUE SHEET filter.")

    if df.empty:
        logger.warning("⚠️  No CATALOGUE SHEET rows found.")
        return _empty

    # ── Locate month column ──────────────────────────────────────────
    month_col = next((c for c in df.columns if "month" in str(c).lower()), None)
    if not month_col:
        logger.warning("⚠️  No month column found.")
        return _empty

    # ── Locate O QTY column ─────────────────────────────────────────
    oqty_col = next(
        (c for c in df.columns if any(x in str(c).lower().strip() for x in ["o qty", "oqty", "o_qty"])),
        None,
    )

    # ── Build working frame ──────────────────────────────────────────
    keep   = ["STYLE", month_col] + ([oqty_col] if oqty_col else [])
    df     = df[keep].copy()
    rn     = {month_col: "MONTH"}
    if oqty_col:
        rn[oqty_col] = "O_QTY"
    df = df.rename(columns=rn)

    if "O_QTY" in df.columns:
        df["O_QTY"] = pd.to_numeric(df["O_QTY"], errors="coerce").fillna(0)

    # ── Clean ────────────────────────────────────────────────────────
    df = df.dropna(subset=["STYLE", "MONTH"])
    df["STYLE"] = df["STYLE"].astype(str).str.strip()
    df["MONTH"] = df["MONTH"].astype(str).str.strip()
    df = df[
        (df["STYLE"] != "") & (df["MONTH"] != "") &
        (df["STYLE"].str.lower() != "nan") & (df["MONTH"].str.lower() != "nan")
    ]
    if df.empty:
        logger.warning("⚠️  No valid pipeline rows after cleaning.")
        return _empty

    # ── Aggregate: combine months (with qty) per STYLE ───────────────
    if "O_QTY" in df.columns:
        grp = df.groupby(["STYLE", "MONTH"], as_index=False)["O_QTY"].sum()
        grp["MONTH_WITH_QTY"] = grp.apply(
            lambda r: f"{r['MONTH']}({int(r['O_QTY'])})" if r["O_QTY"] > 0 else r["MONTH"],
            axis=1,
        )
        final = (
            grp.groupby("STYLE")["MONTH_WITH_QTY"]
            .apply(lambda x: ", ".join(sorted(set(x))))
            .reset_index()
        )
        final.columns = ["STYLE", "MONTH"]
    else:
        final = (
            df.groupby("STYLE")["MONTH"]
            .apply(lambda x: ", ".join(sorted(set(x.astype(str).str.strip()))))
            .reset_index()
        )

    logger.info("✅ Pipeline data processed. Shape: %s", final.shape)
    return final.reset_index(drop=True)


def merge_and_finalize_data(stock_df: pd.DataFrame, pipeline_df: pd.DataFrame) -> pd.DataFrame:
    """Merge stock + pipeline, add SERIES column, reorder."""
    logger.info("🔄 Merging data …")

    if stock_df.empty:
        logger.error("❌ No stock data to process.")
        return pd.DataFrame()

    stock_df = filter_blank_styles(stock_df)
    if stock_df.empty:
        logger.error("❌ No stock data after blank-style filter.")
        return pd.DataFrame()

    if not pipeline_df.empty:
        pipeline_df = filter_blank_styles(pipeline_df)
        stock_df["STYLE"]    = stock_df["STYLE"].astype(str).str.strip()
        pipeline_df["STYLE"] = pipeline_df["STYLE"].astype(str).str.strip()
        stock_df = stock_df.merge(pipeline_df, on="STYLE", how="left")
        logger.info("   Merge complete. Rows: %d", len(stock_df))
    else:
        logger.warning("⚠️  No pipeline data – MONTH column will be empty.")
        stock_df["MONTH"] = ""

    stock_df["MONTH"] = stock_df["MONTH"].fillna("")

    # ── Derive SERIES column ─────────────────────────────────────────
    def _series(style: str) -> str:
        s = str(style).strip()
        if not s or s.lower() == "nan":
            return "UNKNOWN-SERIES"
        return "OR-SERIES" if s.upper().startswith("O") else f"{s[0].upper()}-SERIES"

    stock_df["SERIES"] = stock_df["STYLE"].apply(_series)

    # ── Reorder columns ──────────────────────────────────────────────
    present = [c for c in COLUMN_ORDER if c in stock_df.columns]
    final   = stock_df[present].sort_values(["SERIES", "STYLE"]).reset_index(drop=True)

    logger.info("✅ Final merged data shape: %s", final.shape)
    return final


# ──────────────────────────────────────────────────────────────────────────────
# ③ Excel report builder
# ──────────────────────────────────────────────────────────────────────────────
def _border(color: str = "D1D1D1") -> Border:
    s = Side(style="thin", color=color)
    return Border(left=s, right=s, top=s, bottom=s)


def _hdr(cell, bg: str, fg: str = "FFFFFF") -> None:
    cell.font      = Font(bold=True, color=fg, size=11)
    cell.fill      = PatternFill(start_color=bg, end_color=bg, fill_type="solid")
    cell.alignment = Alignment(horizontal="center", vertical="center")
    cell.border    = _border()


def _title_row(ws, text: str, color: str, row: int = 1) -> None:
    ws.cell(row=row, column=1, value=text)
    c = ws.cell(row=row, column=1)
    c.font      = Font(size=16, bold=True, color="FFFFFF")
    c.fill      = PatternFill(start_color=color, end_color=color, fill_type="solid")
    c.alignment = Alignment(horizontal="center", vertical="center")
    ws.merge_cells(start_row=row, start_column=1, end_row=row, end_column=min(max(ws.max_column, 1), 22))
    ws.row_dimensions[row].height = 32


def _write_df(ws, df: pd.DataFrame, start_row: int = 3,
              hdr_color: str = "4472C4", highlight_last: bool = False) -> None:
    """Write DataFrame into ws with alternating row colours."""
    ALT1, ALT2, TOTAL = "F2F2F2", "FFFFFF", "FFD966"

    for c_idx, col in enumerate(df.columns, 1):
        _hdr(ws.cell(row=start_row, column=c_idx, value=col), hdr_color)

    for r_off, (_, row) in enumerate(df.iterrows(), 1):
        r_idx    = start_row + r_off
        is_total = highlight_last and r_off == len(df)
        bg       = TOTAL if is_total else (ALT1 if r_off % 2 == 0 else ALT2)
        for c_idx, val in enumerate(row, 1):
            v    = "" if (isinstance(val, float) and __import__("math").isnan(val)) else val
            cell = ws.cell(row=r_idx, column=c_idx, value=v)
            cell.fill   = PatternFill(start_color=bg, end_color=bg, fill_type="solid")
            cell.border = _border()
            cell.font   = Font(size=10, bold=is_total)
            cell.alignment = Alignment(
                horizontal=("right" if isinstance(v, (int, float)) else "left"),
                vertical="center",
            )


def _autofit(ws, df: pd.DataFrame) -> None:
    for c_idx, col in enumerate(df.columns, 1):
        mx = max(
            len(str(col)),
            *(len(str(v)) for v in df.iloc[:100, c_idx - 1] if str(v) != "nan"),
            default=10,
        )
        ws.column_dimensions[get_column_letter(c_idx)].width = min(max(mx + 2, 10), 42)


def create_excel_report(df: pd.DataFrame, output_file: str) -> bool:
    """Build a multi-sheet styled Excel workbook."""
    logger.info("📄 Creating Excel report …")
    if df.empty:
        logger.error("❌ No data – cannot create report.")
        return False

    out_dir = os.path.dirname(output_file)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    wb = Workbook()
    wb.remove(wb.active)

    # ── Sheet 1 : Full report ────────────────────────────────────────
    ws1 = wb.create_sheet("Stock Pipeline Report")
    _title_row(ws1, "STOCK AND PIPELINE ANALYSIS REPORT", "366092")
    _write_df(ws1, df, start_row=3, hdr_color="366092")
    _autofit(ws1, df)

    # ── Sheet 2 : Category summary ───────────────────────────────────
    if "CATEGORY" in df.columns:
        agg  = [c for c in SIZE_COLUMNS if c in df.columns]
        cdf  = df.groupby("CATEGORY")[agg].sum().reset_index()
        tot  = cdf.sum(numeric_only=True)
        tot["CATEGORY"] = "TOTAL"
        cdf  = pd.concat([cdf, pd.DataFrame([tot])], ignore_index=True)
        ws2  = wb.create_sheet("Category Summary")
        _title_row(ws2, "CATEGORY SALES SUMMARY", "70AD47")
        _write_df(ws2, cdf, start_row=3, hdr_color="70AD47", highlight_last=True)
        _autofit(ws2, cdf)
    else:
        logger.warning("⚠️  No CATEGORY column – skipping Category Summary sheet.")

    # ── Sheet 3 : Series summary ─────────────────────────────────────
    if "SERIES" in df.columns:
        agg  = [c for c in SIZE_COLUMNS if c in df.columns]
        sdf  = df.groupby("SERIES")[agg].sum().reset_index()
        tot  = sdf.sum(numeric_only=True)
        tot["SERIES"] = "TOTAL"
        sdf  = pd.concat([sdf, pd.DataFrame([tot])], ignore_index=True)
        ws3  = wb.create_sheet("Series Summary")
        _title_row(ws3, "SERIES SUMMARY", "7030A0")
        _write_df(ws3, sdf, start_row=3, hdr_color="7030A0", highlight_last=True)
        _autofit(ws3, sdf)

    # ── Sheet 4 : Executive summary ──────────────────────────────────
    ws4 = wb.create_sheet("Executive Summary")
    _title_row(ws4, "EXECUTIVE SUMMARY", "4472C4")

    total_stock = int(df["STOCK"].sum())   if "STOCK"  in df.columns else 0
    avg_stock   = round(df["STOCK"].mean(), 1) if "STOCK"  in df.columns and len(df) else 0
    max_stock   = int(df["STOCK"].max())   if "STOCK"  in df.columns else 0
    min_stock   = int(df["STOCK"].min())   if "STOCK"  in df.columns else 0
    n_series    = df["SERIES"].nunique()   if "SERIES" in df.columns else "N/A"
    n_pipeline  = int((df["MONTH"] != "").sum()) if "MONTH" in df.columns else 0

    kpis = [
        ["Metric",                        "Value"],
        ["Total Styles",                  len(df)],
        ["Total Stock Units",             total_stock],
        ["Number of Series",              n_series],
        ["Average Stock per Style",       avg_stock],
        ["Maximum Stock (Single Style)",  max_stock],
        ["Minimum Stock (Single Style)",  min_stock],
        ["Styles with Pipeline Month",    n_pipeline],
        ["Report Generated (IST)",        datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
    ]

    for r, row_data in enumerate(kpis, 3):
        for c, val in enumerate(row_data, 1):
            cell = ws4.cell(row=r, column=c, value=val)
            if r == 3:
                _hdr(cell, "4472C4")
            else:
                cell.fill      = PatternFill(start_color="F8F9FA", end_color="F8F9FA", fill_type="solid")
                cell.alignment = Alignment(horizontal="center", vertical="center")
                cell.border    = _border()

    ws4.column_dimensions["A"].width = 38
    ws4.column_dimensions["B"].width = 30

    # ── Timestamp footer ─────────────────────────────────────────────
    ts = datetime.now().strftime("Generated on %Y-%m-%d at %H:%M:%S IST")
    for ws in wb:
        try:
            ws.cell(row=ws.max_row + 2, column=1, value=ts).font = Font(italic=True, color="808080")
        except Exception:
            pass

    wb.save(output_file)
    logger.info("✅ Excel report saved: %s", output_file)
    return True


# ──────────────────────────────────────────────────────────────────────────────
# ④ Email notification
# ──────────────────────────────────────────────────────────────────────────────
def _email_content(status: str, summary: dict | None, error: str = "") -> tuple:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if status == "success" and summary:
        subject = "✅ Stock Pipeline Completed Successfully"
        body = f"""Hello Team,

Please find attached the Stock and Pipeline Analysis Report.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Run Time     : {ts} (IST)
  Total Styles : {summary.get('total_styles', 'N/A'):,}
  Total Stock  : {summary.get('total_stock', 'N/A'):,}
  Series Count : {summary.get('series_count', 'N/A')}
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

The output file has also been uploaded back to Dropbox (GitHub mode).

This is an automated report. Contact IT if you have any issues.

Best regards,
Automated Reporting System"""
    else:
        subject = "❌ Stock Pipeline FAILED"
        body = f"""Hello Team,

The Stock Pipeline encountered an error and did not complete successfully.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Run Time : {ts} (IST)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Error Details:
{error or 'Unknown error – check the logs.'}

Please check the GitHub Actions run logs for the full traceback.

Best regards,
Automated Reporting System"""
    return subject, body


def send_email(
    subject: str,
    body: str,
    sender: str,
    password: str,
    recipients: list,
    attachment_path: str = "",
) -> bool:
    """Send an email via Gmail SMTP with an optional Excel attachment."""
    recipients = [r.strip() for r in recipients if r.strip()]
    if not recipients:
        logger.error("❌ No valid email recipients.")
        return False

    logger.info("✉️  Sending email to: %s", recipients)
    msg             = MIMEMultipart()
    msg["From"]     = sender
    msg["To"]       = ", ".join(recipients)
    msg["Subject"]  = subject
    msg.attach(MIMEText(body, "plain"))

    if attachment_path and os.path.exists(attachment_path):
        with open(attachment_path, "rb") as fh:
            part = MIMEApplication(fh.read(), Name=os.path.basename(attachment_path))
        part["Content-Disposition"] = f'attachment; filename="{os.path.basename(attachment_path)}"'
        msg.attach(part)
    elif attachment_path:
        logger.warning("⚠️  Attachment not found: %s", attachment_path)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as srv:
            srv.starttls()
            srv.login(sender, password)
            srv.sendmail(sender, recipients, msg.as_string())
        logger.info("✅ Email sent successfully.")
        return True
    except Exception as exc:
        logger.error("❌ Email failed: %s", exc)
        return False


# ──────────────────────────────────────────────────────────────────────────────
# ⑤ Run orchestrators
# ──────────────────────────────────────────────────────────────────────────────
def run_pipeline_github() -> bool:
    """
    GitHub Actions mode:
      Authenticate Dropbox → Download files → Process → Upload output → Email
    """
    logger.info("═" * 65)
    logger.info("🚀 GITHUB MODE – starting at %s IST", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("═" * 65)

    summary = None
    try:
        # 1. Authenticate
        token = get_dropbox_access_token()
        dbx   = dropbox.Dropbox(oauth2_access_token=token)
        dbx.users_get_current_account()
        logger.info("✅ Dropbox connection verified.")

        # 2. Download stock file (mandatory)
        ok = download_from_dropbox(dbx, GitHubConfig.DROPBOX_INPUT_STOCK, GitHubConfig.LOCAL_STOCK)
        if not ok:
            raise FileNotFoundError(f"Stock file unavailable on Dropbox: {GitHubConfig.DROPBOX_INPUT_STOCK}")

        # 3. Download pipeline file (optional)
        pipeline_ok = download_from_dropbox(dbx, GitHubConfig.DROPBOX_INPUT_PIPELINE, GitHubConfig.LOCAL_PIPELINE)
        if not pipeline_ok:
            logger.warning("⚠️  Pipeline file unavailable – continuing without it.")

        # 4. Process
        stock_df    = process_stock_data(GitHubConfig.LOCAL_STOCK)
        if stock_df.empty:
            raise ValueError("Stock data processing returned empty DataFrame.")

        pipeline_df = (
            process_pipeline_data(GitHubConfig.LOCAL_PIPELINE) if pipeline_ok
            else pd.DataFrame(columns=["STYLE", "MONTH"])
        )
        final_df    = merge_and_finalize_data(stock_df, pipeline_df)
        if final_df.empty:
            raise ValueError("Merged final data is empty.")

        # 5. Create report
        if not create_excel_report(final_df, GitHubConfig.LOCAL_OUTPUT):
            raise RuntimeError("Excel report creation failed.")

        summary = {
            "total_styles": len(final_df),
            "total_stock":  int(final_df["STOCK"].sum()) if "STOCK" in final_df.columns else 0,
            "series_count": final_df["SERIES"].nunique() if "SERIES" in final_df.columns else 0,
        }

        # 6. Upload output
        upload_to_dropbox(dbx, GitHubConfig.LOCAL_OUTPUT, GitHubConfig.DROPBOX_OUTPUT_PATH)

        # 7. Email success
        subject, body = _email_content("success", summary)
        send_email(subject, body,
                   GitHubConfig.GMAIL_SENDER, GitHubConfig.GMAIL_APP_PASSWORD,
                   [GitHubConfig.GMAIL_RECIPIENT],
                   attachment_path=GitHubConfig.LOCAL_OUTPUT)

        logger.info("═" * 65)
        logger.info("✅ GITHUB Pipeline completed successfully!")
        logger.info("═" * 65)
        return True

    except Exception:
        err = traceback.format_exc()
        logger.error("❌ Pipeline failed:\n%s", err)
        try:
            subject, body = _email_content("failure", None, error=err)
            send_email(subject, body,
                       GitHubConfig.GMAIL_SENDER, GitHubConfig.GMAIL_APP_PASSWORD,
                       [GitHubConfig.GMAIL_RECIPIENT])
        except Exception as mail_err:
            logger.error("❌ Could not send failure email: %s", mail_err)
        return False


def run_pipeline_local() -> bool:
    """
    Local mode:
      Read local files → Process → Save report → Email with attachment
    """
    logger.info("═" * 65)
    logger.info("🚀 LOCAL MODE – starting at %s IST", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    logger.info("═" * 65)

    try:
        if not os.path.exists(LocalConfig.STOCK_FILE):
            logger.error("❌ Stock file not found: %s", LocalConfig.STOCK_FILE)
            return False

        if not os.path.exists(LocalConfig.PIPELINE_FILE):
            logger.warning("⚠️  Pipeline file not found: %s – continuing without it.", LocalConfig.PIPELINE_FILE)

        stock_df    = process_stock_data(LocalConfig.STOCK_FILE)
        if stock_df.empty:
            logger.error("❌ Aborting: stock data processing failed.")
            return False

        pipeline_df = process_pipeline_data(LocalConfig.PIPELINE_FILE)
        final_df    = merge_and_finalize_data(stock_df, pipeline_df)
        if final_df.empty:
            logger.error("❌ Aborting: merged data is empty.")
            return False

        if not create_excel_report(final_df, LocalConfig.OUTPUT_FILE):
            logger.error("❌ Report creation failed.")
            return False

        summary = {
            "total_styles": len(final_df),
            "total_stock":  int(final_df["STOCK"].sum()) if "STOCK" in final_df.columns else 0,
            "series_count": final_df["SERIES"].nunique() if "SERIES" in final_df.columns else 0,
        }

        subject, body = _email_content("success", summary)
        send_email(
            subject, body,
            LocalConfig.GMAIL_SENDER, LocalConfig.GMAIL_APP_PASSWORD,
            LocalConfig.GMAIL_RECIPIENTS,
            attachment_path=LocalConfig.OUTPUT_FILE,
        )

        logger.info("═" * 65)
        logger.info("✅ LOCAL Pipeline completed!  Output → %s", LocalConfig.OUTPUT_FILE)
        logger.info("═" * 65)
        return True

    except Exception:
        logger.error("❌ Pipeline error:\n%s", traceback.format_exc())
        return False


# ──────────────────────────────────────────────────────────────────────────────
# ⑥ Entry point
# ──────────────────────────────────────────────────────────────────────────────
def _schedule_local() -> None:
    schedule.every().day.at(LocalConfig.SCHEDULE_TIME).do(run_pipeline_local)
    logger.info("⏰ Scheduled: runs every day at %s IST.  Press Ctrl+C to stop.", LocalConfig.SCHEDULE_TIME)
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)
    except KeyboardInterrupt:
        logger.info("🛑 Scheduler stopped by user.")


if __name__ == "__main__":
    if RUN_MODE == "github":
        sys.exit(0 if run_pipeline_github() else 1)
    elif len(sys.argv) > 1 and sys.argv[1] == "run":
        sys.exit(0 if run_pipeline_local() else 1)
    else:
        _schedule_local()