import sys
import os
import sqlite3
import traceback
import pandas as pd
import yfinance as yf
from pycoingecko import CoinGeckoAPI
from datetime import datetime, timezone, date
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QDate
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton,
    QTableWidget, QTableWidgetItem, QTabWidget,
    QHBoxLayout, QLabel, QDateEdit, QProgressDialog, QComboBox,
    QMessageBox, QTextEdit
)
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.pyplot as plt

DB_FILE = "crypto_cache.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS crypto_prices (
            coin_id TEXT NOT NULL,
            Date TEXT NOT NULL,
            price REAL,
            PRIMARY KEY (coin_id, Date)
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fiat_rates (
            code TEXT NOT NULL,
            Date TEXT NOT NULL,
            Close REAL,
            PRIMARY KEY (code, Date)
        )
    """)
    conn.commit()
    conn.close()

def save_crypto_cache(coin_id, df):
    if df is None or df.empty:
        return
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    tmp = df.copy()
    if "timestamp" in tmp.columns:
        tmp["Date"] = pd.to_datetime(tmp["timestamp"]).dt.date
    elif "Date" in tmp.columns:
        tmp["Date"] = pd.to_datetime(tmp["Date"]).dt.date
    else:
        tmp = tmp.reset_index()
        tmp["Date"] = pd.to_datetime(tmp.iloc[:, 0]).dt.date
    if "price" not in tmp.columns and "Close" in tmp.columns:
        tmp["price"] = tmp["Close"]
    if "price" not in tmp.columns and tmp.shape[1] >= 2:
        tmp["price"] = tmp.iloc[:, 1]
    rows = []
    for _, r in tmp.iterrows():
        try:
            d = r["Date"].isoformat()
            price = float(r["price"]) if pd.notna(r.get("price")) else None
            rows.append((coin_id, d, price))
        except Exception:
            continue
    if rows:
        cur.executemany("INSERT OR REPLACE INTO crypto_prices (coin_id, Date, price) VALUES (?, ?, ?)", rows)
        conn.commit()
    conn.close()

def load_crypto_cache(coin_id, start_date, end_date):
    conn = sqlite3.connect(DB_FILE)
    q = """
        SELECT Date, price FROM crypto_prices
        WHERE coin_id = ?
        AND Date BETWEEN ? AND ?
        ORDER BY Date ASC
    """
    df = pd.read_sql_query(q, conn, params=(coin_id, start_date.isoformat(), end_date.isoformat()))
    conn.close()
    if df.empty:
        return pd.DataFrame()
    df["Date"] = pd.to_datetime(df["Date"])
    return df.rename(columns={"Date": "timestamp"})

def save_fiat_cache(code, df):
    if df is None or df.empty:
        return
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    tmp = df.copy()
    if "Date" in tmp.columns:
        tmp["Date"] = pd.to_datetime(tmp["Date"]).dt.date
    else:
        tmp = tmp.reset_index()
        tmp["Date"] = pd.to_datetime(tmp.iloc[:, 0]).dt.date
    if "Close" not in tmp.columns and tmp.shape[1] >= 2:
        tmp["Close"] = tmp.iloc[:, 1]
    rows = []
    for _, r in tmp.iterrows():
        try:
            d = r["Date"].isoformat()
            close = float(r["Close"]) if pd.notna(r.get("Close")) else None
            rows.append((code, d, close))
        except Exception:
            continue
    if rows:
        cur.executemany("INSERT OR REPLACE INTO fiat_rates (code, Date, Close) VALUES (?, ?, ?)", rows)
        conn.commit()
    conn.close()

def load_fiat_cache(code, start_date, end_date):
    conn = sqlite3.connect(DB_FILE)
    q = """
        SELECT Date, Close FROM fiat_rates
        WHERE code = ?
        AND Date BETWEEN ? AND ?
        ORDER BY Date ASC
    """
    df = pd.read_sql_query(q, conn, params=(code, start_date.isoformat(), end_date.isoformat()))
    conn.close()
    if df.empty:
        return pd.DataFrame()
    df["Date"] = pd.to_datetime(df["Date"])
    return df

init_db()

class DataWorker(QThread):
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)
    progress = pyqtSignal(int, int)  # current, total
    log = pyqtSignal(str)

    def __init__(self, start_date, end_date, base_currency):
        super().__init__()
        self.start_date = start_date
        self.end_date = end_date
        self.base_currency = base_currency.upper()
        self._stopped = False

    def run(self):
        try:
            result = {"cryptos": [], "fiats": []}
            cg = CoinGeckoAPI()
            vs_currency = self.base_currency.lower()
            top = cg.get_coins_markets(vs_currency=vs_currency, order='market_cap_desc', per_page=20, page=1)

            total_coins = len(top)
            from_ts = int(datetime(self.start_date.year, self.start_date.month, self.start_date.day, tzinfo=timezone.utc).timestamp())
            to_ts = int(datetime(self.end_date.year, self.end_date.month, self.end_date.day, 23, 59, 59, tzinfo=timezone.utc).timestamp())

            for idx, coin in enumerate(top):
                if self._stopped:
                    break
                coin_id = coin.get("id")
                name = coin.get("name")
                self.log.emit(f"Loading data for {name} ({coin_id}) [{idx+1}/{total_coins}]")
                try:
                    hist = cg.get_coin_market_chart_range_by_id(id=coin_id, vs_currency=vs_currency, from_timestamp=from_ts, to_timestamp=to_ts)
                    prices = hist.get("prices", [])
                    df = pd.DataFrame(prices, columns=["timestamp", "price"])
                    if not df.empty:
                        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                        df = df.set_index("timestamp").resample("1D").mean().dropna()
                        avg_price = df["price"].mean() if not df["price"].empty else None
                        hist_df = df.reset_index()
                    else:
                        avg_price = None
                        hist_df = pd.DataFrame()
                except Exception as e:
                    self.log.emit(f"Failed loading {name}: {e}")
                    avg_price = None
                    hist_df = pd.DataFrame()

                try:
                    save_crypto_cache(coin_id, hist_df)
                    self.log.emit(f"Saved cache for {name}")
                except Exception as e:
                    self.log.emit(f"Failed saving cache for {name}: {e}")

                avg_price = float(avg_price) if avg_price is not None else None

                result["cryptos"].append({
                    "id": coin_id,
                    "name": name,
                    "avg_price": avg_price,
                    "history": hist_df
                })

                self.progress.emit(idx + 1, total_coins)

            # Fiat currencies
            fiats = [
                ("EUR", "Euro"),
                ("JPY", "Japanese Yen"),
                ("GBP", "British Pound"),
                ("AUD", "Australian Dollar"),
                ("CAD", "Canadian Dollar"),
                ("CHF", "Swiss Franc"),
                ("CNY", "Chinese Yuan"),
                ("HKD", "Hong Kong Dollar"),
                ("NZD", "New Zealand Dollar"),
                ("BRL", "Brazilian Real")
            ]

            total_fiats = len(fiats)
            total_steps = total_coins + total_fiats

            for idx, (code, name) in enumerate(fiats):
                if self._stopped:
                    break
                self.log.emit(f"Loading fiat data for {name} ({code}) [{idx+1}/{total_fiats}]")
                try:
                    if code == self.base_currency:
                        idx_range = pd.date_range(self.start_date, self.end_date)
                        df = pd.DataFrame({"Close": 1.0}, index=idx_range)
                        avg_rate = 1.0
                        hist_df = df.reset_index().rename(columns={"index": "Date"})
                    else:
                        ticker = f"{code}{self.base_currency}=X"
                        df = yf.download(ticker, start=self.start_date, end=self.end_date, auto_adjust=True, progress=False)
                        if df.empty:
                            ticker_inv = f"{self.base_currency}{code}=X"
                            df_inv = yf.download(ticker_inv, start=self.start_date, end=self.end_date, auto_adjust=True, progress=False)
                            if not df_inv.empty:
                                df = df_inv
                                df["Close"] = 1.0 / df["Close"]
                            else:
                                df = pd.DataFrame()  # keep empty

                        if not df.empty:
                            close_series = pd.to_numeric(df["Close"], errors="coerce").dropna()
                            avg_rate = float(close_series.mean()) if not close_series.empty else None
                            hist_df = df[["Close"]].reset_index().rename(columns={"index": "Date"})
                        else:
                            avg_rate = None
                            hist_df = pd.DataFrame()
                except Exception as e:
                    self.log.emit(f"Failed loading fiat {name}: {e}")
                    avg_rate = None
                    hist_df = pd.DataFrame()

                try:
                    save_fiat_cache(code, hist_df)
                    self.log.emit(f"Saved cache for fiat {name}")
                except Exception as e:
                    self.log.emit(f"Failed saving cache for fiat {name}: {e}")

                result["fiats"].append({
                    "code": code,
                    "name": name,
                    "avg_rate": avg_rate,
                    "history": hist_df
                })

                self.progress.emit(total_coins + idx + 1, total_steps)

            self.finished.emit(result)
        except Exception as e:
            tb = traceback.format_exc()
            self.error.emit(f"Error fetching data: {e}\n{tb}")

    def stop(self):
        self._stopped = True

class MarketApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("GlobalAssetTracker")
        self.resize(1100, 850)
        self.worker = None
        self.data = None

        main_layout = QVBoxLayout()
        filter_layout = QHBoxLayout()

        filter_layout.addWidget(QLabel("Start Date:"))
        self.start_date = QDateEdit()
        self.start_date.setCalendarPopup(True)
        self.start_date.setDate(QDate.currentDate().addYears(-1))
        filter_layout.addWidget(self.start_date)

        filter_layout.addWidget(QLabel("End Date:"))
        self.end_date = QDateEdit()
        self.end_date.setCalendarPopup(True)
        self.end_date.setDate(QDate.currentDate())
        filter_layout.addWidget(self.end_date)

        filter_layout.addWidget(QLabel("Base Currency:"))
        self.base_select = QComboBox()
        self.base_select.addItems(["USD", "EUR", "BRL"])
        filter_layout.addWidget(self.base_select)

        self.load_btn = QPushButton("Load Data")
        self.load_btn.clicked.connect(self.on_load)
        filter_layout.addWidget(self.load_btn)

        main_layout.addLayout(filter_layout)

        self.tabs = QTabWidget()
        self.tab_crypto = QWidget()
        self.tab_fiat = QWidget()
        self.tabs.addTab(self.tab_crypto, "Cryptocurrencies (Top 20)")
        self.tabs.addTab(self.tab_fiat, "Fiat Currencies (Top 10)")

        self.crypto_table = QTableWidget()
        self.crypto_table.setColumnCount(3)
        self.crypto_table.setHorizontalHeaderLabels(["Crypto", "Average Price", "View Chart"])
        crypto_layout = QVBoxLayout()
        crypto_layout.addWidget(self.crypto_table)
        self.tab_crypto.setLayout(crypto_layout)

        self.fiat_table = QTableWidget()
        self.fiat_table.setColumnCount(3)
        self.fiat_table.setHorizontalHeaderLabels(["Currency", "Average Rate", "View Chart"])
        fiat_layout = QVBoxLayout()
        fiat_layout.addWidget(self.fiat_table)
        self.tab_fiat.setLayout(fiat_layout)

        main_layout.addWidget(self.tabs)

        self.figure, self.ax = plt.subplots(figsize=(9, 4))
        self.canvas = FigureCanvas(self.figure)
        main_layout.addWidget(self.canvas)

        main_layout.addWidget(QLabel("Log de Carregamento:"))
        self.log_widget = QTextEdit()
        self.log_widget.setReadOnly(True)
        self.log_widget.setMaximumHeight(150)
        main_layout.addWidget(self.log_widget)

        self.setLayout(main_layout)

        self.progress = None

    def on_load(self):
        start = self.start_date.date().toPyDate()
        end = self.end_date.date().toPyDate()
        base = self.base_select.currentText()
        if start > end:
            QMessageBox.warning(self, "Invalid dates", "Start date must be before end date.")
            return
        self.load_btn.setEnabled(False)
        self.progress = QProgressDialog("Loading market data...", None, 0, 100, self)
        self.progress.setWindowTitle("Please wait")
        self.progress.setWindowModality(Qt.WindowModal)
        self.progress.setMinimumDuration(0)
        self.progress.setValue(0)
        self.progress.show()
        self.log_widget.clear()
        QApplication.processEvents()
        self.worker = DataWorker(start, end, base)
        self.worker.finished.connect(self.on_data_ready)
        self.worker.error.connect(self.on_error)
        self.worker.progress.connect(self.on_progress)
        self.worker.log.connect(self.on_log)
        self.worker.start()

    def on_progress(self, current, total):
        if self.progress and total > 0:
            self.progress.setMaximum(total)
            self.progress.setValue(current)

    def on_log(self, message):
        self.log_widget.append(message)
        self.log_widget.verticalScrollBar().setValue(self.log_widget.verticalScrollBar().maximum())

    def on_error(self, message):
        if self.progress:
            self.progress.close()
        QMessageBox.critical(self, "Error", message)
        self.load_btn.setEnabled(True)
        self.log_widget.append(f"Error: {message}")

    def on_data_ready(self, data):
        self.data = data
        self.populate_crypto_table(data["cryptos"])
        self.populate_fiat_table(data["fiats"])
        if self.progress:
            self.progress.close()
        self.load_btn.setEnabled(True)
        self.ax.clear()
        self.ax.set_title("Select an asset and click 'View Chart' to display here.")
        self.canvas.draw()
        self.log_widget.append("Load finished.")

    def populate_crypto_table(self, cryptos):
        self.crypto_table.setRowCount(len(cryptos))
        for row, coin in enumerate(cryptos):
            name_item = QTableWidgetItem(coin.get("name", ""))
            avg = coin.get("avg_price")
            avg_str = f"{avg:,.4f}" if avg is not None else "N/A"
            avg_item = QTableWidgetItem(f"{avg_str} {self.base_select.currentText()}" if avg is not None else "N/A")
            self.crypto_table.setItem(row, 0, name_item)
            self.crypto_table.setItem(row, 1, avg_item)
            btn = QPushButton("📈 View Chart")
            btn.clicked.connect(lambda _, c=coin: self.plot_crypto(c))
            self.crypto_table.setCellWidget(row, 2, btn)

    def populate_fiat_table(self, fiats):
        self.fiat_table.setRowCount(len(fiats))
        for row, cur in enumerate(fiats):
            name_item = QTableWidgetItem(f"{cur.get('name')} ({cur.get('code')})")
            avg = cur.get("avg_rate")
            avg_str = f"{avg:,.6f}" if avg is not None else "N/A"
            avg_item = QTableWidgetItem(avg_str if avg is not None else "N/A")
            self.fiat_table.setItem(row, 0, name_item)
            self.fiat_table.setItem(row, 1, avg_item)
            btn = QPushButton("📈 View Chart")
            btn.clicked.connect(lambda _, c=cur: self.plot_fiat(c))
            self.fiat_table.setCellWidget(row, 2, btn)

    def plot_crypto(self, coin):
        df = coin.get("history", pd.DataFrame())
        start = self.start_date.date().toPyDate()
        end = self.end_date.date().toPyDate()
        currency = self.base_select.currentText()
        if df.empty:
            df = load_crypto_cache(coin.get("id"), start, end)
        if df.empty:
            QMessageBox.information(self, "No data", "No historical data available for this crypto in the selected period (and cache).")
            return
        if "timestamp" in df.columns:
            x = pd.to_datetime(df["timestamp"])
            y = pd.to_numeric(df.get("price", df.iloc[:, 1] if df.shape[1]>=2 else df.iloc[:,0]), errors="coerce")
        else:
            x = pd.to_datetime(df.iloc[:,0])
            y = pd.to_numeric(df.iloc[:,1], errors="coerce")
        y = y.dropna()
        if y.empty:
            QMessageBox.information(self, "No numeric data", "Historical data exists but contains no numeric values to plot.")
            return
        self.ax.clear()
        self.ax.plot(x.iloc[:len(y)], y, linewidth=1.2)
        self.ax.set_title(f"{coin.get('name')} price ({currency})")
        self.ax.set_ylabel(f"Price ({currency})")
        self.ax.set_xlabel("Date")
        self.ax.grid(True)
        avg = coin.get("avg_price")
        if avg is not None:
            try:
                avg_f = float(avg)
                self.ax.axhline(avg_f, color='orange', linestyle='--', label=f"Average: {avg_f:.4f} {currency}")
                self.ax.legend()
            except Exception:
                pass
        self.figure.autofmt_xdate()
        self.canvas.draw()

    def plot_fiat(self, cur):
        df = cur.get("history", pd.DataFrame())
        currency = self.base_select.currentText()
        if df.empty:
            QMessageBox.information(self, "No data", "No historical data available for this currency in the selected period (and cache).")
            return

        if "Date" in df.columns:
            x = pd.to_datetime(df["Date"])
            y = pd.to_numeric(df.get("Close", df.iloc[:, 1] if df.shape[1]>=2 else df.iloc[:, 0]), errors="coerce")
        else:
            x = pd.to_datetime(df.iloc[:,0])
            y = pd.to_numeric(df.iloc[:,1], errors="coerce")
        y = y.dropna()
        if y.empty:
            QMessageBox.information(self, "No numeric data", "Historical data exists but contains no numeric values to plot.")
            return

        self.ax.clear()
        self.ax.plot(x.iloc[:len(y)], y, linewidth=1.2)
        self.ax.set_title(f"{cur.get('name')} rate ({currency})")
        self.ax.set_ylabel(f"Rate ({currency})")
        self.ax.set_xlabel("Date")
        self.ax.grid(True)
        avg = cur.get("avg_rate")
        if avg is not None:
            try:
                avg_f = float(avg)
                self.ax.axhline(avg_f, color='orange', linestyle='--', label=f"Average: {avg_f:.6f} {currency}")
                self.ax.legend()
            except Exception:
                pass
        self.figure.autofmt_xdate()
        self.canvas.draw()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = MarketApp()
    win.show()
    sys.exit(app.exec())
