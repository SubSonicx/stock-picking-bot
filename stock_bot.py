#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║              STOCK SIGNAL BOT  –  Einzelne Datei            ║
║                                                              ║
║  Installiert fehlende Pakete automatisch beim ersten Start.  ║
║  Starten:  python stock_bot.py                               ║
║  Quick:    python stock_bot.py --quick                       ║
║  Sofort:   python stock_bot.py --now                         ║
║  Test:     python stock_bot.py --test                        ║
╚══════════════════════════════════════════════════════════════╝
"""

# ══════════════════════════════════════════════════════════════════════════════
#  AUTO-INSTALL  –  fehlende Pakete werden automatisch nachinstalliert
# ══════════════════════════════════════════════════════════════════════════════
import subprocess, sys

REQUIRED = ["yfinance", "groq", "python-dotenv", "requests", "schedule",
            "pandas", "numpy", "lxml", "mplfinance", "matplotlib"]

def _auto_install():
    import importlib
    mapping = {"python-dotenv": "dotenv", "scikit-learn": "sklearn"}
    missing = []
    for pkg in REQUIRED:
        mod = mapping.get(pkg, pkg.replace("-", "_").split("[")[0])
        try:
            importlib.import_module(mod)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"⚙️  Installiere fehlende Pakete: {', '.join(missing)} …")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet"] + missing
        )
        print("✓  Pakete installiert. Starte Bot …\n")

_auto_install()

# ══════════════════════════════════════════════════════════════════════════════
#  IMPORTS  (nach Auto-Install)
# ══════════════════════════════════════════════════════════════════════════════
import os, json, re, time, random, logging, argparse
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import requests
import numpy as np
import pandas as pd
import yfinance as yf
from groq import Groq
import schedule
from dotenv import load_dotenv

load_dotenv()

# ══════════════════════════════════════════════════════════════════════════════
#  KONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
BOT_DIR = Path(__file__).parent

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")
GROQ_API_KEY       = os.environ.get("GROQ_API_KEY", "")

MIN_RATING          = 7.0    # Minimum Claude-Rating zum Senden
TOP_CANDIDATES      = 6      # Wie viele Kandidaten an Claude geschickt werden
MAX_SIGNALS_PER_DAY = 3      # Spam-Schutz: max. Signale täglich
COOLDOWN_DAYS       = 14     # Tage bis selbe Aktie erneut gemeldet werden darf
SCAN_HOURS          = [7, 11, 15, 20]  # Uhrzeit der täglichen Scans

CACHE_FILE = BOT_DIR / "sent_cache.json"
LOG_FILE   = BOT_DIR / "stock_bot.log"

# ══════════════════════════════════════════════════════════════════════════════
#  LOGGING
# ══════════════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger("stockbot")

# ══════════════════════════════════════════════════════════════════════════════
#  AKTIEN-UNIVERSUM  (~3000 Titel weltweit)
# ══════════════════════════════════════════════════════════════════════════════

# ── USA ───────────────────────────────────────────────────────────────────────
NASDAQ100 = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AVGO","COST",
    "NFLX","AMD","ADBE","QCOM","INTC","INTU","CMCSA","AMAT","ISRG","MU",
    "LRCX","KLAC","SNPS","CDNS","MRVL","ORLY","ABNB","CRWD","FTNT","DXCM",
    "PANW","MELI","KDP","TEAM","ODFL","FAST","BIIB","IDXX","ILMN","PCAR",
    "ROST","VRSK","MNST","AZN","REGN","MAR","CTAS","PAYX","CPRT","WDAY",
    "NXPI","GEHC","FANG","CEG","DASH","ZS","DDOG","ANSS","TTWO","WBD",
    "SIRI","MTCH","PDD","ENPH","LCID","RIVN","COIN","HOOD","RBLX","ROKU",
    "PLTR","SHOP","NET","MDB","SNOW","OKTA","TWLO","U","GTLB","DOCN",
]
SP500_EXTRA = [
    "JPM","BAC","WFC","GS","MS","C","AXP","BLK","SCHW","USB",
    "V","MA","PYPL","SQ","FI","COF","DFS","SYF","AIG","PRU",
    "UNH","CVS","CI","HUM","ELV","MCK","ABC","CAH","MOH","CNC",
    "JNJ","PFE","MRK","ABBV","BMY","LLY","AMGN","GILD","REGN","VRTX",
    "TMO","DHR","BSX","MDT","ABT","SYK","EW","BDX","ZBH","HOLX",
    "XOM","CVX","COP","EOG","SLB","HAL","BKR","MPC","PSX","VLO",
    "NEE","DUK","SO","AEP","D","EXC","XEL","PEG","SRE","ES",
    "BA","LMT","NOC","GD","RTX","HII","LHX","TDG","HWM","TXT",
    "CAT","DE","HON","GE","ETN","EMR","PH","ROK","IR","AME",
    "AMZN","HD","MCD","SBUX","NKE","TGT","WMT","COST","LOW","TJX",
    "DG","DLTR","KR","SYY","YUM","QSR","DRI","CMG","MKC","CPB",
    "PG","KO","PEP","PM","MO","CL","CLX","CHD","EL","COTY",
    "BRK.B","LIN","APD","ECL","DD","PPG","SHW","NEM","FCX","NUE",
    "EQIX","PLD","AMT","CCI","SPG","PSA","AVB","EQR","MAA","UDR",
    "ETN","PWR","GNRC","AOS","MAS","AWK","WM","RSG","CBRE","JCI",
    "NOW","CRM","ORCL","SAP","INFY","WIT","IBM","CTSH","ACN","EPAM",
    "UBER","LYFT","ABNB","EXPE","BKNG","TRIP","RCL","CCL","NCLH","MAR",
]
RUSSELL_GROWTH = [
    "SMCI","AEHR","ASTS","IONQ","ARRY","FSLR","SEDG","ENPH","RUN","NOVA",
    "WOLF","AEVA","LAZR","INVZ","OUST","LIDR","MVIS","VUZI","UEIC","DMTK",
    "UPST","AFRM","SOFI","LC","OPEN","RDFN","CVNA","VROOM","KMX","AN",
    "MNDY","FRSH","S","SMAR","APPF","PCTY","PAYC","COUP","PAYO","WEX",
    "CELH","BYND","TTCF","APPH","OZON","GRAB","SE","BEKE","KE","YUMC",
    "ACMR","ONTO","FORM","ICHR","COHU","KLIC","MKSI","UCTT","CAMT","AEIS",
    "RXRX","BEAM","EDIT","CRSP","NTLA","PCVX","MRVI","NUVB","IMVT","ARQT",
    "IONM","ACCD","ONEM","HIMS","DOCS","AMWL","TDOC","VCYT","NVAX","MRTX",
    "POWL","ALIT","BFAM","APOG","ATKR","BECN","BMBL","BOOT","CANO","CARG",
    "PRCT","HALO","NARI","INSP","ITRI","LMAT","NVCR","PRVA","TMDX","XENE",
]

# ── Europa erweitert ──────────────────────────────────────────────────────────
DAX = [
    "ADS.DE","AIR.DE","ALV.DE","BAYN.DE","BAS.DE","BMW.DE","CBK.DE",
    "CON.DE","DHL.DE","DTE.DE","EOAN.DE","FME.DE","FRE.DE","HEI.DE",
    "HEN3.DE","IFX.DE","LIN.DE","MBG.DE","MRK.DE","MTX.DE","MUV2.DE",
    "P911.DE","PAH3.DE","RHM.DE","RWE.DE","SAP.DE","SIE.DE","VNA.DE",
    "VOW3.DE","ZAL.DE","DB1.DE","DBK.DE","SHL.DE","SY1.DE","BEI.DE",
]
MDAX = [
    "AIL.DE","AFX.DE","BOSS.DE","CARL.DE","CMNDF","DRW3.DE","EVD.DE",
    "FPE3.DE","G1A.DE","GXI.DE","HAB.DE","HHFA.DE","HOT.DE","HYQ.DE",
    "INN1.DE","JEN.DE","KGX.DE","KSB.DE","LEG.DE","LHA.DE","MDG1.DE",
    "NDX1.DE","NOEJ.DE","O2D.DE","PSM.DE","PUM.DE","RAA.DE","RRTL.DE",
    "SCK.DE","SDX.DE","SDF.DE","SKB.DE","SMHN.DE","SPM.DE","SRT3.DE",
    "SZG.DE","TKA.DE","TTK.DE","UTDI.DE","VBK.DE","VIB3.DE","WAF.DE",
    "WCH.DE","WUW.DE","ZO1.DE","1COV.DE","DHER.DE","DWNI.DE","EMR.DE",
]
SMI = [
    "ABBN.SW","ALC.SW","GEBN.SW","GIVN.SW","HOLN.SW","LONN.SW",
    "NESN.SW","NOVN.SW","ROG.SW","SCMN.SW","SIKA.SW","SREN.SW",
    "UBSG.SW","ZURN.SW","TERN.SW","VAT.SW","PGHN.SW","BAER.SW",
    "CSGN.SW","KNIN.SW","LEON.SW","BUCN.SW","BCGE.SW","CLIN.SW",
    "DKSH.SW","EFGN.SW","EMMN.SW","HIAG.SW","HELN.SW","MOBN.SW",
]
CAC40 = [
    "AI.PA","AIR.PA","ALO.PA","ATO.PA","BN.PA","BNP.PA","CA.PA",
    "CAP.PA","CS.PA","DSY.PA","ENGI.PA","EL.PA","HO.PA","KER.PA",
    "LR.PA","MC.PA","ML.PA","OR.PA","ORA.PA","RI.PA","RMS.PA",
    "SAF.PA","SAN.PA","SGO.PA","SU.PA","TTE.PA","VIE.PA","VIV.PA",
    "ERF.PA","URW.PA","WLN.PA","FP.PA","SW.PA","STMPA.PA",
]
SBF120_EXTRA = [
    "ACA.PA","AF.PA","AGN.PA","AKZA.PA","ALT.PA","AMUN.PA","BB.PA",
    "BVI.PA","CAPP.PA","CNP.PA","COFA.PA","DBG.PA","DOMO.PA","EDEN.PA",
    "ESSO.PA","ELIS.PA","FNAC.PA","GFC.PA","GLE.PA","GTT.PA","INF.PA",
    "IPS.PA","LOUP.PA","LNA.PA","MERY.PA","NEXI.PA","OPN.PA","PRC.PA",
    "REXL.PA","RXL.PA","SEV.PA","SFCA.PA","SGEF.PA","SLCO.PA","TPEX.PA",
]
FTSE100 = [
    "AAL.L","ABF.L","AZN.L","BA.L","BAE.L","BARC.L","BATS.L","BHP.L",
    "BP.L","DGE.L","EXPN.L","FERG.L","FLTR.L","GLEN.L","GSK.L",
    "HLMA.L","HSBA.L","IAG.L","IHG.L","IMB.L","ITRK.L","LGEN.L",
    "LLOY.L","NWG.L","PRU.L","REL.L","RIO.L","RKT.L","RR.L",
    "SHEL.L","TSCO.L","ULVR.L","VOD.L","WPP.L","AHT.L","ANTO.L",
    "AUTO.L","AVV.L","AWE.L","BEZ.L","BNZL.L","BRBY.L","CCH.L",
    "CNA.L","CPG.L","CRH.L","DCC.L","ENT.L","EVR.L","EZJ.L","FRES.L",
    "HIK.L","HL.L","HSBA.L","III.L","INF.L","JD.L","JMAT.L","KGF.L",
    "LAND.L","LSEG.L","MKS.L","MNG.L","MRO.L","NXT.L","PHNX.L","PSN.L",
    "PSON.L","REX.L","RS1.L","SBRY.L","SDR.L","SGE.L","SKG.L","SMDS.L",
    "SMIN.L","SMT.L","SPX.L","SSE.L","STAN.L","SVT.L","TUI.L","UU.L",
    "WEIR.L","WHR.L","WTB.L",
]
STOXX_EXTRA = [
    # Netherlands
    "ASML","HEIA.AS","NN.AS","PHIA.AS","REN.AS","URW.AS","WKL.AS",
    "ADYEN.AS","BESI.AS","IMCD.AS","TKWY.AS","DSYB.AS","GLPG.AS",
    # Sweden
    "ATCO-A.ST","ERIC-B.ST","ESSITY-A.ST","HEXA-B.ST","INVE-B.ST",
    "NDA-SE.ST","SAND.ST","SEB-A.ST","SWED-A.ST","VOLV-B.ST","SHB-A.ST",
    # Denmark
    "CARL-B.CO","CHR.CO","COLO-B.CO","DSV.CO","GN.CO","MAERSK-B.CO",
    "NOVO-B.CO","ORSTED.CO","PNDORA.CO","RBREW.CO","VWS.CO",
    # Spain
    "ACS.MC","AMS.MC","BBVA.MC","BKT.MC","CABK.MC","ELE.MC","ENG.MC",
    "FER.MC","GRF.MC","IAG.MC","IBE.MC","IDGE.MC","ITX.MC","MAP.MC",
    "MRL.MC","REE.MC","REP.MC","SAB.MC","SAN.MC","TEF.MC",
    # Italy
    "A2A.MI","AMP.MI","ATL.MI","BAMI.MI","BMPS.MI","BZU.MI","CPR.MI",
    "ENEL.MI","ENI.MI","ERG.MI","FCA.MI","FHI.MI","G.MI","GEO.MI",
    "HER.MI","ISP.MI","LDO.MI","MONC.MI","PIRC.MI","PRY.MI",
    "REC.MI","SPM.MI","SRG.MI","STM.MI","TIT.MI","TRN.MI","UCG.MI",
    # Belgium / Austria / Portugal
    "ABI.BR","AGS.BR","APAM.BR","ARGX.BR","BPOST.BR","COLR.BR","GBLB.BR",
    "ING.BR","KBC.BR","MELE.BR","PROX.BR","SOF.BR","UCB.BR",
    "AUT.VI","BAN.VI","CAI.VI","EBS.VI","OMV.VI","POST.VI","VIG.VI",
    "BCP.LS","EDP.LS","EGL.LS","ESON.LS","GALP.LS","NOS.LS","SEM.LS",
    # Finland / Norway
    "FORTUM.HE","KESKO-B.HE","NESTE.HE","NOKIA.HE","SAMPO.HE","STERV.HE",
    "DNB.OL","EQNR.OL","MOWI.OL","NHY.OL","ORK.OL","SALM.OL","TEL.OL",
    # Switzerland extra
    "AMS.SW","ARYN.SW","BARN.SW","CICN.SW","CLTN.SW","COTN.SW","FTON.SW",
    "GMVN.SW","HELN.SW","INRN.SW","LISP.SW","METN.SW","MCHN.SW","NATN.SW",
    "ORON.SW","PSPN.SW","SFPN.SW","SLHN.SW","SNBN.SW","SOON.SW","STGN.SW",
]

# ── Asien erweitert ───────────────────────────────────────────────────────────
JAPAN = [
    "7203.T","6758.T","6861.T","9984.T","7974.T","8306.T","6954.T",
    "9432.T","8058.T","6501.T","7267.T","4063.T","6902.T","8316.T",
    "6367.T","9433.T","4661.T","7201.T","6752.T","4519.T","9983.T",
    "6098.T","4543.T","6857.T","8031.T","7741.T","6971.T","8801.T",
    "9020.T","4911.T","4901.T","6503.T","5401.T","7733.T","8411.T",
    "6762.T","7832.T","4689.T","3382.T","8267.T","2413.T","6326.T",
    "4704.T","4307.T","6302.T","7270.T","6723.T","4452.T","8035.T",
]
HK_CHINA = [
    "0700.HK","9988.HK","3690.HK","0941.HK","1299.HK","2318.HK",
    "0005.HK","1211.HK","0388.HK","9618.HK","0883.HK","2269.HK",
    "9999.HK","2382.HK","6098.HK","0175.HK","1024.HK","9626.HK",
    "2020.HK","0669.HK","0762.HK","1038.HK","2628.HK","0386.HK",
    "3968.HK","0002.HK","0003.HK","0006.HK","0011.HK","0012.HK",
    "0016.HK","0017.HK","0019.HK","0027.HK","0066.HK","0083.HK",
    "0101.HK","0151.HK","0267.HK","0291.HK","0322.HK","0358.HK",
]
KOREA_TAIWAN = [
    "005930.KS","000660.KS","035420.KS","051910.KS","005380.KS",
    "000270.KS","068270.KS","035720.KS","003670.KS","207940.KS",
    "006400.KS","028260.KS","096770.KS","017670.KS","030200.KS",
    "TSM","UMC","ASX","LOGI","ACER.TW","2317.TW","2330.TW",
]
INDIA_EM = [
    # India ADRs / US-listed
    "INFY","WIT","HDB","IBN","VEDL","TTM","MFG","SIFY","RECON","MMYT",
    # EM / Global
    "SE","MELI","NU","GRAB","BABA","JD","PDD","BIDU","NIO","LI",
    "XPEV","NTES","IQ","BILI","TME","VNET","WB","YMM","BOSS","KC",
    "VALE","PBR","SQM","LTM","GGB","BAP","BVN","CPAC","VIST","YPF",
    # South Africa / Other EM
    "NPN.JO","CFR.JO","BHP","RIO","GLEN.L","AAL.L","FQVLF","AAUKF",
]

US_GROWTH = [
    "NVDA","AMD","MSFT","AAPL","GOOGL","AMZN","META","TSLA","AVGO","ORCL",
    "CRM","ADBE","SNOW","PLTR","CRWD","ZS","DDOG","NET","MDB","SHOP",
    "SQ","COIN","RBLX","ABNB","UBER","DASH","TWLO","LLY","NVO","ABBV",
    "REGN","VRTX","GILD","MRNA","BNTX","RTX","LMT","NOC","GD","BA",
    "CAT","DE","HON","GE","ETN","XOM","CVX","SLB","FCX","NEM","GOLD",
    "SMCI","ASTS","IONQ","RXRX","BEAM","HIMS","DOCS","TDOC","POWL","PWR",
    "APP","TTWO","EA","TAKE","ZNGA","UNITY","RBLX","MTCH","BMBL","SNAP",
    "PINS","RDDT","DUOL","COUR","UDMY","CHGG","2U","LEGALZOOM","ANGI",
]

QUICK_UNIVERSE = US_GROWTH[:40] + DAX[:15] + SMI[:10] + NASDAQ100[:20]


def get_sp500() -> list[str]:
    try:
        df = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies")[0]
        return [t.replace("-", ".") for t in df["Symbol"].tolist()]
    except Exception as e:
        log.warning(f"S&P500 fetch failed ({e}), using fallback")
        return SP500_EXTRA


def get_full_universe() -> list[str]:
    sp500 = get_sp500()
    all_tickers = list(set(
        sp500 + NASDAQ100 + SP500_EXTRA + RUSSELL_GROWTH +
        DAX + MDAX + SMI + CAC40 + SBF120_EXTRA +
        FTSE100 + STOXX_EXTRA +
        JAPAN + HK_CHINA + KOREA_TAIWAN + INDIA_EM +
        US_GROWTH
    ))
    log.info(f"Universe: {len(all_tickers)} stocks")
    return all_tickers


# ══════════════════════════════════════════════════════════════════════════════
#  SCREENER  –  2-Phase approach for speed
#  Phase 1: Batch download prices for ALL tickers → technical score only (fast)
#  Phase 2: Fetch fundamentals only for top 60 candidates (slow but selective)
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class Stock:
    ticker: str
    name: str = ""
    isin: str = ""
    currency: str = ""
    sector: str = ""
    industry: str = ""
    current_price: float = 0.0
    market_cap: float = 0.0

    # ── Technical indicators ───────────────────────────────────────────────
    rsi: float = 0.0
    stoch_k: float = 0.0          # Stochastic %K
    stoch_d: float = 0.0          # Stochastic %D
    above_ma50: bool = False
    above_ma200: bool = False
    golden_cross: bool = False     # MA50 crossed above MA200 recently
    death_cross: bool = False      # MA50 crossed below MA200 recently
    macd_crossover: bool = False
    macd_bullish: bool = False
    momentum_1m: float = 0.0
    momentum_3m: float = 0.0
    momentum_6m: float = 0.0
    week52_pct: float = 0.0        # % below 52w high (negative = cheaper)
    bb_squeeze: bool = False       # Bollinger Band squeeze (breakout imminent)
    bb_position: float = 0.0      # 0=lower band, 1=upper band
    atr_pct: float = 0.0          # ATR as % of price (volatility)
    volume_ratio: float = 0.0     # Recent vol vs 20d avg (>1.5 = strong signal)
    hammer: bool = False           # Bullish hammer candlestick
    breakout: bool = False         # Price breaking above resistance

    # ── Fundamental ───────────────────────────────────────────────────────
    pe_forward: float = 0.0
    pe_trailing: float = 0.0
    pb_ratio: float = 0.0
    ev_ebitda: float = 0.0
    peg_ratio: float = 0.0
    dividend_yield: float = 0.0    # %
    dividend_growth: float = 0.0   # 5y dividend growth %
    equity_ratio: float = 0.0      # Equity / Total Assets %
    analyst_target: float = 0.0
    analyst_upside: float = 0.0
    num_analysts: int = 0
    recommendation: str = ""
    insider_pct: float = 0.0       # % insider ownership (management skin-in-game)
    short_float: float = 0.0       # Short interest % (contrarian signal)

    # ── Growth & Quality (Moat proxies) ──────────────────────────────────
    revenue_growth: float = 0.0
    earnings_growth: float = 0.0
    gross_margin: float = 0.0      # High gross margin = pricing power / moat
    profit_margin: float = 0.0
    operating_margin: float = 0.0
    roe: float = 0.0               # Return on equity
    roa: float = 0.0               # Return on assets
    roic: float = 0.0              # Return on invested capital (best moat indicator)
    debt_equity: float = 0.0
    current_ratio: float = 0.0     # Liquidity
    free_cashflow_yield: float = 0.0  # FCF / Market cap %
    revenue_per_employee: float = 0.0 # Scalability proxy

    # ── Market context ────────────────────────────────────────────────────
    beta: float = 0.0              # Market sensitivity

    # ── Scores (out of 10 total) ──────────────────────────────────────────
    tech_score: float = 0.0        # max 4.0
    fundamental_score: float = 0.0 # max 3.5
    growth_score: float = 0.0      # max 2.5
    total_score: float = 0.0       # max 10.0


def _rsi(close: pd.Series, n=14) -> float:
    d = close.diff()
    g = d.where(d > 0, 0.0).rolling(n).mean()
    l = (-d.where(d < 0, 0.0)).rolling(n).mean()
    rs = g / l.replace(0, float("nan"))
    v = (100 - 100 / (1 + rs)).iloc[-1]
    return float(v) if not pd.isna(v) else 50.0


def _stochastic(high: pd.Series, low: pd.Series, close: pd.Series, k=14, d=3):
    lowest  = low.rolling(k).min()
    highest = high.rolling(k).max()
    k_line  = 100 * (close - lowest) / (highest - lowest + 1e-9)
    d_line  = k_line.rolling(d).mean()
    return float(k_line.iloc[-1]), float(d_line.iloc[-1])


def _macd(close: pd.Series):
    e12 = close.ewm(span=12, adjust=False).mean()
    e26 = close.ewm(span=26, adjust=False).mean()
    m   = e12 - e26
    s   = m.ewm(span=9, adjust=False).mean()
    return m, s


def _bollinger(close: pd.Series, n=20, k=2):
    mid = close.rolling(n).mean()
    std = close.rolling(n).std()
    upper = mid + k * std
    lower = mid - k * std
    cur = close.iloc[-1]
    bw  = float((upper - lower).iloc[-1] / mid.iloc[-1])   # bandwidth
    pos = float((cur - lower.iloc[-1]) / (upper.iloc[-1] - lower.iloc[-1] + 1e-9))
    # Squeeze: bandwidth in lowest 20% of last 6 months
    bw_series = (upper - lower) / mid
    squeeze = bool(bw <= bw_series.rolling(126).quantile(0.2).iloc[-1])
    return squeeze, round(pos, 2), round(bw * 100, 1)


def _atr(hist: pd.DataFrame, n=14) -> float:
    h, l, c = hist["High"], hist["Low"], hist["Close"]
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return float(tr.rolling(n).mean().iloc[-1])


def _volume_ratio(hist: pd.DataFrame, n=20) -> float:
    vol = hist["Volume"].dropna()
    if len(vol) < n + 3:
        return 1.0
    avg = float(vol.rolling(n).mean().iloc[-2])   # exclude today
    recent = float(vol.iloc[-3:].mean())
    return round(recent / avg, 2) if avg > 0 else 1.0


def _golden_cross(close: pd.Series) -> tuple[bool, bool]:
    if len(close) < 205:
        return False, False
    ma50  = close.rolling(50).mean()
    ma200 = close.rolling(200).mean()
    # Golden cross: MA50 crossed above MA200 in last 10 days
    golden = any(
        ma50.iloc[-(i+1)] > ma200.iloc[-(i+1)] and ma50.iloc[-(i+2)] <= ma200.iloc[-(i+2)]
        for i in range(min(10, len(ma50)-2))
    )
    # Death cross: MA50 crossed below MA200 in last 10 days
    death = any(
        ma50.iloc[-(i+1)] < ma200.iloc[-(i+1)] and ma50.iloc[-(i+2)] >= ma200.iloc[-(i+2)]
        for i in range(min(10, len(ma50)-2))
    )
    return golden, death


def _hammer_candle(hist: pd.DataFrame) -> bool:
    """Detect bullish hammer in last 3 candles."""
    for i in range(-3, 0):
        try:
            o = hist["Open"].iloc[i]
            h = hist["High"].iloc[i]
            l = hist["Low"].iloc[i]
            c = hist["Close"].iloc[i]
            body   = abs(c - o)
            lower_wick = min(o, c) - l
            upper_wick = h - max(o, c)
            if body > 0 and lower_wick >= 2 * body and upper_wick <= body * 0.3 and c > o:
                return True
        except Exception:
            pass
    return False


def _breakout(close: pd.Series, lookback=20) -> bool:
    """Price breaking above recent resistance (20-day high)."""
    if len(close) < lookback + 5:
        return False
    resistance = float(close.iloc[-(lookback+1):-1].max())
    return float(close.iloc[-1]) > resistance * 1.005


def _pct(close: pd.Series, days: int) -> float:
    if len(close) < days + 1:
        return 0.0
    return float((close.iloc[-1] / close.iloc[-(days + 1)] - 1) * 100)



# ── Bekannte ISINs (Yahoo Finance liefert diese oft nicht mehr) ───────────────
KNOWN_ISINS: dict[str, str] = {
    # US Mega-Cap Tech
    "NVDA":  "US67066G1040", "AMD":   "US0079031078", "MSFT":  "US5949181045",
    "AAPL":  "US0378331005", "GOOGL": "US02079K3059", "GOOG":  "US02079K1079",
    "AMZN":  "US0231351067", "META":  "US30303M1027", "TSLA":  "US88160R1014",
    "AVGO":  "US11135F1012", "ORCL":  "US68389X1054", "CRM":   "US79466L3024",
    "ADBE":  "US00724F1012", "INTC":  "US4581401001", "QCOM":  "US7475251036",
    "TXN":   "US8825081040", "AMAT":  "US0138501014", "MU":    "US6283711064",
    "LRCX":  "US5486611073", "KLAC":  "US4824801009",
    # US Cloud / SaaS
    "SNOW":  "US8334451098", "PLTR":  "US69608A1088", "CRWD":  "US22788C1053",
    "ZS":    "US98980G1022", "DDOG":  "US23804L1035", "NET":   "US18915M1071",
    "MDB":   "US60937P1066", "SHOP":  "US82981L1085", "NOW":   "US81762P1021",
    "WDAY":  "US98138H1014", "OKTA":  "US6792951054", "TWLO":  "US90138F1021",
    # US Finance
    "JPM":   "US46625H1005", "BAC":   "US0605051046", "GS":    "US38141G1040",
    "MS":    "US6174464486", "WFC":   "US9497461015", "C":     "US1729674242",
    "V":     "US92826C8394", "MA":    "US57636Q1040", "AXP":   "US0258161092",
    "BRK-B": "US0846707026",
    # US Healthcare
    "LLY":   "US5324571083", "NVO":   "US6709051084", "ABBV":  "US00287Y1091",
    "REGN":  "US75886F1075", "VRTX":  "US92532F1003", "GILD":  "US3755581036",
    "MRNA":  "US60770K1079", "PFE":   "US7170811035", "JNJ":   "US4781601046",
    "UNH":   "US91324P1021", "TMO":   "US8835561023", "ABT":   "US0028241000",
    # US Industrie / Energie
    "CAT":   "US1491231015", "DE":    "US2441991054", "HON":   "US4385161066",
    "GE":    "US3696043013", "ETN":   "US2786421030", "RTX":   "US75513E1010",
    "LMT":   "US5398301094", "NOC":   "US6668071029", "BA":    "US0970231058",
    "XOM":   "US30231G1022", "CVX":   "US1667641005", "SLB":   "US8085131055",
    # US Consumer
    "AMZN":  "US0231351067", "HD":    "US4370761029", "MCD":   "US5801351017",
    "SBUX":  "US8552441094", "NKE":   "US6541061031", "TGT":   "US8793601015",
    "WMT":   "US9311421039", "COST":  "US22160K1051",
    # EM / Global Growth
    "TSM":   "US8740391003", "MELI":  "US58733R1023", "SE":    "US81141R1005",
    "BABA":  "US01609W1027", "PDD":   "US7223041028", "JD":    "US47215P1066",
    # Deutschland
    "SAP.DE":   "DE0007164600", "SIE.DE":   "DE0007236101", "ALV.DE":   "DE0008404005",
    "BAYN.DE":  "DE000BAY0017", "BAS.DE":   "DE000BASF111", "BMW.DE":   "DE0005190003",
    "MBG.DE":   "DE0007100000", "VOW3.DE":  "DE0007664039", "ADS.DE":   "DE000A1EWWW0",
    "DTE.DE":   "DE0005557508", "RHM.DE":   "DE0007030009", "MUV2.DE":  "DE0008430026",
    "DBK.DE":   "DE0005140008", "DHL.DE":   "DE0005552004", "IFX.DE":   "DE0006231004",
    "EOAN.DE":  "DE000ENAG999", "RWE.DE":   "DE0007037129", "HEN3.DE":  "DE0006048432",
    "FRE.DE":   "DE0005785604", "LIN.DE":   "IE00BZ12WP82",
    # Schweiz
    "NESN.SW":  "CH0012221716", "NOVN.SW":  "CH0012221716", "ROG.SW":   "CH0012032048",
    "UBSG.SW":  "CH0244767585", "ABBN.SW":  "CH0012221716", "ZURN.SW":  "CH0011075394",
    "SIKA.SW":  "CH0418792922", "GEBN.SW":  "CH0030170408", "GIVN.SW":  "CH0010645932",
    "LONN.SW":  "CH0013841017", "HOLN.SW":  "CH0012214059", "VAT.SW":   "CH0311864901",
    # Frankreich
    "MC.PA":    "FR0000121014", "OR.PA":    "FR0000120321", "TTE.PA":   "FR0014000MR3",
    "SAN.PA":   "FR0000120578", "BNP.PA":   "FR0000131104", "AIR.PA":   "NL0000235190",
    "RMS.PA":   "FR0000052292", "KER.PA":   "FR0000121485", "DSY.PA":   "FR0014003TT8",
    # UK
    "SHEL.L":   "GB00BP6MXD84", "AZN.L":    "GB0009895292", "HSBA.L":   "GB0005405286",
    "BP.L":     "GB0007980591", "GSK.L":    "GB0009252882", "RIO.L":    "GB0007188757",
    "ULVR.L":   "GB00B10RZP78", "BAE.L":    "GB0002634946", "LLOY.L":   "GB0008706128",
}


def _get_isin(t: yf.Ticker, info: dict) -> str:
    """
    Gets ISIN via: 1) hardcoded table, 2) yfinance property, 3) Yahoo API.
    """
    ticker = t.ticker.upper()

    # Method 1: hardcoded table (most reliable for known stocks)
    if ticker in KNOWN_ISINS:
        return KNOWN_ISINS[ticker]

    # Method 2: yfinance property
    try:
        val = t.isin
        if val and isinstance(val, str) and len(val) >= 10 and val not in ("-", "None", "nan"):
            return val.strip()
    except Exception:
        pass

    # Method 3: info dict
    val = info.get("isin", "") or ""
    if val and len(val) >= 10:
        return val.strip()

    # Method 4: Yahoo Finance quoteSummary API
    try:
        url = (f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
               f"?modules=assetProfile")
        r = requests.get(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            isin = (r.json().get("quoteSummary", {})
                            .get("result", [{}])[0]
                            .get("assetProfile", {})
                            .get("isin", ""))
            if isin and len(isin) >= 10:
                return isin.strip()
    except Exception:
        pass

    return ""


def score_stock(ticker: str) -> Optional[Stock]:
    try:
        t    = yf.Ticker(ticker)
        info = t.info or {}
        s    = Stock(ticker=ticker)

        s.name       = info.get("longName") or info.get("shortName") or ticker
        s.currency   = info.get("currency", "")
        s.sector     = info.get("sector", "")
        s.industry   = info.get("industry", "")
        s.market_cap = info.get("marketCap", 0) or 0
        s.beta       = info.get("beta") or 0.0
        if s.market_cap < 100_000_000:
            return None

        s.isin = _get_isin(t, info)

        hist = t.history(period="1y", auto_adjust=True)
        if hist.empty or len(hist) < 50:
            return None

        close  = hist["Close"].dropna()
        s.current_price = float(close.iloc[-1])
        if s.current_price <= 0:
            return None

        # ── Technical indicators ───────────────────────────────────────────
        ma50  = float(close.rolling(50).mean().iloc[-1])
        ma200 = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else ma50
        s.above_ma50  = s.current_price > ma50
        s.above_ma200 = s.current_price > ma200

        s.golden_cross, s.death_cross = _golden_cross(close)
        s.rsi = _rsi(close)

        # Stochastic
        if "High" in hist.columns and "Low" in hist.columns:
            s.stoch_k, s.stoch_d = _stochastic(hist["High"], hist["Low"], close)

        # MACD
        macd, sig = _macd(close)
        s.macd_bullish   = bool(macd.iloc[-1] > sig.iloc[-1])
        s.macd_crossover = (len(macd) >= 2 and
                            bool(macd.iloc[-1] > sig.iloc[-1]) and
                            bool(macd.iloc[-2] <= sig.iloc[-2]))

        # Momentum
        s.momentum_1m = _pct(close, 21)
        s.momentum_3m = _pct(close, 63)
        s.momentum_6m = _pct(close, 126)
        s.week52_pct  = (s.current_price / (info.get("fiftyTwoWeekHigh") or float(close.max())) - 1) * 100

        # Bollinger Bands
        s.bb_squeeze, s.bb_position, _ = _bollinger(close)

        # ATR (volatility as % of price)
        if "High" in hist.columns and "Low" in hist.columns:
            atr_abs = _atr(hist)
            s.atr_pct = round(atr_abs / s.current_price * 100, 2)

        # Volume
        s.volume_ratio = _volume_ratio(hist)

        # Candlestick & breakout patterns
        s.hammer   = _hammer_candle(hist)
        s.breakout = _breakout(close)

        # ── Fundamental ────────────────────────────────────────────────────
        s.pe_forward   = info.get("forwardPE") or 0.0
        s.pe_trailing  = info.get("trailingPE") or 0.0
        s.pb_ratio     = info.get("priceToBook") or 0.0
        s.ev_ebitda    = info.get("enterpriseToEbitda") or 0.0
        s.peg_ratio    = info.get("trailingPegRatio") or info.get("pegRatio") or 0.0

        # Dividend
        s.dividend_yield  = (info.get("dividendYield") or 0.0) * 100
        s.dividend_growth = (info.get("fiveYearAvgDividendYield") or 0.0)

        # Balance sheet quality
        total_assets = info.get("totalAssets") or 0
        total_equity = info.get("bookValue", 0) * info.get("sharesOutstanding", 0) if info.get("bookValue") else 0
        if total_assets > 0 and total_equity > 0:
            s.equity_ratio = round(total_equity / total_assets * 100, 1)

        s.current_ratio = info.get("currentRatio") or 0.0
        s.short_float   = (info.get("shortPercentOfFloat") or 0.0) * 100
        s.insider_pct   = (info.get("heldPercentInsiders") or 0.0) * 100

        # Analyst consensus
        s.analyst_target = info.get("targetMeanPrice") or 0.0
        s.num_analysts   = info.get("numberOfAnalystOpinions") or 0
        s.recommendation = info.get("recommendationKey", "")
        if s.analyst_target and s.current_price:
            s.analyst_upside = (s.analyst_target / s.current_price - 1) * 100

        # ── Growth & Quality (Moat proxies) ───────────────────────────────
        s.revenue_growth    = (info.get("revenueGrowth")    or 0.0) * 100
        s.earnings_growth   = (info.get("earningsGrowth")   or 0.0) * 100
        s.gross_margin      = (info.get("grossMargins")     or 0.0) * 100
        s.profit_margin     = (info.get("profitMargins")    or 0.0) * 100
        s.operating_margin  = (info.get("operatingMargins") or 0.0) * 100
        s.roe               = (info.get("returnOnEquity")   or 0.0) * 100
        s.roa               = (info.get("returnOnAssets")   or 0.0) * 100
        s.roic              = (info.get("returnOnCapital")  or 0.0) * 100
        s.debt_equity       = info.get("debtToEquity") or 0.0

        # Free cashflow yield
        fcf = info.get("freeCashflow") or 0
        if fcf and s.market_cap:
            s.free_cashflow_yield = round(fcf / s.market_cap * 100, 2)

        # Scalability proxy: revenue per employee
        employees = info.get("fullTimeEmployees") or 0
        revenue   = info.get("totalRevenue") or 0
        if employees > 0 and revenue > 0:
            s.revenue_per_employee = round(revenue / employees / 1000, 0)  # in k USD

        # ── Composite score ────────────────────────────────────────────────
        s.tech_score        = _score_tech(s)
        s.fundamental_score = _score_fund(s)
        s.growth_score      = _score_growth(s)
        s.total_score       = round(s.tech_score + s.fundamental_score + s.growth_score, 2)
        return s

    except Exception as e:
        log.debug(f"score_stock error {ticker}: {e}")
        return None


def _score_tech(s: Stock) -> float:
    """
    Technical score 0–4.0
    MA Trend + Golden/Death Cross: 1.5
    RSI + Stochastic:              1.0
    MACD:                          0.8
    Volume confirmation:           0.4
    Bollinger/Breakout/Hammer:     0.3
    """
    sc = 0.0
    # Trend (0–1.5)
    if s.above_ma200:    sc += 0.7
    if s.above_ma50:     sc += 0.5
    if s.golden_cross:   sc += 0.3   # strong buy signal
    if s.death_cross:    sc -= 0.5   # penalty

    # RSI (0–0.7)
    if   40 <= s.rsi <= 60:  sc += 0.7   # ideal zone
    elif 60 < s.rsi <= 70:   sc += 0.4   # bullish but watch
    elif 30 <= s.rsi < 40:   sc += 0.5   # oversold bounce potential
    elif s.rsi > 75:          sc -= 0.2   # overbought penalty

    # Stochastic (0–0.3): confirms RSI
    if 20 <= s.stoch_k <= 70 and s.stoch_k > s.stoch_d:
        sc += 0.3
    elif s.stoch_k < 25:     # oversold – potential reversal
        sc += 0.2

    # MACD (0–0.8)
    if s.macd_crossover:     sc += 0.8
    elif s.macd_bullish:     sc += 0.4

    # Volume confirmation (0–0.4)
    if s.volume_ratio >= 2.0:    sc += 0.4   # very strong volume
    elif s.volume_ratio >= 1.5:  sc += 0.25
    elif s.volume_ratio >= 1.2:  sc += 0.1

    # Momentum (0–0.4)
    if s.momentum_3m > 15:   sc += 0.25
    elif s.momentum_3m > 5:  sc += 0.15
    if s.momentum_6m > 25:   sc += 0.15
    elif s.momentum_6m > 10: sc += 0.08

    # Bollinger / Pattern (0–0.3)
    if s.bb_squeeze:   sc += 0.15   # breakout imminent
    if s.breakout:     sc += 0.1
    if s.hammer:       sc += 0.05

    return min(max(sc, 0.0), 4.0)


def _score_fund(s: Stock) -> float:
    """
    Fundamental score 0–3.5
    Valuation (P/E, PEG, EV/EBITDA): 1.2
    Analyst targets & consensus:      1.0
    Dividend & balance sheet:         0.8
    Insider ownership:                0.3
    Short interest (contrarian):      0.2
    """
    sc = 0.0
    # Valuation (0–1.2)
    pe = s.pe_forward if s.pe_forward > 0 else s.pe_trailing
    if   0 < pe <= 10:   sc += 1.2
    elif 10 < pe <= 18:  sc += 0.9
    elif 18 < pe <= 28:  sc += 0.5
    elif 28 < pe <= 40:  sc += 0.2
    # PEG bonus (growth-adjusted P/E)
    if 0 < s.peg_ratio <= 1.0:   sc += 0.2
    elif 1.0 < s.peg_ratio <= 2: sc += 0.1
    # EV/EBITDA
    if 0 < s.ev_ebitda <= 10:    sc += 0.15
    elif 10 < s.ev_ebitda <= 18: sc += 0.08

    # Analyst targets (0–1.0)
    if   s.analyst_upside > 40 and s.num_analysts >= 5:  sc += 1.0
    elif s.analyst_upside > 25 and s.num_analysts >= 3:  sc += 0.7
    elif s.analyst_upside > 15 and s.num_analysts >= 2:  sc += 0.4
    elif s.analyst_upside > 10:                          sc += 0.2
    if s.recommendation in ("strong_buy","strongBuy"):   sc += 0.3
    elif s.recommendation == "buy":                      sc += 0.15

    # Dividend (0–0.4)
    if s.dividend_yield > 5:    sc += 0.4
    elif s.dividend_yield > 3:  sc += 0.25
    elif s.dividend_yield > 1:  sc += 0.1

    # Balance sheet quality (0–0.4)
    if s.equity_ratio > 50:     sc += 0.2
    elif s.equity_ratio > 30:   sc += 0.1
    if s.current_ratio > 2:     sc += 0.1
    elif s.current_ratio > 1.2: sc += 0.05
    if 0 < s.free_cashflow_yield > 5:  sc += 0.1

    # Insider ownership – "skin in the game" (0–0.3)
    if s.insider_pct > 20:    sc += 0.3
    elif s.insider_pct > 10:  sc += 0.2
    elif s.insider_pct > 5:   sc += 0.1

    # Short interest – high short = squeeze potential (contrarian) (0–0.2)
    if s.short_float > 20:    sc += 0.2   # potential short squeeze
    elif s.short_float > 10:  sc += 0.1

    return min(max(sc, 0.0), 3.5)


def _score_growth(s: Stock) -> float:
    """
    Growth & Quality / Moat score 0–2.5
    Revenue & earnings growth:    0.9
    Gross margin (moat proxy):    0.5
    ROIC / ROE (moat quality):    0.5
    Scalability (margin + FCF):   0.4
    Debt:                         0.2
    """
    sc = 0.0
    # Revenue growth (0–0.5)
    if   s.revenue_growth > 30:  sc += 0.5
    elif s.revenue_growth > 20:  sc += 0.4
    elif s.revenue_growth > 10:  sc += 0.25
    elif s.revenue_growth > 5:   sc += 0.1

    # Earnings growth (0–0.4)
    if   s.earnings_growth > 40: sc += 0.4
    elif s.earnings_growth > 25: sc += 0.3
    elif s.earnings_growth > 10: sc += 0.15
    elif s.earnings_growth > 0:  sc += 0.05

    # Gross margin = pricing power / moat (0–0.5)
    if   s.gross_margin > 70:   sc += 0.5   # software/pharma level
    elif s.gross_margin > 50:   sc += 0.35
    elif s.gross_margin > 35:   sc += 0.2
    elif s.gross_margin > 20:   sc += 0.1

    # ROIC – best single indicator of moat (0–0.3)
    if   s.roic > 25:  sc += 0.3
    elif s.roic > 15:  sc += 0.2
    elif s.roic > 8:   sc += 0.1

    # ROE quality (0–0.2)
    if   s.roe > 30:   sc += 0.2
    elif s.roe > 15:   sc += 0.1

    # Scalability: operating margin expansion proxy (0–0.2)
    if   s.operating_margin > 30:  sc += 0.2
    elif s.operating_margin > 20:  sc += 0.12
    elif s.operating_margin > 10:  sc += 0.06

    # Debt (0–0.2) – low debt = resilient moat
    if   0 <= s.debt_equity <= 20:   sc += 0.2
    elif 20 < s.debt_equity <= 50:   sc += 0.12
    elif 50 < s.debt_equity <= 100:  sc += 0.05

    return min(max(sc, 0.0), 2.5)


def _tech_score_from_series(close: pd.Series, hist: pd.DataFrame = None) -> float:
    """
    Phase 1 technical score (0–4) from price/volume data only.
    Now includes: MA, Golden Cross, RSI, Stochastic, MACD, Bollinger, Volume, Breakout.
    """
    if len(close) < 60:
        return 0.0
    sc = 0.0

    # MA trend
    ma50  = close.rolling(50).mean().iloc[-1]
    ma200 = close.rolling(200).mean().iloc[-1] if len(close) >= 200 else ma50
    cur   = close.iloc[-1]
    if cur > ma200: sc += 0.7
    if cur > ma50:  sc += 0.5
    if len(close) >= 205:
        gc, dc = _golden_cross(close)
        if gc: sc += 0.3
        if dc: sc -= 0.5

    # RSI
    rsi_val = _rsi(close)
    if   40 <= rsi_val <= 60:  sc += 0.7
    elif 60 < rsi_val <= 70:   sc += 0.4
    elif 30 <= rsi_val < 40:   sc += 0.5
    elif rsi_val > 75:          sc -= 0.2

    # MACD
    m, sig = _macd(close)
    if len(m) >= 2 and m.iloc[-1] > sig.iloc[-1] and m.iloc[-2] <= sig.iloc[-2]:
        sc += 0.8
    elif m.iloc[-1] > sig.iloc[-1]:
        sc += 0.4

    # Bollinger squeeze
    bb_squeeze, _, _ = _bollinger(close)
    if bb_squeeze: sc += 0.15

    # Breakout
    if _breakout(close): sc += 0.1

    # Momentum
    mom3m = _pct(close, 63)
    mom6m = _pct(close, 126)
    if mom3m > 15: sc += 0.25
    elif mom3m > 5: sc += 0.15
    if mom6m > 25: sc += 0.15
    elif mom6m > 10: sc += 0.08

    # Volume (if hist available)
    if hist is not None and "Volume" in hist.columns:
        vr = _volume_ratio(hist)
        if vr >= 2.0:   sc += 0.4
        elif vr >= 1.5: sc += 0.25
        elif vr >= 1.2: sc += 0.1

    return min(max(sc, 0.0), 4.0)


def run_screening(tickers: list[str], delay: float = 0.35) -> list[Stock]:
    """
    2-Phase screening:
    Phase 1 – Batch download 1y prices for all tickers → technical score (fast)
    Phase 2 – Full fundamental analysis for:
      a) Top 120 by technical/momentum score  (momentum plays)
      b) Top 30 by value indicators            (undervalued, low momentum)
    Total candidates for Phase 2: up to 150 unique stocks
    """
    log.info(f"Phase 1: Batch price download for {len(tickers)} tickers …")
    tg_status(f"🔍 Scan started: {len(tickers)} stocks. Phase 1: technical screening …")

    # ── Phase 1: Batch download in chunks of 200 ──────────────────────────
    tech_scores: dict[str, float] = {}
    price_data:  dict[str, pd.Series] = {}
    chunk_size = 200

    for i in range(0, len(tickers), chunk_size):
        chunk = tickers[i:i + chunk_size]
        try:
            raw = yf.download(
                chunk, period="1y", auto_adjust=True,
                progress=False, threads=True, timeout=30
            )
            if isinstance(raw.columns, pd.MultiIndex):
                closes = raw["Close"]
            else:
                closes = raw[["Close"]] if "Close" in raw.columns else raw

            for ticker in chunk:
                try:
                    if ticker in closes.columns:
                        col = closes[ticker].dropna()
                    else:
                        col = closes.dropna() if len(chunk) == 1 else pd.Series()
                    if len(col) >= 60:
                        sc = _tech_score_from_series(col)
                        tech_scores[ticker] = sc
                        price_data[ticker]  = col
                except Exception:
                    pass
        except Exception as e:
            log.debug(f"Batch chunk {i} error: {e}")
        time.sleep(0.5)

    log.info(f"Phase 1 done: {len(tech_scores)} stocks with price data")

    # ── Candidate selection: Momentum top 120 + Value wildcards ──────────
    sorted_by_tech = sorted(tech_scores, key=lambda x: tech_scores[x], reverse=True)

    # a) Top 120 momentum candidates
    momentum_candidates = sorted_by_tech[:120]

    # b) Value wildcards: stocks NOT in top 120 but with recent strong 1M momentum
    #    (catching stocks that may be waking up from undervaluation)
    value_candidates = []
    for ticker in sorted_by_tech[120:]:
        col = price_data.get(ticker)
        if col is not None and len(col) >= 25:
            mom1m = _pct(col, 21)
            # Waking up signal: 1M momentum turning positive from low base
            if 3 <= mom1m <= 20 and tech_scores[ticker] >= 1.5:
                value_candidates.append(ticker)
        if len(value_candidates) >= 30:
            break

    phase2_tickers = list(dict.fromkeys(momentum_candidates + value_candidates))
    log.info(
        f"Phase 2: {len(phase2_tickers)} candidates "
        f"({len(momentum_candidates)} momentum + {len(value_candidates)} value wildcards)"
    )
    tg_status(
        f"📊 Phase 2: Deep analysis of {len(phase2_tickers)} candidates\n"
        f"({len(momentum_candidates)} momentum + {len(value_candidates)} value/turnaround picks)"
    )

    # ── Phase 2: Full fundamental + technical score ────────────────────────
    results = []
    for idx, ticker in enumerate(phase2_tickers, 1):
        if idx % 30 == 0:
            log.info(f"  Phase 2: {idx}/{len(phase2_tickers)} …")
        r = score_stock(ticker)
        if r and r.total_score > 0:
            results.append(r)
        time.sleep(0.4)

    results.sort(key=lambda x: x.total_score, reverse=True)
    log.info(f"Screening done: {len(results)} results from {len(tickers)} universe.")
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  GROQ ANALYZER  –  KI-gestützte Kaufanalyse auf Deutsch (kostenlos)
# ══════════════════════════════════════════════════════════════════════════════
_GROQ = Groq(api_key=GROQ_API_KEY)

_SYSTEM = """You are a senior equity analyst at a Swiss private bank with 20 years of experience.
You identify exceptional long-term buying opportunities (horizon: 3 months to several years).
You are EXTREMELY selective. Rating 9–10 is rarer than once a year. 8–9 is an excellent signal. Only send=true if genuinely convinced.

For every signal you MUST explain WHY each positive metric matters and how it compares to the sector median.
Use concrete sector benchmarks (e.g. "Software median gross margin is ~70%, this stock has 78% → above average moat").

Respond ONLY with a pure JSON object – no markdown, no backticks, no preamble.

{
  "rating": <float 0.0–10.0>,
  "rating_label": "<e.g. STRONG BUY SIGNAL>",
  "summary": "<2–3 sentences why buy now – be specific about what makes this unique>",
  "bull_case": "<1–2 sentences main structural advantage / catalyst>",
  "risk": "<1 sentence main risk>",

  "metric_explanations": {
    "technical": "<explain the 2–3 strongest technical signals and what they mean for price action>",
    "valuation": "<explain P/E, PEG, EV/EBITDA vs sector median – is it cheap or expensive for a reason?>",
    "moat": "<explain gross margin, ROIC, ROE vs sector – does the company have pricing power / competitive moat?>",
    "growth": "<explain revenue and earnings growth rate – is it accelerating? how does it compare to sector?>",
    "management": "<explain insider ownership, ROE trend, capital allocation quality>",
    "market_position": "<comment on sector growth trend (AI, defense, energy etc.) and company's position in it>"
  },

  "price_target": <float 12-month target or null>,
  "price_target_fmt": "<e.g. '240 USD'>",
  "upside_pct": <float>,
  "entry_limit": <float or null>,
  "entry_limit_note": "<e.g. 'Set limit at 131 USD – first support zone at MA50' or null>",
  "sell_target_note": "<e.g. 'Partial profit at 165 USD, then open end' or 'Open End – trend following'>",
  "stop_loss": <float or null>,
  "horizon": "<e.g. '6–12 months' or '2–4 years'>",
  "send": <true/false>
}"""


def analyze_candidate(s: Stock) -> Optional[dict]:
    payload = dict(
        # Identity
        ticker=s.ticker, name=s.name, isin=s.isin, currency=s.currency,
        sector=s.sector, industry=s.industry,
        market_cap_bn=round(s.market_cap / 1e9, 1),
        current_price=s.current_price,
        # Technical
        rsi=round(s.rsi, 1),
        stochastic_k=round(s.stoch_k, 1), stochastic_d=round(s.stoch_d, 1),
        above_ma50=s.above_ma50, above_ma200=s.above_ma200,
        golden_cross=s.golden_cross, death_cross=s.death_cross,
        macd_bullish_crossover=s.macd_crossover, macd_bullish=s.macd_bullish,
        bollinger_squeeze=s.bb_squeeze, bollinger_position=s.bb_position,
        breakout=s.breakout, hammer_candle=s.hammer,
        volume_ratio=s.volume_ratio,
        atr_pct=s.atr_pct,
        momentum_1m=round(s.momentum_1m, 1),
        momentum_3m=round(s.momentum_3m, 1),
        momentum_6m=round(s.momentum_6m, 1),
        pct_from_52w_high=round(s.week52_pct, 1),
        # Valuation
        forward_pe=round(s.pe_forward, 1) if s.pe_forward else None,
        trailing_pe=round(s.pe_trailing, 1) if s.pe_trailing else None,
        peg_ratio=round(s.peg_ratio, 2) if s.peg_ratio else None,
        pb_ratio=round(s.pb_ratio, 2) if s.pb_ratio else None,
        ev_ebitda=round(s.ev_ebitda, 1) if s.ev_ebitda else None,
        dividend_yield_pct=round(s.dividend_yield, 2),
        free_cashflow_yield_pct=round(s.free_cashflow_yield, 2),
        # Analyst consensus
        analyst_target=s.analyst_target,
        analyst_upside_pct=round(s.analyst_upside, 1),
        num_analysts=s.num_analysts,
        recommendation=s.recommendation,
        # Balance sheet & ownership
        equity_ratio_pct=round(s.equity_ratio, 1),
        current_ratio=round(s.current_ratio, 2),
        debt_equity=round(s.debt_equity, 1),
        insider_ownership_pct=round(s.insider_pct, 1),
        short_interest_pct=round(s.short_float, 1),
        # Growth
        revenue_growth_pct=round(s.revenue_growth, 1),
        earnings_growth_pct=round(s.earnings_growth, 1),
        # Moat / Quality
        gross_margin_pct=round(s.gross_margin, 1),
        operating_margin_pct=round(s.operating_margin, 1),
        profit_margin_pct=round(s.profit_margin, 1),
        roe_pct=round(s.roe, 1),
        roa_pct=round(s.roa, 1),
        roic_pct=round(s.roic, 1),
        revenue_per_employee_k=s.revenue_per_employee,
        beta=round(s.beta, 2),
        # Composite scores
        algo_score_total=s.total_score,
        algo_score_technical=s.tech_score,
        algo_score_fundamental=s.fundamental_score,
        algo_score_growth=s.growth_score,
    )
    prompt = (
        f"Analyze this stock and decide if it is a genuine buy signal:\n\n"
        f"{json.dumps(payload, ensure_ascii=False, indent=2)}\n\n"
        "Reply exclusively with the JSON object. "
        "10/10 = next Apple/Nvidia before their breakthrough."
    )
    try:
        resp = _GROQ.chat.completions.create(
            model="llama-3.3-70b-versatile",
            max_tokens=1000,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user",   "content": prompt},
            ],
            temperature=0.3,
        )
        raw = resp.choices[0].message.content.strip()
        raw = re.sub(r"^```json\s*", "", raw)
        raw = re.sub(r"```$", "", raw).strip()
        result = json.loads(raw)
        result["stock"] = s
        if not result.get("send", False):
            log.info(f"  {s.ticker}: KI lehnt ab (Rating {result.get('rating', 0):.1f})")
            return None
        log.info(f"  {s.ticker}: ✅ Signal! Rating {result.get('rating', 0):.1f}/10")
        return result
    except Exception as e:
        log.error(f"Analyse-Fehler {s.ticker}: {e}")
        return None


def analyze_top(candidates: list[Stock], n: int = TOP_CANDIDATES) -> list[dict]:
    approved = []
    log.info(f"Schicke Top-{min(n, len(candidates))} an Groq …")
    for s in candidates[:n]:
        log.info(f"  Analysiere {s.ticker} (Score {s.total_score:.1f}) …")
        r = analyze_candidate(s)
        if r:
            approved.append(r)
    log.info(f"Groq: {len(approved)} Signal(e) genehmigt")
    return approved


# ══════════════════════════════════════════════════════════════════════════════
#  SENTIMENT, NEWS & CHART
# ══════════════════════════════════════════════════════════════════════════════

def get_stocktwits_sentiment(ticker: str) -> dict:
    """Bullish/bearish ratio from StockTwits public API – no key needed."""
    clean = ticker.split(".")[0].replace("-", ".")
    try:
        r = requests.get(
            f"https://api.stocktwits.com/api/2/streams/symbol/{clean}.json",
            timeout=8, headers={"User-Agent": "Mozilla/5.0"}
        )
        if r.status_code != 200:
            return {}
        messages = r.json().get("messages", [])
        if not messages:
            return {}
        bull = sum(1 for m in messages
                   if m.get("entities", {}).get("sentiment", {}).get("basic") == "Bullish")
        bear = sum(1 for m in messages
                   if m.get("entities", {}).get("sentiment", {}).get("basic") == "Bearish")
        total = bull + bear
        if total == 0:
            return {}
        return {"bull_pct": round(bull/total*100), "bear_pct": round(bear/total*100),
                "msg_count": len(messages)}
    except Exception:
        return {}


def get_news_articles(ticker: str) -> list[dict]:
    """
    Fetches real article URLs from yfinance ticker.news.
    Only returns articles with a direct URL (not homepages).
    Max 2 articles shown.
    """
    try:
        t = yf.Ticker(ticker)
        news = t.news or []
        articles = []
        for item in news:
            # yfinance news item structure varies – handle both formats
            content = item.get("content", {}) if isinstance(item.get("content"), dict) else {}
            title = (content.get("title") or item.get("title") or "").strip()
            url   = (content.get("canonicalUrl", {}).get("url") or
                     item.get("link") or item.get("url") or "")
            # Only include if URL is a real article (has path beyond just domain)
            if title and url and url.startswith("http") and url.count("/") >= 4:
                articles.append({"title": title[:60] + ("…" if len(title)>60 else ""), "url": url})
            if len(articles) >= 2:
                break
        return articles
    except Exception:
        return []


def send_candlestick_chart(ticker: str, name: str, currency: str) -> bool:
    """
    Generates a 12-month weekly candlestick chart and sends it as a photo to Telegram.
    """
    import mplfinance as mpf
    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend
    import io

    try:
        t    = yf.Ticker(ticker)
        hist = t.history(period="1y", interval="1wk", auto_adjust=True)
        if hist.empty or len(hist) < 8:
            log.warning(f"Not enough data for chart: {ticker}")
            return False

        # Clean data
        hist = hist[["Open","High","Low","Close","Volume"]].dropna()
        hist.index = pd.to_datetime(hist.index).tz_localize(None)

        # Style
        mc = mpf.make_marketcolors(
            up="#00C853", down="#D50000",
            wick={"up": "#00C853", "down": "#D50000"},
            edge={"up": "#00C853", "down": "#D50000"},
            volume={"up": "#69F0AE", "down": "#FF5252"},
        )
        style = mpf.make_mpf_style(
            marketcolors=mc,
            base_mpf_style="nightclouds",
            figcolor="#0D1117",
            facecolor="#0D1117",
            gridcolor="#1F2937",
            gridstyle="--",
            y_on_right=True,
        )

        buf = io.BytesIO()
        mpf.plot(
            hist,
            type="candle",
            style=style,
            title=f"\n{name}  ({ticker})  –  12-Month Weekly Chart",
            ylabel=f"Price ({currency})",
            volume=True,
            figsize=(12, 7),
            tight_layout=True,
            savefig=dict(fname=buf, format="png", dpi=130, bbox_inches="tight"),
        )
        buf.seek(0)

        r = requests.post(
            f"{_TG_URL}/sendPhoto",
            data={"chat_id": TELEGRAM_CHAT_ID,
                  "caption": f"📈 {name} ({ticker}) – Weekly Candlestick Chart (12M)"},
            files={"photo": ("chart.png", buf, "image/png")},
            timeout=30,
        )
        success = r.status_code == 200
        if not success:
            log.error(f"Chart send failed: {r.status_code} {r.text[:100]}")
        return success

    except Exception as e:
        log.error(f"Chart error {ticker}: {e}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM SENDER
# ══════════════════════════════════════════════════════════════════════════════
_TG_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"


def _stars(rating: float) -> str:
    n = round(rating / 2)
    return "⭐" * n + "☆" * (5 - n)


def _color(rating: float) -> str:
    return "🔴" if rating >= 9 else "🟠" if rating >= 8 else "🟡" if rating >= 7 else "⚪"


def format_message(a: dict) -> str:
    s      = a["stock"]
    rating = a.get("rating", 0)
    cur    = s.currency or ""
    now    = datetime.now().strftime("%d.%m.%Y %H:%M")
    label  = a.get("rating_label", "BUY SIGNAL")
    cp     = f"{s.current_price:.2f} {cur}"

    # ── Header ────────────────────────────────────────────────────────────
    msg  = f"{_color(rating)} <b>{label}: {s.name}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"📌 <b>Ticker:</b> <code>{s.ticker}</code>  |  <b>ISIN:</b> <code>{s.isin if s.isin else 'N/A'}</code>\n"
    if s.sector:
        msg += f"🏭 {s.sector}"
        if s.industry: msg += f" · {s.industry}"
        msg += "\n"

    msg += f"\n{_stars(rating)} <b>Rating: {rating:.1f} / 10</b>\n\n"

    # ── AI Analysis ───────────────────────────────────────────────────────
    msg += f"💡 <b>Analysis:</b>\n{a.get('summary', '')}\n\n"
    msg += f"🚀 <b>Bull Case:</b>\n{a.get('bull_case', '')}\n\n"
    msg += f"⚠️ <b>Main Risk:</b> {a.get('risk', '–')}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"

    # ── Metric Explanations (new!) ────────────────────────────────────────
    me = a.get("metric_explanations", {})
    if me:
        if me.get("technical"):
            msg += f"📈 <b>Technical:</b> {me['technical']}\n\n"
        if me.get("valuation"):
            msg += f"💲 <b>Valuation vs Sector:</b> {me['valuation']}\n\n"
        if me.get("moat"):
            msg += f"🏰 <b>Competitive Moat:</b> {me['moat']}\n\n"
        if me.get("growth"):
            msg += f"📊 <b>Growth:</b> {me['growth']}\n\n"
        if me.get("management"):
            msg += f"👔 <b>Management:</b> {me['management']}\n\n"
        if me.get("market_position"):
            msg += f"🌍 <b>Market Position:</b> {me['market_position']}\n"
        msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"

    # ── Price & Targets ───────────────────────────────────────────────────
    msg += f"💰 <b>Current Price:</b> {cp}\n"
    if a.get("price_target_fmt"):
        msg += f"🎯 <b>Price Target (12M):</b> {a['price_target_fmt']}  <i>(+{a.get('upside_pct', 0):.0f}%)</i>\n"
    if a.get("entry_limit_note"):
        msg += f"📥 <b>Entry:</b> {a['entry_limit_note']}\n"
    if a.get("sell_target_note"):
        msg += f"📤 <b>Exit Target:</b> {a['sell_target_note']}\n"
    if a.get("stop_loss"):
        sl_pct = (a["stop_loss"] / s.current_price - 1) * 100
        msg += f"🛡 <b>Stop-Loss:</b> {a['stop_loss']:.2f} {cur}  <i>({sl_pct:.0f}%)</i>\n"
    msg += f"⏱ <b>Horizon:</b> {a.get('horizon', '–')}\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"

    # ── Key Metrics ───────────────────────────────────────────────────────
    metrics = []
    if s.pe_forward:          metrics.append(f"Fwd P/E: {s.pe_forward:.1f}")
    if s.peg_ratio:           metrics.append(f"PEG: {s.peg_ratio:.1f}")
    if s.ev_ebitda:           metrics.append(f"EV/EBITDA: {s.ev_ebitda:.1f}")
    if s.gross_margin:        metrics.append(f"Gross Margin: {s.gross_margin:.0f}%")
    if s.roic:                metrics.append(f"ROIC: {s.roic:.0f}%")
    if s.revenue_growth:      metrics.append(f"Rev Growth: {s.revenue_growth:.0f}%")
    if s.earnings_growth:     metrics.append(f"EPS Growth: {s.earnings_growth:.0f}%")
    if s.dividend_yield:      metrics.append(f"Dividend: {s.dividend_yield:.1f}%")
    if s.free_cashflow_yield: metrics.append(f"FCF Yield: {s.free_cashflow_yield:.1f}%")
    if s.insider_pct:         metrics.append(f"Insider: {s.insider_pct:.0f}%")
    if s.debt_equity:         metrics.append(f"D/E: {s.debt_equity:.0f}")
    if s.rsi:                 metrics.append(f"RSI: {s.rsi:.0f}")
    if s.stoch_k:             metrics.append(f"Stoch: {s.stoch_k:.0f}")
    if s.volume_ratio > 1.2:  metrics.append(f"Vol: {s.volume_ratio:.1f}x avg")
    tech_flags = []
    if s.golden_cross:   tech_flags.append("🌟Golden Cross")
    if s.bb_squeeze:     tech_flags.append("🔔BB Squeeze")
    if s.breakout:       tech_flags.append("⬆️Breakout")
    if s.hammer:         tech_flags.append("🔨Hammer")
    if s.above_ma50:     tech_flags.append("✓MA50")
    if s.above_ma200:    tech_flags.append("✓MA200")
    if metrics:
        msg += "📊 <b>Key Metrics:</b>\n" + "  ·  ".join(metrics) + "\n"
    if tech_flags:
        msg += "🔔 <b>Signals:</b> " + "  ".join(tech_flags) + "\n"

    # ── Analyst Consensus ─────────────────────────────────────────────────
    if s.recommendation:
        rec_map = {
            "strong_buy": "⬆️ Strong Buy", "strongBuy": "⬆️ Strong Buy",
            "buy": "✅ Buy", "hold": "➡️ Hold",
            "sell": "🔻 Sell", "strong_sell": "⛔ Strong Sell",
        }
        rec_label = rec_map.get(s.recommendation, s.recommendation.title())
        msg += f"🏦 <b>Analyst Consensus:</b> {rec_label}"
        if s.analyst_target:
            msg += f"  |  Target: {s.analyst_target:.2f} {cur}"
        msg += "\n"

    # ── StockTwits Sentiment ───────────────────────────────────────────────
    sentiment = get_stocktwits_sentiment(s.ticker)
    if sentiment:
        bull = sentiment["bull_pct"]
        bear = sentiment["bear_pct"]
        bar_len = 12
        bull_bars = round(bull / 100 * bar_len)
        bar = "🟢" * bull_bars + "🔴" * (bar_len - bull_bars)
        msg += f"💬 <b>StockTwits Sentiment:</b> {bar}\n"
        msg += f"   Bullish {bull}%  ·  Bearish {bear}%  <i>({sentiment['msg_count']} recent posts)</i>\n"

    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"

    # ── Further Reading – only real articles ──────────────────────────────
    articles = get_news_articles(s.ticker)
    if articles:
        msg += "📰 <b>Latest News:</b>\n"
        for art in articles:
            msg += f"  • <a href='{art['url']}'>{art['title']}</a>\n"
        msg += "\n"

    msg += f"\n<i>🤖 Generated: {now}</i>"
    return msg


def format_summary_table(signals: list[dict]) -> str:
    """Sends a ranked summary table after all signals in a scan."""
    if not signals:
        return ""

    # Sort by rating descending
    ranked = sorted(signals, key=lambda x: x.get("rating", 0), reverse=True)

    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    msg  = f"📋 <b>SCAN SUMMARY – {now}</b>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += "<b>Ranked by Rating (best first)</b>\n\n"
    msg += "<code>"
    msg += f"{'#':<2} {'Ticker':<10} {'Rating':>6} {'Price':>10} {'Target':>10} {'Upside':>7}\n"
    msg += "─" * 50 + "\n"

    for i, sig in enumerate(ranked, 1):
        s      = sig["stock"]
        rating = sig.get("rating", 0)
        cur    = s.currency or ""
        price  = f"{s.current_price:.2f}"
        target = sig.get("price_target_fmt", "–")
        upside = f"+{sig.get('upside_pct', 0):.0f}%" if sig.get("upside_pct") else "–"
        ticker = s.ticker[:9]
        msg   += f"{i:<2} {ticker:<10} {rating:>5.1f}★ {price:>9} {str(target):>10} {upside:>7}\n"

    msg += "</code>\n"
    msg += "━━━━━━━━━━━━━━━━━━━━━━━━\n"
    msg += f"<i>{len(ranked)} signal(s) this scan · Max {MAX_SIGNALS_PER_DAY}/day</i>"
    return msg


def tg_send(text: str) -> bool:
    try:
        r = requests.post(
            f"{_TG_URL}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text,
                  "parse_mode": "HTML", "disable_web_page_preview": False},
            timeout=15,
        )
        return r.status_code == 200
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


def tg_status(text: str) -> bool:
    return tg_send(f"ℹ️ <b>Stock Bot:</b> {text}")


def tg_test() -> bool:
    try:
        r = requests.get(f"{_TG_URL}/getMe", timeout=10)
        if r.ok:
            log.info(f"Telegram OK: @{r.json()['result']['username']}")
            return True
    except Exception as e:
        log.error(f"Telegram test failed: {e}")
    return False


# ══════════════════════════════════════════════════════════════════════════════
#  CACHE  –  verhindert Duplikate & Spam
# ══════════════════════════════════════════════════════════════════════════════
def _load_cache() -> dict:
    if not CACHE_FILE.exists():
        return {"sent": {}, "daily": {}}
    try:
        return json.loads(CACHE_FILE.read_text())
    except Exception:
        return {"sent": {}, "daily": {}}


def _save_cache(data: dict):
    CACHE_FILE.write_text(json.dumps(data, indent=2))


def in_cooldown(ticker: str) -> bool:
    c = _load_cache()
    if ticker not in c.get("sent", {}):
        return False
    last = datetime.fromisoformat(c["sent"][ticker]["at"])
    return datetime.now() - last < timedelta(days=COOLDOWN_DAYS)


def mark_sent(ticker: str, rating: float):
    c = _load_cache()
    c.setdefault("sent", {})[ticker] = {"at": datetime.now().isoformat(), "rating": rating}
    _save_cache(c)


def daily_count() -> int:
    c = _load_cache()
    return c.get("daily", {}).get(datetime.now().strftime("%Y-%m-%d"), 0)


def inc_daily():
    c = _load_cache()
    today = datetime.now().strftime("%Y-%m-%d")
    c.setdefault("daily", {})[today] = c.get("daily", {}).get(today, 0) + 1
    _save_cache(c)


def cleanup_cache():
    c = _load_cache()
    cutoff = datetime.now() - timedelta(days=90)
    before = len(c.get("sent", {}))
    c["sent"] = {k: v for k, v in c.get("sent", {}).items()
                 if datetime.fromisoformat(v["at"]) > cutoff}
    if len(c["sent"]) != before:
        _save_cache(c)


# ══════════════════════════════════════════════════════════════════════════════
#  SCAN LOOP
# ══════════════════════════════════════════════════════════════════════════════
def run_scan(quick: bool = False):
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    log.info(f"{'='*55}")
    log.info(f"SCAN GESTARTET: {now}")
    log.info(f"{'='*55}")

    if daily_count() >= MAX_SIGNALS_PER_DAY:
        log.info(f"Tages-Limit erreicht ({MAX_SIGNALS_PER_DAY}). Scan übersprungen.")
        return

    tickers = QUICK_UNIVERSE if quick else get_full_universe()
    random.shuffle(tickers)
    tickers = [t for t in tickers if not in_cooldown(t)]
    log.info(f"{len(tickers)} Ticker nach Cooldown-Filter verbleibend")

    if not tickers:
        log.info("Alle im Cooldown. Scan beendet.")
        return

    results = run_screening(tickers)
    candidates = [r for r in results if r.total_score >= 6.0]
    log.info(f"Kandidaten ≥ 6.0: {len(candidates)}")

    if not candidates:
        log.info("Keine Kandidaten. Warte auf nächsten Scan.")
        return

    signals = analyze_top(candidates)
    sent = []
    sent_signals = []

    for sig in signals:
        if daily_count() >= MAX_SIGNALS_PER_DAY:
            break
        ticker = sig["stock"].ticker
        rating = sig.get("rating", 0)
        if rating < MIN_RATING:
            continue
        log.info(f"Sending: {ticker} (Rating {rating:.1f}/10) …")
        if tg_send(format_message(sig)):
            mark_sent(ticker, rating)
            inc_daily()
            sent.append(ticker)
            sent_signals.append(sig)
            # Send candlestick chart
            stock = sig["stock"]
            log.info(f"  Sending chart for {ticker} …")
            send_candlestick_chart(ticker, stock.name, stock.currency)
            time.sleep(2)

    # Send summary table if more than one signal
    if len(sent_signals) > 1:
        summary = format_summary_table(sent_signals)
        if summary:
            tg_send(summary)

    log.info(f"SCAN DONE: {len(sent)} signal(s) → {', '.join(sent) or '–'}")
    cleanup_cache()


# ══════════════════════════════════════════════════════════════════════════════
#  COMMAND LISTENER  –  /status  /lastsignals  /help  /start
# ══════════════════════════════════════════════════════════════════════════════
_last_update_id = 0


def _handle_commands():
    """Polls Telegram for incoming commands and responds. Non-blocking."""
    global _last_update_id
    try:
        r = requests.get(
            f"{_TG_URL}/getUpdates",
            params={"offset": _last_update_id + 1, "timeout": 2, "limit": 10},
            timeout=8,
        )
        if not r.ok:
            return
        updates = r.json().get("result", [])
        for upd in updates:
            _last_update_id = upd["update_id"]
            msg   = upd.get("message", {})
            text  = msg.get("text", "").strip().lower().split("@")[0]
            chat  = str(msg.get("chat", {}).get("id", ""))

            # Only respond to the configured chat
            if chat != str(TELEGRAM_CHAT_ID):
                continue

            if text in ("/start", "/help"):
                _cmd_help()
            elif text == "/status":
                _cmd_status()
            elif text == "/lastsignals":
                _cmd_lastsignals()

    except Exception as e:
        log.debug(f"Command poll error: {e}")


def _cmd_help():
    msg = (
        "📈 <b>Stock Picking Bot – Help</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━━\n"
        "Scans 800+ global stocks (US, EU, Asia) 4x daily.\n"
        "Signals only sent at rating 7/10 or higher.\n\n"
        "<b>Commands:</b>\n"
        "/status – Bot status and next scan time\n"
        "/lastsignals – Last 3 sent signals\n"
        "/help – This message\n\n"
        "<b>Rating scale:</b>\n"
        "10 – Generational opportunity\n"
        "9  – Exceptional, extremely rare\n"
        "8  – Strong conviction buy\n"
        "7  – Good signal, solid opportunity\n"
        "Below 7 – Not sent\n\n"
        "<i>Not financial advice. Always do your own research.</i>"
    )
    tg_send(msg)


def _cmd_status():
    now  = datetime.now()
    next_scan = None
    for h in sorted(SCAN_HOURS):
        candidate = now.replace(hour=h, minute=5, second=0, microsecond=0)
        if candidate > now:
            next_scan = candidate
            break
    if not next_scan:
        # Next scan is tomorrow at first hour
        import datetime as dt
        tomorrow = now + timedelta(days=1)
        next_scan = tomorrow.replace(hour=sorted(SCAN_HOURS)[0], minute=5, second=0)

    mins_until = int((next_scan - now).total_seconds() / 60)
    sent_today = daily_count()
    cache      = _load_cache()
    total_sent = len(cache.get("sent", {}))

    msg = (
        f"✅ <b>Bot Status</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 Running  |  Uptime: {_uptime()}\n"
        f"⏰ Next scan: {next_scan.strftime('%H:%M')}  <i>(in {mins_until} min)</i>\n"
        f"📊 Signals today: {sent_today} / {MAX_SIGNALS_PER_DAY}\n"
        f"📁 Total signals ever sent: {total_sent}\n"
        f"🌍 Universe: ~800 stocks (US, EU, Asia)\n"
        f"🕐 Scan times: {', '.join(f'{h:02d}:05' for h in SCAN_HOURS)}"
    )
    tg_send(msg)


def _cmd_lastsignals():
    cache = _load_cache()
    sent  = cache.get("sent", {})
    if not sent:
        tg_send("📭 No signals sent yet.")
        return

    # Sort by date, newest first
    sorted_signals = sorted(
        sent.items(),
        key=lambda x: x[1]["at"],
        reverse=True
    )[:3]

    msg = "📋 <b>Last Signals Sent</b>\n━━━━━━━━━━━━━━━━━━━━━━━━\n"
    for ticker, data in sorted_signals:
        sent_at = datetime.fromisoformat(data["at"]).strftime("%d.%m.%Y %H:%M")
        rating  = data.get("rating", "–")
        isin    = KNOWN_ISINS.get(ticker, "–")
        msg += (
            f"📌 <b>{ticker}</b>  |  ISIN: <code>{isin}</code>\n"
            f"   Rating: {rating}/10  ·  Sent: {sent_at}\n\n"
        )
    tg_send(msg)



import signal as _signal
import atexit
import traceback

_BOT_START_TIME: datetime | None = None
_DAEMON_MODE = False   # only send stop msg when running as daemon


def _uptime() -> str:
    if _BOT_START_TIME is None:
        return "–"
    delta = datetime.now() - _BOT_START_TIME
    h, rem = divmod(int(delta.total_seconds()), 3600)
    m = rem // 60
    return f"{h}h {m}min"


def _send_stop_notification(reason: str = "unknown"):
    """Sends a stop alert to Telegram. Called on any exit."""
    if not _DAEMON_MODE:
        return  # don't spam on --test / --quick / --now
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    msg = (
        f"🔴 <b>STOCK BOT STOPPED</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 Time: {now}\n"
        f"⏱ Uptime: {_uptime()}\n"
        f"❓ Reason: {reason}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"<i>Restart: bash ~/Desktop/Stock_picking/start.sh</i>"
    )
    tg_send(msg)
    log.info(f"Stop notification sent: {reason}")


def _handle_sigterm(signum, frame):
    """macOS shutdown / launchctl stop → SIGTERM"""
    _send_stop_notification("System shutdown or manual stop (SIGTERM)")
    sys.exit(0)


def _handle_sigint(signum, frame):
    """Ctrl+C → SIGINT"""
    _send_stop_notification("Manually stopped (Ctrl+C)")
    sys.exit(0)


def _on_crash():
    """Called by atexit – catches unexpected crashes."""
    # Only fires on crash (non-zero exit), not on clean sys.exit(0)
    exc = sys.exc_info()
    if exc[0] is not None and exc[0] not in (SystemExit, KeyboardInterrupt):
        tb = "".join(traceback.format_exception(*exc))[:300]
        _send_stop_notification(f"💥 Crash!\n<code>{tb}</code>")


def main():
    global _BOT_START_TIME, _DAEMON_MODE

    parser = argparse.ArgumentParser(description="Stock Signal Bot")
    parser.add_argument("--quick", action="store_true", help="Small universe, single run")
    parser.add_argument("--now",   action="store_true", help="Immediate full scan")
    parser.add_argument("--test",  action="store_true", help="Telegram connection test only")
    args = parser.parse_args()

    log.info("╔══════════════════════════════════════╗")
    log.info("║      STOCK SIGNAL BOT – Start        ║")
    log.info("╚══════════════════════════════════════╝")

    # Config check
    missing = [k for k, v in {
        "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
        "TELEGRAM_CHAT_ID":   TELEGRAM_CHAT_ID,
        "GROQ_API_KEY":       GROQ_API_KEY,
    }.items() if not v]
    if missing:
        log.error(f"Missing .env values: {', '.join(missing)}")
        log.error("Please fill in the .env file!")
        sys.exit(1)

    if args.test:
        ok = tg_test()
        if ok:
            tg_status("✅ Connection successful – Bot is ready!")
            log.info("Test successful!")
        else:
            log.error("Test failed!")
        return

    if args.quick:
        run_scan(quick=True)
        return

    if args.now:
        run_scan(quick=False)
        # then fall through to scheduler

    # ── Daemon mode ───────────────────────────────────────────────────────
    _DAEMON_MODE = True
    _BOT_START_TIME = datetime.now()

    # Register stop/crash handlers
    _signal.signal(_signal.SIGTERM, _handle_sigterm)
    _signal.signal(_signal.SIGINT,  _handle_sigint)
    atexit.register(_on_crash)

    # Scheduler
    for h in SCAN_HOURS:
        schedule.every().day.at(f"{h:02d}:05").do(run_scan)
        log.info(f"  Scan scheduled: {h:02d}:05")

    tg_status(
        f"✅ Bot started. Scans: {', '.join(f'{h:02d}:05' for h in SCAN_HOURS)}\n"
        f"<i>You'll get a message if the bot stops for any reason.</i>"
    )
    log.info("Scheduler running. Waiting for next scan …")

    try:
        import threading
        def run_scan_threaded():
            t = threading.Thread(target=run_scan, daemon=True)
            t.start()

        # Replace scheduler jobs with threaded version
        schedule.clear()
        for h in SCAN_HOURS:
            schedule.every().day.at(f"{h:02d}:05").do(run_scan_threaded)
            log.info(f"  Scan scheduled: {h:02d}:05")

        while True:
            schedule.run_pending()
            _handle_commands()   # always responsive, even during scans
            time.sleep(5)        # check every 5s for fast command response
    except Exception as e:
        _send_stop_notification(f"💥 Unexpected error: {e}")
        raise


if __name__ == "__main__":
    main()
