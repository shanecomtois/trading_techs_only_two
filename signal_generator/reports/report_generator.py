"""
Modern card-based HTML report generator for trade signals.
Matches UET light theme design from reference template.
"""
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional
from collections import Counter
import logging
import pandas as pd
import html as _html
import re

logger = logging.getLogger(__name__)

# Import AI alignment modules (optional, will fail gracefully if not available)
try:
    # Try multiple import strategies to handle different execution contexts
    try:
        # Strategy 1: Relative import (when run as part of signal_generator package)
        from ..ai import get_or_fetch_ai_alignment, build_trade_payload, determine_structure_type
    except ImportError:
        try:
            # Strategy 2: Absolute import with signal_generator prefix
            from signal_generator.ai import get_or_fetch_ai_alignment, build_trade_payload, determine_structure_type
        except ImportError:
            # Strategy 3: Direct import (when signal_generator is in path)
            from ai import get_or_fetch_ai_alignment, build_trade_payload, determine_structure_type
    AI_ALIGN_AVAILABLE = True
except ImportError as e:
    AI_ALIGN_AVAILABLE = False
    logger.warning(f"AI alignment modules not available. AI alignment will be disabled. Error: {e}")
except Exception as e:
    AI_ALIGN_AVAILABLE = False
    logger.warning(f"AI alignment modules not available. AI alignment will be disabled. Unexpected error: {e}")

# Molecule code to display code mapping
MOLECULE_CODE_MAP = {
    'C3': 'C3',      # Propane
    'C4': 'NC4',     # Normal Butane
    'IC4': 'IC4',    # Isobutane
    'C5': 'C5',      # Natural Gasoline
    'CL': 'CL',      # Crude
    'HO': 'HO',      # Heating Oil
    'NG': 'NG',      # Natural Gas
    'RBOB': 'RBOB'   # Gasoline (RBOB)
}


class ReportGenerator:
    """
    Generates modern, card-based HTML reports for trade signals.
    Matches UET light theme design.
    """
    
    def __init__(self, config: dict):
        """
        Initialize report generator.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.report_settings = config.get('report_settings', {})
        
        # Check if AI alignment is enabled
        ai_align_config = config.get('ai_align', {})
        self.ai_align_enabled = (
            AI_ALIGN_AVAILABLE and 
            ai_align_config.get('enabled', False)
        )
        if self.ai_align_enabled:
            self.ai_align_config = ai_align_config
            logger.info("AI alignment is ENABLED")
        else:
            self.ai_align_config = {}
            if not AI_ALIGN_AVAILABLE:
                logger.info("AI alignment is DISABLED (modules not available)")
            else:
                logger.info("AI alignment is DISABLED (config: enabled=false)")
        
        # Load symbol matrix for metadata lookup (optional, will try to load if available)
        self.symbol_matrix = None
        self._load_symbol_matrix()
    
    def _load_symbol_matrix(self):
        """Load symbol matrix CSV for metadata lookup."""
        try:
            symbol_matrix_path = Path(__file__).parent.parent.parent / 'lists_and_matrix' / 'symbol_matrix.csv'
            if symbol_matrix_path.exists():
                self.symbol_matrix = pd.read_csv(symbol_matrix_path, low_memory=False)
                logger.debug(f"Loaded symbol matrix: {len(self.symbol_matrix)} rows for formal name lookup")
            else:
                logger.debug(f"Symbol matrix not found: {symbol_matrix_path}")
        except Exception as e:
            logger.debug(f"Could not load symbol matrix: {e}")
    
    def _format_location_name(self, location: str) -> str:
        """
        Format location string to title case (e.g., 'MT BELVIEU LST' -> 'Mt Belvieu').
        
        Args:
            location: Location string from symbol matrix
        
        Returns:
            Formatted location name
        """
        if not location or location in ['n/a', '']:
            return ''
        
        # Remove location codes (like LST, NTET) - keep only the main location name
        # Pattern: "MT BELVIEU LST" -> "MT BELVIEU"
        parts = location.split()
        if len(parts) > 1:
            # Check if last part is a code (all caps, 2-5 chars)
            last_part = parts[-1]
            if last_part.isupper() and 2 <= len(last_part) <= 5:
                # Remove the code, keep the location name
                location_name = ' '.join(parts[:-1])
            else:
                location_name = location
        else:
            location_name = location
        
        # Convert to title case: "MT BELVIEU" -> "Mt Belvieu"
        location_name = location_name.title()
        
        return location_name
    
    def _get_molecule_display_code(self, molecule: str, symbol_root: str = '') -> str:
        """
        Get molecule display code from molecule string or symbol root.
        
        Args:
            molecule: Molecule code (e.g., 'C3', 'C4', 'C5')
            symbol_root: Symbol root code (e.g., 'CL', 'HO', 'NG', 'XRB' for RBOB)
        
        Returns:
            Display code (e.g., 'C3', 'NC4', 'IC4', 'C5', 'CL', 'HO', 'NG', 'RBOB')
        """
        # Check symbol root first for special cases
        symbol_root_upper = symbol_root.upper().lstrip('%')
        if symbol_root_upper == 'CL':
            return 'CL'
        elif symbol_root_upper == 'HO':
            return 'HO'
        elif symbol_root_upper == 'NG':
            return 'NG'
        elif symbol_root_upper == 'XRB':
            return 'RBOB'
        
        # Check molecule code mapping
        molecule_upper = molecule.upper() if molecule else ''
        if molecule_upper in MOLECULE_CODE_MAP:
            return MOLECULE_CODE_MAP[molecule_upper]
        
        # Fallback: use molecule as-is or symbol root
        return molecule_upper if molecule_upper else symbol_root_upper
    
    def _get_formal_symbol_name(self, symbol: str, row_data: Dict = None) -> str:
        """
        Get formal symbol name in format "Mt Belvieu NC4" from symbol and metadata.
        
        Args:
            symbol: ICE symbol (e.g., '%NBI F!-IEU')
            row_data: Row data dictionary with metadata (optional, will lookup if not provided)
        
        Returns:
            Formal name (e.g., 'Mt Belvieu NC4') or symbol if metadata not available
        """
        if not symbol:
            return ''
        
        # Try to get metadata from row_data first
        location = None
        molecule = None
        symbol_root = None
        
        if row_data:
            location = row_data.get('location', '')
            molecule = row_data.get('molecule', '')
            symbol_root = row_data.get('symbol_root', '')
        
        # If not in row_data, try to lookup from symbol matrix
        if (not location or location in ['n/a', '']) and self.symbol_matrix is not None:
            try:
                match = self.symbol_matrix[self.symbol_matrix['ice_symbol'] == symbol]
                if len(match) > 0:
                    row = match.iloc[0]
                    location = row.get('location', '')
                    molecule = row.get('molecule', '')
                    symbol_root = row.get('symbol_root', '')
            except Exception as e:
                logger.debug(f"Could not lookup symbol {symbol} in matrix: {e}")
        
        # Extract symbol root from symbol if not available
        if not symbol_root:
            root_match = re.match(r'%([A-Z]+)', symbol)
            if root_match:
                symbol_root = root_match.group(1)
        
        # Format location
        location_formatted = self._format_location_name(location) if location else ''
        
        # Get molecule display code
        molecule_code = self._get_molecule_display_code(molecule, symbol_root)
        
        # Combine: "Mt Belvieu NC4"
        if location_formatted and molecule_code:
            return f"{location_formatted} {molecule_code}"
        elif location_formatted:
            return location_formatted
        elif molecule_code:
            return molecule_code
        else:
            # Fallback: return cleaned symbol
            return symbol.lstrip('%').replace('-IEU', '').strip()
    
    def _extract_display_symbol_for_stats(self, signal: Dict) -> str:
        """
        Extract display symbol from signal for statistics.
        
        For spreads: returns symbol_2 (the leg being bought/sold)
        For outrights: returns the main symbol
        
        Args:
            signal: Signal dictionary with symbol, row_data, etc.
        
        Returns:
            Formal symbol name (e.g., 'Mt Belvieu NC4')
        """
        row_data = signal.get('row_data', {})
        is_outright = row_data.get('is_outright', True)
        is_spread = not is_outright if isinstance(is_outright, bool) else False
        
        if is_spread:
            # For spreads, use symbol_2 (the leg being bought/sold)
            # Buy spread: sell symbol_1, buy symbol_2 -> show symbol_2
            # Sell spread: buy symbol_1, sell symbol_2 -> show symbol_2
            symbol_2 = row_data.get('symbol_2', '')
            if symbol_2:
                # Get metadata for symbol_2
                meta_2 = {}
                
                # Check if symbol_2 is a quarterly formula (starts with '=')
                lookup_symbol = symbol_2
                if symbol_2.startswith('='):
                    # Quarterly formula - try to extract first component symbol
                    # Pattern: =((('%ROOT MONTH!-EXCHANGE')+...
                    component_match = re.search(r"%([A-Z]+)\s+[FGHJKMNQUVXZ]!", symbol_2)
                    if component_match:
                        # Extract first component for lookup (approximation)
                        root = component_match.group(1)
                        # Try to find a monthly symbol with this root to get metadata
                        if self.symbol_matrix is not None:
                            try:
                                # Find first monthly symbol with this root
                                root_matches = self.symbol_matrix[
                                    (self.symbol_matrix['symbol_root'] == root) &
                                    (self.symbol_matrix['quarter_numb'] == 'N')
                                ]
                                if len(root_matches) > 0:
                                    lookup_symbol = root_matches.iloc[0]['ice_symbol']
                            except Exception:
                                pass
                
                # First, try to get metadata from row_data (spread metadata fields)
                location_2 = row_data.get('location_2', '')
                molecule_2 = row_data.get('molecule_2', '')
                symbol_root_2 = row_data.get('symbol_root_2', '')
                if location_2 or molecule_2:
                    meta_2 = {
                        'location': location_2,
                        'molecule': molecule_2,
                        'symbol_root': symbol_root_2
                    }
                # If not in row_data, lookup from symbol matrix
                elif self.symbol_matrix is not None:
                    try:
                        match = self.symbol_matrix[self.symbol_matrix['ice_symbol'] == lookup_symbol]
                        if len(match) > 0:
                            row = match.iloc[0]
                            meta_2 = {
                                'location': row.get('location', ''),
                                'molecule': row.get('molecule', ''),
                                'symbol_root': row.get('symbol_root', '')
                            }
                    except Exception:
                        pass
                
                return self._get_formal_symbol_name(symbol_2, meta_2)
        
        # For outrights, use main symbol
        symbol = signal.get('symbol', '') or signal.get('ice_connect_symbol', '')
        if symbol:
            return self._get_formal_symbol_name(symbol, row_data)
        
        return ''
    
    def generate_html_report(
        self,
        trend_signals: Dict,
        enhanced_trend_signals: Dict = None,
        mean_reversion_signals: Dict = None,
        macd_rsi_exhaustion_signals: Dict = None,
        ice_chat_formatter = None,
        run_date: datetime = None,
        data_date: datetime = None,
        total_symbols: int = 0,
        curve_data: Dict = None
    ) -> str:
        """
        Generate complete HTML report.
        
        Args:
            trend_signals: Dictionary with 'buy_signals' and 'sell_signals' from standard trend following (MACD)
            enhanced_trend_signals: Dictionary with 'buy_signals' and 'sell_signals' from enhanced trend following (multi-trigger)
            mean_reversion_signals: Dictionary with 'buy_signals' and 'sell_signals' from mean reversion
            macd_rsi_exhaustion_signals: Dictionary with 'buy_signals' and 'sell_signals' from MACD/RSI exhaustion
            ice_chat_formatter: ICEChatFormatter instance
            run_date: Date of report generation
            data_date: Actual date from the data (from Date column in CSV)
            total_symbols: Total number of symbols analyzed
        
        Returns:
            HTML string
        """
        if run_date is None:
            run_date = datetime.now()
        if data_date is None:
            data_date = run_date
        if enhanced_trend_signals is None:
            enhanced_trend_signals = {'buy_signals': [], 'sell_signals': []}
        if mean_reversion_signals is None:
            mean_reversion_signals = {'buy_signals': [], 'sell_signals': []}
        if macd_rsi_exhaustion_signals is None:
            macd_rsi_exhaustion_signals = {'buy_signals': [], 'sell_signals': []}
        
        # Build HTML
        html = self._generate_html_header()
        html += self._generate_command_center(run_date, data_date, total_symbols, trend_signals, enhanced_trend_signals, mean_reversion_signals, macd_rsi_exhaustion_signals)
        html += self._generate_strategy_section(
            "📈 MACD Trend Following System",
            "trend_following",
            trend_signals,
            ice_chat_formatter
        )
        html += self._generate_strategy_section(
            "🚀 Enhanced Trend Following Signals",
            "enhanced_trend_following",
            enhanced_trend_signals,
            ice_chat_formatter
        )
        html += self._generate_strategy_section(
            "🔄 Standard Mean Reversion Signals",
            "mean_reversion",
            mean_reversion_signals,
            ice_chat_formatter
        )
        html += self._generate_strategy_section(
            "⚡ MACD/RSI Exhaustion Signals",
            "macd_rsi_exhaustion",
            macd_rsi_exhaustion_signals,
            ice_chat_formatter
        )
        html += self._generate_forward_curve_section(
            trend_signals,
            enhanced_trend_signals,
            mean_reversion_signals,
            macd_rsi_exhaustion_signals,
            curve_data,
            ice_chat_formatter,
            data_date,
            run_date
        )
        html += self._generate_ice_connect_section(
            trend_signals,
            enhanced_trend_signals,
            mean_reversion_signals,
            macd_rsi_exhaustion_signals,
            ice_chat_formatter
        )
        html += self._generate_html_footer()
        
        return html
    
    def _generate_html_header(self) -> str:
        """Generate HTML header with UET light theme CSS."""
        return """<!DOCTYPE html>
<html>
<head>
<meta charset='utf-8'>
<title>Technical Signals Report</title>
<style>
/* ====== UET Light Theme (global) ====== */
:root{
  --bg:#f8fafc;
  --fg:#0f172a;
  --muted:#475569;
  --border:#e5e7eb;
  --card-bg:#ffffff;
  --accent:#0ea5e9;
  --buy:#10b981;
  --sell:#ef4444;
  --shadow:0 8px 30px rgba(0,0,0,0.06);
}
html,body{height:100%}
body{
  margin:0;
  font-family: Inter, Segoe UI, Roboto, Arial, sans-serif;
  background:var(--bg);
  color:var(--fg);
  -webkit-font-smoothing:antialiased;
  -moz-osx-font-smoothing:grayscale;
}
.uet-page{max-width:clamp(1280px, 96vw, 1920px); margin:18px auto 64px; padding:0 18px;}
h1{font-size:28px; font-weight:700; margin:0 0 12px}
h2{font-size:20px; font-weight:700; margin:18px 0 8px}
h3{font-size:16px; font-weight:600; margin:14px 0 6px}
.uet-subtle{color:var(--muted); font-size:13px}
.uet-card{
  background:var(--card-bg); border:1px solid var(--border); border-radius:12px;
  padding:14px 16px; box-shadow:var(--shadow); margin:14px 0;
}
.uet-grid{display:grid; gap:8px}
.uet-grid.cols-2{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}
.uet-grid.cols-3{grid-template-columns:repeat(3,1fr)}
.uet-grid.cols-4{grid-template-columns:repeat(4,1fr)}
.uet-grid.cols-5{grid-template-columns:repeat(5,1fr)}
@media (min-width: 900px){
  .uet-grid.cols-2{grid-template-columns:minmax(0,1fr) minmax(0,1fr)}
  .uet-grid.cols-3{grid-template-columns:repeat(3,1fr)}
  .uet-grid.cols-4{grid-template-columns:repeat(4,1fr)}
  .uet-grid.cols-5{grid-template-columns:repeat(5,1fr)}
}
/* Compact KPI cards */
.uet-kpi-card{background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:10px 12px; box-shadow:0 2px 4px rgba(0,0,0,0.05);}
.uet-kpi-label{font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:4px; font-weight:600}
.uet-kpi-value{font-size:18px; font-weight:700; color:#0f172a; line-height:1.2}
.uet-kpi-subvalue{font-size:11px; color:var(--muted); margin-top:2px}
/* Compact strategy cards */
.uet-strategy-mini{display:flex; align-items:center; justify-content:space-between; padding:8px 10px; background:#f8fafc; border-radius:6px; border-left:3px solid #cbd5e1}
.uet-strategy-mini-name{font-size:11px; font-weight:600; color:#0f172a}
.uet-strategy-mini-stats{display:flex; gap:12px; font-size:11px}
.uet-strategy-mini-stat{text-align:center}
.uet-strategy-mini-stat-label{font-size:9px; color:var(--muted); text-transform:uppercase}
.uet-strategy-mini-stat-value{font-size:14px; font-weight:700; color:#0f172a}
/* Compact alignment badges */
.uet-alignment-compact{display:flex; gap:8px; flex-wrap:wrap; align-items:center}
.uet-alignment-badge{display:inline-flex; align-items:center; gap:4px; padding:4px 8px; background:#f1f5f9; border-radius:4px; font-size:10px; font-weight:600}
.uet-pill{display:inline-block; border-radius:999px; padding:2px 10px; font-size:10px; color:#fff}
.uet-pill.buy{background:var(--buy)}
.uet-pill.sell{background:var(--sell)}
/* Banners */
.uet-banner{display:inline-block; padding:6px 12px; border-radius:10px; color:#fff !important; font-weight:600}
.uet-banner.buy{background:#10b981 !important; color:#fff !important}
.uet-banner.sell{background:#ef4444 !important; color:#fff !important}
/* Explanation Cards */
.uet-explanation-card{
  background:linear-gradient(135deg, #f8fafc 0%, #f1f5f9 100%);
  border:1px solid #e2e8f0;
  border-radius:12px;
  padding:16px 20px;
  margin:12px 0;
  box-shadow:0 2px 8px rgba(0,0,0,0.04);
  transition:all 0.2s ease;
}
.uet-explanation-card:hover{
  box-shadow:0 4px 12px rgba(0,0,0,0.08);
  transform:translateY(-1px);
}
.uet-explanation-card h4{
  margin:0 0 12px 0;
  color:var(--fg);
  font-size:16px;
  font-weight:600;
}
.uet-explanation-card p{
  margin:8px 0;
  color:var(--muted);
  font-size:14px;
  line-height:1.5;
}
.uet-explanation-card ul{
  margin:8px 0;
  padding-left:20px;
}
.uet-explanation-card li{
  margin:4px 0;
  color:var(--muted);
  font-size:13px;
  line-height:1.4;
}
.uet-explanation-card strong{
  color:var(--fg);
  font-weight:600;
}

/* Tables */
table.uet-table{width:100%; border-collapse:collapse; background:var(--card-bg); border:1px solid var(--border); border-radius:12px; overflow:hidden; box-shadow:var(--shadow); table-layout:fixed}
table.uet-table thead th{
  background:#f1f5f9; text-align:left; padding:8px 10px; border-bottom:2px solid var(--border); border-right:1px solid var(--border); font-size:11px; color:#0f172a;
}
table.uet-table thead th:last-child{border-right:none}
table.uet-table thead th:nth-child(1){width:20%}  /* ICE Symbol */
table.uet-table thead th:nth-child(2){width:10%}  /* Strategy_Type */
table.uet-table thead th:nth-child(3){width:6%}   /* Signal */
table.uet-table thead th:nth-child(4){width:8%}   /* Price */
table.uet-table thead th:nth-child(5){width:8%}   /* Stop */
table.uet-table thead th:nth-child(6){width:8%}   /* Target */
table.uet-table thead th:nth-child(7){width:6%}   /* Pos % */
table.uet-table thead th:nth-child(8){width:6%}   /* Score */
table.uet-table thead th:nth-child(9){width:6%}   /* PRWK (or AI Align if enabled) */
table.uet-table thead th:nth-child(10){width:16%} /* Entry Date (or AI Conf/PRWK if AI enabled) */
table.uet-table thead th:nth-child(11){width:6%}  /* PRWK (if AI enabled) */
table.uet-table thead th:nth-child(12){width:16%} /* Entry Date (if AI enabled) */
table.uet-table tbody td{
  padding:8px 10px; border-bottom:1px solid var(--border); border-right:1px solid var(--border); vertical-align:top; font-size:11px; word-wrap:break-word; overflow-wrap:break-word;
}
table.uet-table tbody td:last-child{border-right:none}
table.uet-table tbody tr:last-child td{border-bottom:none}
.uet-num{text-align:right}
.uet-center{text-align:center}
.uet-note{color:var(--muted); font-size:10px; margin-top:6px}
/* ICE Chat + Score rows */
tr.uet-icechat td{ background:#e0f2fe; color:#000000; padding:4px 8px; }  /* Very light blue background with black font for ICE Chat */
tr.uet-scoredetails td{ background:#f0f9ff; color:#000000; padding:4px 8px; }  /* Lightest blue background with black font for Score details */
tr.uet-ai-analysis td{ background:#dcfce7; color:#000000; padding:4px 8px; }  /* Light green background with black font for AI Analysis */
.icechat-line{ font-size:10px; color:#000000; font-weight:bold; font-style:normal; padding:1px 2px; margin:0; line-height:1.3; }

/* Signal header row (above each signal) */
tr.uet-signal-header th{ 
  border-bottom: 2px solid rgba(255,255,255,0.3); 
  text-align: left;
}

/* Alignment icons */
.uet-alignment-icon{
  display: inline-block;
  text-align: center;
  min-width: 60px;
  font-size: 16px;
  line-height: 1.2;
}

/* Fallback badge */
.uet-fallback-badge{
  display: inline-block;
  background: #f97316;
  color: #ffffff;
  font-size: 11px;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 4px;
  margin-left: 8px;
  vertical-align: middle;
}

/* Fallback row styling */
tr.uet-fallback-row td{
  background: #fff7ed;
  opacity: 0.9;
}

/* Legend styling */
.legend-item{
  display: flex;
  align-items: center;
  margin: 8px 0;
  padding: 4px 0;
  font-size: 13px;
}
.legend-item strong{
  margin-left: 8px;
  color: var(--fg);
}
</style>

<style>
/* UET banner-kpis and buy/sell banners */
.banner{font-weight:700;letter-spacing:.5px;margin:16px 0 8px 0;padding:10px 14px;border-radius:8px;color:#0b1220;display:inline-block;box-shadow:0 1px 2px rgba(0,0,0,0.06);}
.banner-buy{background:#16a34a20;border:1px solid #16a34a33;}
.banner-buy::before{content:"BUY";background:#16a34a;margin-right:10px;padding:2px 8px;border-radius:999px;font-size:.75rem;color:#fff;}
.banner-sell{background:#dc262620;border:1px solid #dc262633;}
.banner-sell::before{content:"SELL";background:#dc2626;margin-right:10px;padding:2px 8px;border-radius:999px;font-size:.75rem;color:#fff;}
</style>

</head>
<body>
<div class='uet-page'>
  <h1>Technical Signals Report</h1>
"""
    
    def _generate_command_center(self, run_date: datetime, data_date: datetime, total_symbols: int,
                                 trend_signals: Dict, enhanced_trend_signals: Dict = None, mean_reversion_signals: Dict = None, macd_rsi_exhaustion_signals: Dict = None) -> str:
        """Generate compact command center with all KPIs, strategies, and alignment in one card."""
        if enhanced_trend_signals is None:
            enhanced_trend_signals = {'buy_signals': [], 'sell_signals': []}
        if mean_reversion_signals is None:
            mean_reversion_signals = {'buy_signals': [], 'sell_signals': []}
        if macd_rsi_exhaustion_signals is None:
            macd_rsi_exhaustion_signals = {'buy_signals': [], 'sell_signals': []}
        
        # Collect all signals for KPI calculations
        all_signals = []
        all_signals.extend(trend_signals.get('buy_signals', []))
        all_signals.extend(trend_signals.get('sell_signals', []))
        all_signals.extend(enhanced_trend_signals.get('buy_signals', []))
        all_signals.extend(enhanced_trend_signals.get('sell_signals', []))
        all_signals.extend(mean_reversion_signals.get('buy_signals', []))
        all_signals.extend(mean_reversion_signals.get('sell_signals', []))
        all_signals.extend(macd_rsi_exhaustion_signals.get('buy_signals', []))
        all_signals.extend(macd_rsi_exhaustion_signals.get('sell_signals', []))
        
        high_conviction = [s for s in all_signals if s.get('points', 0) >= 75]
        total_available = len(all_signals)
        
        # Count alignment categories
        strong_agree = len([s for s in all_signals if s.get('alignment_score', 0) >= 90])
        agree = len([s for s in all_signals if 80 <= s.get('alignment_score', 0) < 90])
        neutral = len([s for s in all_signals if 70 <= s.get('alignment_score', 0) < 80])
        disagree = len([s for s in all_signals if 60 <= s.get('alignment_score', 0) < 70])
        strong_disagree = len([s for s in all_signals if s.get('alignment_score', 0) < 60])
        
        # Strategy counts
        trend_buy = len([s for s in trend_signals.get('buy_signals', []) if s.get('points', 0) >= 75])
        trend_sell = len([s for s in trend_signals.get('sell_signals', []) if s.get('points', 0) >= 75])
        enhanced_buy = len([s for s in enhanced_trend_signals.get('buy_signals', []) if s.get('points', 0) >= 75])
        enhanced_sell = len([s for s in enhanced_trend_signals.get('sell_signals', []) if s.get('points', 0) >= 75])
        mr_buy = len([s for s in mean_reversion_signals.get('buy_signals', []) if s.get('points', 0) >= 75])
        mr_sell = len([s for s in mean_reversion_signals.get('sell_signals', []) if s.get('points', 0) >= 75])
        exhaustion_buy = len([s for s in macd_rsi_exhaustion_signals.get('buy_signals', []) if s.get('points', 0) >= 75])
        exhaustion_sell = len([s for s in macd_rsi_exhaustion_signals.get('sell_signals', []) if s.get('points', 0) >= 75])
        
        # Use actual data_date from the DataFrame (not run_date)
        data_date_str = data_date.strftime('%Y-%m-%d')
        weekday = data_date.strftime('%A')
        is_current = " (CURRENT)" if (datetime.now() - data_date).days <= 7 else ""
        
        return f"""
  <div class='uet-card' style='padding:12px 14px; margin-bottom:12px;'>
    <div class='uet-grid cols-5' style='margin-bottom:10px;'>
      <div class='uet-kpi-card'>
        <div class='uet-kpi-label'>Run Date</div>
        <div class='uet-kpi-value'>{run_date.strftime('%Y-%m-%d')}</div>
      </div>
      <div class='uet-kpi-card'>
        <div class='uet-kpi-label'>Data Date</div>
        <div class='uet-kpi-value'>{data_date_str}</div>
        <div class='uet-kpi-subvalue'>{weekday}{is_current}</div>
      </div>
      <div class='uet-kpi-card'>
        <div class='uet-kpi-label'>High-Conviction (≥75)</div>
        <div class='uet-kpi-value'>{len(high_conviction)}</div>
        <div class='uet-kpi-subvalue'>of {total_available} total</div>
      </div>
      <div class='uet-kpi-card'>
        <div class='uet-kpi-label'>Alignment</div>
        <div class='uet-kpi-value'>{strong_agree + agree}</div>
        <div class='uet-kpi-subvalue'>{strong_agree}🔥 {agree}⭐</div>
      </div>
      <div class='uet-kpi-card'>
        <div class='uet-kpi-label'>Low Confidence</div>
        <div class='uet-kpi-value'>{strong_disagree + disagree}</div>
        <div class='uet-kpi-subvalue'>{disagree}⚠️ {strong_disagree}💥</div>
      </div>
    </div>
    <div style='display:flex; flex-direction:column; gap:6px; margin-bottom:10px;'>
      <div class='uet-strategy-mini'>
        <div class='uet-strategy-mini-name'>📈 MACD Trend Following</div>
        <div class='uet-strategy-mini-stats'>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Buy</div><div class='uet-strategy-mini-stat-value'>{trend_buy}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Sell</div><div class='uet-strategy-mini-stat-value'>{trend_sell}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Total</div><div class='uet-strategy-mini-stat-value'>{trend_buy + trend_sell}</div></div>
        </div>
      </div>
      <div class='uet-strategy-mini'>
        <div class='uet-strategy-mini-name'>🚀 Enhanced Trend Following</div>
        <div class='uet-strategy-mini-stats'>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Buy</div><div class='uet-strategy-mini-stat-value'>{enhanced_buy}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Sell</div><div class='uet-strategy-mini-stat-value'>{enhanced_sell}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Total</div><div class='uet-strategy-mini-stat-value'>{enhanced_buy + enhanced_sell}</div></div>
        </div>
      </div>
      <div class='uet-strategy-mini'>
        <div class='uet-strategy-mini-name'>🔄 Mean Reversion</div>
        <div class='uet-strategy-mini-stats'>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Buy</div><div class='uet-strategy-mini-stat-value'>{mr_buy}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Sell</div><div class='uet-strategy-mini-stat-value'>{mr_sell}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Total</div><div class='uet-strategy-mini-stat-value'>{mr_buy + mr_sell}</div></div>
        </div>
      </div>
      <div class='uet-strategy-mini'>
        <div class='uet-strategy-mini-name'>⚡ MACD/RSI Exhaustion</div>
        <div class='uet-strategy-mini-stats'>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Buy</div><div class='uet-strategy-mini-stat-value'>{exhaustion_buy}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Sell</div><div class='uet-strategy-mini-stat-value'>{exhaustion_sell}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Total</div><div class='uet-strategy-mini-stat-value'>{exhaustion_buy + exhaustion_sell}</div></div>
        </div>
      </div>
    </div>
    <div style='padding:8px 10px; background:#f8fafc; border-radius:6px; border:1px solid #e2e8f0;'>
      <div class='uet-alignment-compact'>
        <div class='uet-alignment-badge'><span>🔥</span> <span>Strong Agree (90+)</span></div>
        <div class='uet-alignment-badge'><span>⭐</span> <span>Agree (80-89)</span></div>
        <div class='uet-alignment-badge'><span>⚡</span> <span>Neutral (70-79)</span></div>
        <div class='uet-alignment-badge'><span>⚠️</span> <span>Disagree (60-69)</span></div>
        <div class='uet-alignment-badge'><span>💥</span> <span>Strong Disagree (<60)</span></div>
      </div>
    </div>
  </div>
"""
    
    def _generate_run_kpis_card(self, run_date: datetime, data_date: datetime, total_symbols: int, 
                                trend_signals: Dict, mean_reversion_signals: Dict) -> str:
        """Generate Run KPIs card."""
        # Count high-conviction signals (>= 75 points)
        all_signals = []
        all_signals.extend(trend_signals.get('buy_signals', []))
        all_signals.extend(trend_signals.get('sell_signals', []))
        all_signals.extend(mean_reversion_signals.get('buy_signals', []))
        all_signals.extend(mean_reversion_signals.get('sell_signals', []))
        
        high_conviction = [s for s in all_signals if s.get('points', 0) >= 75]
        total_available = len(all_signals)
        
        # Count alignment categories
        strong_agree = len([s for s in all_signals if s.get('alignment_score', 0) >= 90])
        agree = len([s for s in all_signals if 80 <= s.get('alignment_score', 0) < 90])
        neutral = len([s for s in all_signals if 70 <= s.get('alignment_score', 0) < 80])
        disagree = len([s for s in all_signals if 60 <= s.get('alignment_score', 0) < 70])
        strong_disagree = len([s for s in all_signals if s.get('alignment_score', 0) < 60])
        
        # Use actual data_date from the DataFrame (not run_date)
        data_date_str = data_date.strftime('%Y-%m-%d')
        weekday = data_date.strftime('%A')
        is_current = " (CURRENT)" if (datetime.now() - data_date).days <= 7 else ""
        
        return f"""
  <div class='uet-card' style='padding:12px 14px;'>
    <div class='uet-grid cols-5' style='margin-bottom:8px;'>
      <div class='uet-kpi-card'>
        <div class='uet-kpi-label'>Run Date</div>
        <div class='uet-kpi-value'>{run_date.strftime('%Y-%m-%d')}</div>
      </div>
      <div class='uet-kpi-card'>
        <div class='uet-kpi-label'>Data Date</div>
        <div class='uet-kpi-value'>{data_date_str}</div>
        <div class='uet-kpi-subvalue'>{weekday}{is_current}</div>
      </div>
      <div class='uet-kpi-card'>
        <div class='uet-kpi-label'>High-Conviction (≥75)</div>
        <div class='uet-kpi-value'>{len(high_conviction)}</div>
        <div class='uet-kpi-subvalue'>of {total_available} total</div>
      </div>
      <div class='uet-kpi-card'>
        <div class='uet-kpi-label'>Alignment</div>
        <div class='uet-kpi-value'>{strong_agree + agree}</div>
        <div class='uet-kpi-subvalue'>{strong_agree}🔥 {agree}⭐</div>
      </div>
      <div class='uet-kpi-card'>
        <div class='uet-kpi-label'>Low Confidence</div>
        <div class='uet-kpi-value'>{strong_disagree + disagree}</div>
        <div class='uet-kpi-subvalue'>{disagree}⚠️ {strong_disagree}💥</div>
      </div>
    </div>
"""
    
    def _generate_at_a_glance_card(self, trend_signals: Dict, mean_reversion_signals: Dict) -> str:
        """Generate At a Glance card."""
        trend_buy = len([s for s in trend_signals.get('buy_signals', []) if s.get('points', 0) >= 75])
        trend_sell = len([s for s in trend_signals.get('sell_signals', []) if s.get('points', 0) >= 75])
        mr_buy = len([s for s in mean_reversion_signals.get('buy_signals', []) if s.get('points', 0) >= 75])
        mr_sell = len([s for s in mean_reversion_signals.get('sell_signals', []) if s.get('points', 0) >= 75])
        
        return f"""
    <div style='display:flex; flex-direction:column; gap:6px;'>
      <div class='uet-strategy-mini'>
        <div class='uet-strategy-mini-name'>📈 MACD Trend Following</div>
        <div class='uet-strategy-mini-stats'>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Buy</div><div class='uet-strategy-mini-stat-value'>{trend_buy}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Sell</div><div class='uet-strategy-mini-stat-value'>{trend_sell}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Total</div><div class='uet-strategy-mini-stat-value'>{trend_buy + trend_sell}</div></div>
        </div>
      </div>
      <div class='uet-strategy-mini'>
        <div class='uet-strategy-mini-name'>🔄 Mean Reversion</div>
        <div class='uet-strategy-mini-stats'>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Buy</div><div class='uet-strategy-mini-stat-value'>{mr_buy}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Sell</div><div class='uet-strategy-mini-stat-value'>{mr_sell}</div></div>
          <div class='uet-strategy-mini-stat'><div class='uet-strategy-mini-stat-label'>Total</div><div class='uet-strategy-mini-stat-value'>{mr_buy + mr_sell}</div></div>
        </div>
      </div>
    </div>
  </div>
"""
    
    def _generate_alignment_legend(self) -> str:
        """Generate Signal Alignment Legend - compact version."""
        return """
  <div style='padding:8px 10px; background:#f8fafc; border-radius:6px; border:1px solid #e2e8f0;'>
    <div class='uet-alignment-compact'>
      <div class='uet-alignment-badge'><span>🔥</span> <span>Strong Agree (90+)</span></div>
      <div class='uet-alignment-badge'><span>⭐</span> <span>Agree (80-89)</span></div>
      <div class='uet-alignment-badge'><span>⚡</span> <span>Neutral (70-79)</span></div>
      <div class='uet-alignment-badge'><span>⚠️</span> <span>Disagree (60-69)</span></div>
      <div class='uet-alignment-badge'><span>💥</span> <span>Strong Disagree (<60)</span></div>
    </div>
  </div>
"""
    
    def _calculate_strategy_stats(self, signals: Dict) -> Dict:
        """
        Calculate statistics for a strategy's signals.
        
        Args:
            signals: Dictionary with 'buy_signals' and 'sell_signals' lists
        
        Returns:
            Dictionary with stats including:
            - top_products_buy: List of (product, count) tuples for buy signals
            - top_products_sell: List of (product, count) tuples for sell signals
            - avg_points: Average points across all signals
            - point_distribution: Dict with counts for different point ranges
            - high_conviction_count: Number of signals >= 75 points
            - product_type_counts: Dict of product code -> count
            - spread_vs_outright: Dict with 'spreads' and 'outrights' counts
        """
        buy_signals = signals.get('buy_signals', [])
        sell_signals = signals.get('sell_signals', [])
        all_signals = buy_signals + sell_signals
        
        if not all_signals:
            return {
                'top_products_buy': [],
                'top_products_sell': [],
                'avg_points': 0,
                'point_distribution': {},
                'high_conviction_count': 0,
                'product_type_counts': {},
                'spread_vs_outright': {'spreads': 0, 'outrights': 0}
            }
        
        # Extract product codes from symbols or metadata
        def extract_product_code(signal):
            """Extract product code from signal metadata, row_data, symbol_matrix, or symbol string."""
            # First try row_data metadata
            row_data = signal.get('row_data', {})
            if row_data:
                # Try product field first
                product = row_data.get('product', '')
                if product and product not in ['n/a', '']:
                    return str(product).upper().strip()
                # Try symbol_root (e.g., 'PRL', 'CL', 'AFE')
                symbol_root = row_data.get('symbol_root', '')
                if symbol_root and symbol_root not in ['n/a', '']:
                    # Remove % if present
                    symbol_root = str(symbol_root).lstrip('%').upper().strip()
                    if symbol_root:
                        return symbol_root
            
            # Try symbol_matrix lookup
            symbol = signal.get('symbol', '') or signal.get('ice_connect_symbol', '')
            if symbol and self.symbol_matrix is not None and len(self.symbol_matrix) > 0:
                try:
                    # Look up symbol in symbol_matrix
                    matches = self.symbol_matrix[self.symbol_matrix['ice_symbol'] == symbol]
                    if len(matches) > 0:
                        # Get first match
                        match = matches.iloc[0]
                        # Try product first
                        product = match.get('product', '')
                        if product and product not in ['n/a', '']:
                            return str(product).upper().strip()
                        # Try symbol_root
                        symbol_root = match.get('symbol_root', '')
                        if symbol_root and symbol_root not in ['n/a', '']:
                            symbol_root = str(symbol_root).lstrip('%').upper().strip()
                            if symbol_root:
                                return symbol_root
                except Exception as e:
                    logger.debug(f"Error looking up symbol {symbol} in symbol_matrix: {e}")
            
            # Fall back to extracting from symbol string
            if symbol:
                # Pattern: %PRODUCT_CODE ... (e.g., %PRL, %CL, %AFE)
                match = re.match(r'%([A-Z]+)', str(symbol))
                if match:
                    return match.group(1)
            
            return None
        
        # Count products for buy and sell
        buy_products = []
        sell_products = []
        all_products = []
        points_list = []
        spread_count = 0
        outright_count = 0
        
        # Track symbols for buy/sell distribution
        buy_symbols = []
        sell_symbols = []
        
        for signal in buy_signals:
            product = extract_product_code(signal)
            if product:
                buy_products.append(product)
                all_products.append(product)
            
            # Track symbol for buy/sell distribution (use formal name)
            display_symbol = self._extract_display_symbol_for_stats(signal)
            if display_symbol:
                buy_symbols.append(display_symbol)
            
            points = signal.get('points', 0)
            if points:
                points_list.append(points)
            
            # Check if spread (has symbol_1 and symbol_2 in row_data)
            row_data = signal.get('row_data', {})
            if row_data.get('symbol_1') and row_data.get('symbol_2'):
                spread_count += 1
            else:
                outright_count += 1
        
        for signal in sell_signals:
            product = extract_product_code(signal)
            if product:
                sell_products.append(product)
                all_products.append(product)
            
            # Track symbol for buy/sell distribution (use formal name)
            display_symbol = self._extract_display_symbol_for_stats(signal)
            if display_symbol:
                sell_symbols.append(display_symbol)
            
            points = signal.get('points', 0)
            if points:
                points_list.append(points)
            
            # Check if spread (has symbol_1 and symbol_2 in row_data)
            row_data = signal.get('row_data', {})
            if row_data.get('symbol_1') and row_data.get('symbol_2'):
                spread_count += 1
            else:
                outright_count += 1
        
        # Calculate top products (top 5)
        buy_product_counts = Counter(buy_products)
        sell_product_counts = Counter(sell_products)
        all_product_counts = Counter(all_products)
        
        top_products_buy = buy_product_counts.most_common(5)
        top_products_sell = sell_product_counts.most_common(5)
        
        # Calculate point statistics
        avg_points = sum(points_list) / len(points_list) if points_list else 0
        high_conviction_count = len([p for p in points_list if p >= 75])
        
        # Point distribution
        point_distribution = {
            '75-84': len([p for p in points_list if 75 <= p < 85]),
            '85-94': len([p for p in points_list if 85 <= p < 95]),
            '95+': len([p for p in points_list if p >= 95])
        }
        
        # Calculate symbol-level buy/sell distribution (top 5 symbols by total signal count)
        buy_symbol_counts = Counter(buy_symbols)
        sell_symbol_counts = Counter(sell_symbols)
        all_symbol_counts = buy_symbol_counts + sell_symbol_counts
        top_symbols = all_symbol_counts.most_common(5)
        
        # For each top symbol, get buy and sell counts
        symbol_buy_sell = []
        for symbol, total_count in top_symbols:
            buy_count = buy_symbol_counts.get(symbol, 0)
            sell_count = sell_symbol_counts.get(symbol, 0)
            symbol_buy_sell.append({
                'symbol': symbol,
                'buy': buy_count,
                'sell': sell_count,
                'total': total_count
            })
        
        return {
            'top_products_buy': top_products_buy,
            'top_products_sell': top_products_sell,
            'avg_points': avg_points,
            'point_distribution': point_distribution,
            'high_conviction_count': high_conviction_count,
            'product_type_counts': dict(all_product_counts),
            'spread_vs_outright': {'spreads': spread_count, 'outrights': outright_count},
            'buy_count': len(buy_signals),
            'sell_count': len(sell_signals),
            'symbol_buy_sell': symbol_buy_sell
        }
    
    def _generate_strategy_stats_html(self, stats: Dict, strategy_key: str = None) -> str:
        """Generate HTML for strategy statistics dashboard."""
        if not stats or stats['high_conviction_count'] == 0:
            return """
        <div style='padding:12px; background:#f8fafc; border-radius:6px; border:1px solid #e2e8f0;'>
          <div style='color:#64748b; font-size:13px;'>No signals to display stats</div>
        </div>
"""
        
        # Calculate values for tiles
        buy_count = stats.get('buy_count', 0)
        sell_count = stats.get('sell_count', 0)
        total_signals = buy_count + sell_count
        buy_pct = (buy_count / total_signals * 100) if total_signals > 0 else 0
        sell_pct = (sell_count / total_signals * 100) if total_signals > 0 else 0
        
        dist_75_84 = stats['point_distribution'].get('75-84', 0)
        dist_85_94 = stats['point_distribution'].get('85-94', 0)
        dist_95_plus = stats['point_distribution'].get('95+', 0)
        max_dist = max(dist_75_84, dist_85_94, dist_95_plus, 1)  # Avoid division by zero
        
        symbol_buy_sell = stats.get('symbol_buy_sell', [])
        
        html = """
        <div style='padding:12px; background:#f8fafc; border-radius:6px; border:1px solid #e2e8f0;'>
          <h4 style='margin:0 0 12px 0; font-size:14px; color:#1e293b;'>Strategy Statistics</h4>
          <div class='uet-grid cols-5' style='gap:8px;'>
"""
        
        # Tile 1: Buy/Sell Distribution
        html += f"""
            <div style='padding:8px; background:white; border-radius:6px; border:1px solid #e2e8f0;'>
              <div style='font-size:11px; font-weight:600; color:#475569; margin-bottom:6px;'>Buy/Sell Distribution</div>
              <div style='display:flex; flex-direction:column; gap:4px;'>
                <div>
                  <div style='display:flex; justify-content:space-between; font-size:9px; color:#334155; margin-bottom:2px;'>
                    <span>Buy: {buy_count}</span>
                    <span>{buy_pct:.1f}%</span>
                  </div>
                  <div style='height:14px; background:#e5e7eb; border-radius:4px; overflow:hidden;'>
                    <div style='height:100%; width:{buy_pct}%; background:#059669; transition:width 0.3s;'></div>
                  </div>
                </div>
                <div>
                  <div style='display:flex; justify-content:space-between; font-size:9px; color:#334155; margin-bottom:2px;'>
                    <span>Sell: {sell_count}</span>
                    <span>{sell_pct:.1f}%</span>
                  </div>
                  <div style='height:14px; background:#e5e7eb; border-radius:4px; overflow:hidden;'>
                    <div style='height:100%; width:{sell_pct}%; background:#dc2626; transition:width 0.3s;'></div>
                  </div>
                </div>
              </div>
            </div>
"""
        
        # Tile 2: Top Symbols
        if symbol_buy_sell:
            html += """
            <div style='padding:8px; background:white; border-radius:6px; border:1px solid #e2e8f0;'>
              <div style='font-size:11px; font-weight:600; color:#475569; margin-bottom:6px;'>Top Symbols</div>
"""
            for item in symbol_buy_sell[:4]:  # Top 4 symbols (reduced for narrower tile)
                symbol = item['symbol']
                buy = item['buy']
                sell = item['sell']
                total = item['total']
                
                # Truncate long symbols for display
                symbol_display = symbol[:18] + '...' if len(symbol) > 18 else symbol
                
                html += f"""
              <div style='margin-bottom:3px;'>
                <div style='font-size:8px; color:#64748b; margin-bottom:1px;'>{symbol_display} ({total})</div>
                <div style='display:flex; gap:2px; height:8px;'>
                  <div style='flex:{buy}; background:#059669; border-radius:2px; display:flex; align-items:center; justify-content:center;'>
                    <span style='font-size:6px; color:white; font-weight:600;'>{buy}</span>
                  </div>
                  <div style='flex:{sell}; background:#dc2626; border-radius:2px; display:flex; align-items:center; justify-content:center;'>
                    <span style='font-size:6px; color:white; font-weight:600;'>{sell}</span>
                  </div>
                </div>
              </div>
"""
            html += """
            </div>
"""
        else:
            html += """
            <div style='padding:8px; background:white; border-radius:6px; border:1px solid #e2e8f0;'>
              <div style='font-size:11px; font-weight:600; color:#475569; margin-bottom:6px;'>Top Symbols</div>
              <div style='font-size:9px; color:#64748b;'>No symbols to display</div>
            </div>
"""
        
        # Tile 3: Signal Quality
        html += f"""
            <div style='padding:8px; background:white; border-radius:6px; border:1px solid #e2e8f0;'>
              <div style='font-size:11px; font-weight:600; color:#475569; margin-bottom:6px;'>Signal Quality</div>
              <div style='display:flex; gap:8px; margin-bottom:6px; font-size:9px; color:#334155;'>
                <div>Avg: <strong>{stats['avg_points']:.1f}</strong></div>
                <div>≥75: <strong>{stats['high_conviction_count']}</strong></div>
              </div>
              <div>
                <div style='font-size:8px; color:#64748b; margin-bottom:2px;'>Point Distribution</div>
                <div style='display:flex; gap:2px; align-items:flex-end; height:40px;'>
                  <div style='flex:1; display:flex; flex-direction:column; align-items:center;'>
                    <div style='width:100%; background:#3b82f6; border-radius:3px 3px 0 0; height:{(dist_75_84 / max_dist * 30) + 10}px; display:flex; align-items:flex-end; justify-content:center; padding-bottom:1px;'>
                      <span style='font-size:7px; color:white; font-weight:600;'>{dist_75_84}</span>
                    </div>
                    <div style='font-size:7px; color:#64748b; margin-top:1px;'>75-84</div>
                  </div>
                  <div style='flex:1; display:flex; flex-direction:column; align-items:center;'>
                    <div style='width:100%; background:#8b5cf6; border-radius:3px 3px 0 0; height:{(dist_85_94 / max_dist * 30) + 10}px; display:flex; align-items:flex-end; justify-content:center; padding-bottom:1px;'>
                      <span style='font-size:7px; color:white; font-weight:600;'>{dist_85_94}</span>
                    </div>
                    <div style='font-size:7px; color:#64748b; margin-top:1px;'>85-94</div>
                  </div>
                  <div style='flex:1; display:flex; flex-direction:column; align-items:center;'>
                    <div style='width:100%; background:#10b981; border-radius:3px 3px 0 0; height:{(dist_95_plus / max_dist * 30) + 10}px; display:flex; align-items:flex-end; justify-content:center; padding-bottom:1px;'>
                      <span style='font-size:7px; color:white; font-weight:600;'>{dist_95_plus}</span>
                    </div>
                    <div style='font-size:7px; color:#64748b; margin-top:1px;'>95+</div>
                  </div>
                </div>
              </div>
            </div>
"""
        
        # Tile 4: Top Products + Product Types
        html += """
            <div style='padding:8px; background:white; border-radius:6px; border:1px solid #e2e8f0;'>
              <div style='font-size:11px; font-weight:600; color:#475569; margin-bottom:6px;'>Top Products</div>
"""
        # Top Products with bar charts
        if stats['top_products_buy'] or stats['top_products_sell']:
            # Calculate max count for scaling bars
            all_product_counts = []
            if stats['top_products_buy']:
                all_product_counts.extend([count for _, count in stats['top_products_buy'][:3]])
            if stats['top_products_sell']:
                all_product_counts.extend([count for _, count in stats['top_products_sell'][:3]])
            max_count = max(all_product_counts) if all_product_counts else 1
            
            # Buy products with bars
            if stats['top_products_buy']:
                html += """
              <div style='margin-bottom:6px;'>
                <div style='font-size:9px; color:#059669; font-weight:600; margin-bottom:3px;'>Buy</div>
"""
                for prod, count in stats['top_products_buy'][:3]:
                    bar_width = (count / max_count * 100) if max_count > 0 else 0
                    # Truncate long product names
                    prod_display = prod[:18] + '...' if len(prod) > 18 else prod
                    html += f"""
                <div style='margin-bottom:2px;'>
                  <div style='display:flex; align-items:center; gap:4px;'>
                    <div style='flex:0 0 60px; font-size:7px; color:#64748b;'>{prod_display}</div>
                    <div style='flex:1; height:10px; background:#e5e7eb; border-radius:2px; overflow:hidden;'>
                      <div style='height:100%; width:{bar_width}%; background:#059669; transition:width 0.3s;'></div>
                    </div>
                    <div style='flex:0 0 25px; font-size:7px; color:#334155; text-align:right; font-weight:600;'>{count}</div>
                  </div>
                </div>
"""
                html += """
              </div>
"""
            
            # Sell products with bars
            if stats['top_products_sell']:
                html += """
              <div style='margin-bottom:6px;'>
                <div style='font-size:9px; color:#dc2626; font-weight:600; margin-bottom:3px;'>Sell</div>
"""
                for prod, count in stats['top_products_sell'][:3]:
                    bar_width = (count / max_count * 100) if max_count > 0 else 0
                    # Truncate long product names
                    prod_display = prod[:18] + '...' if len(prod) > 18 else prod
                    html += f"""
                <div style='margin-bottom:2px;'>
                  <div style='display:flex; align-items:center; gap:4px;'>
                    <div style='flex:0 0 60px; font-size:7px; color:#64748b;'>{prod_display}</div>
                    <div style='flex:1; height:10px; background:#e5e7eb; border-radius:2px; overflow:hidden;'>
                      <div style='height:100%; width:{bar_width}%; background:#dc2626; transition:width 0.3s;'></div>
                    </div>
                    <div style='flex:0 0 25px; font-size:7px; color:#334155; text-align:right; font-weight:600;'>{count}</div>
                  </div>
                </div>
"""
                html += """
              </div>
"""
        else:
            html += """
              <div style='font-size:9px; color:#64748b; margin-bottom:6px;'>No product data available</div>
"""
        
        # Product Types & Spread/Outright
        if stats['product_type_counts']:
            top_products = sorted(stats['product_type_counts'].items(), key=lambda x: x[1], reverse=True)[:3]  # Limit to 3
            max_type_count = max([count for _, count in top_products]) if top_products else 1
            
            html += f"""
              <div style='font-size:11px; font-weight:600; color:#475569; margin-top:8px; margin-bottom:4px; border-top:1px solid #e2e8f0; padding-top:6px;'>Product Types</div>
"""
            for prod, count in top_products:
                bar_width = (count / max_type_count * 100) if max_type_count > 0 else 0
                # Truncate long product names
                prod_display = prod[:18] + '...' if len(prod) > 18 else prod
                html += f"""
              <div style='margin-bottom:2px;'>
                <div style='display:flex; align-items:center; gap:4px;'>
                  <div style='flex:0 0 60px; font-size:7px; color:#64748b;'>{prod_display}</div>
                  <div style='flex:1; height:10px; background:#e5e7eb; border-radius:2px; overflow:hidden;'>
                    <div style='height:100%; width:{bar_width}%; background:#3b82f6; transition:width 0.3s;'></div>
                  </div>
                  <div style='flex:0 0 25px; font-size:7px; color:#334155; text-align:right; font-weight:600;'>{count}</div>
                </div>
              </div>
"""
            html += f"""
              <div style='font-size:8px; color:#64748b; margin-top:4px; padding-top:4px; border-top:1px solid #f1f5f9;'>
                Spreads: <strong style='color:#334155;'>{stats['spread_vs_outright']['spreads']}</strong> | 
                Outrights: <strong style='color:#334155;'>{stats['spread_vs_outright']['outrights']}</strong>
              </div>
"""
        else:
            html += f"""
              <div style='font-size:11px; font-weight:600; color:#475569; margin-top:8px; margin-bottom:4px; border-top:1px solid #e2e8f0; padding-top:6px;'>Product Types</div>
              <div style='font-size:8px; color:#64748b;'>
                Spreads: <strong style='color:#334155;'>{stats['spread_vs_outright']['spreads']}</strong> | 
                Outrights: <strong style='color:#334155;'>{stats['spread_vs_outright']['outrights']}</strong>
              </div>
"""
        
        html += """
            </div>
"""
        
        # Tile 5: Backtesting Results
        html += self._generate_backtesting_tile(strategy_key)
        
        html += """
          </div>
        </div>
"""
        return html
    
    def _generate_backtesting_tile(self, strategy_key: str = None) -> str:
        """Generate HTML for backtesting results tile, filtered by strategy if provided."""
        from pathlib import Path
        import pandas as pd
        
        # Try to load backtest results
        output_dir = Path(__file__).parent.parent.parent / 'backtesting_outputs'
        backtest_file = output_dir / 'backtest_summary.csv'
        
        if not backtest_file.exists():
            return """
            <div style='padding:8px; background:white; border-radius:6px; border:1px solid #e2e8f0;'>
              <div style='font-size:11px; font-weight:600; color:#475569; margin-bottom:6px;'>Backtesting Results</div>
              <div style='font-size:9px; color:#64748b; font-style:italic;'>No results available. Run backtests to generate.</div>
            </div>
"""
        
        try:
            df = pd.read_csv(backtest_file)
            if len(df) == 0:
                return """
            <div style='padding:8px; background:white; border-radius:6px; border:1px solid #e2e8f0;'>
              <div style='font-size:11px; font-weight:600; color:#475569; margin-bottom:6px;'>Backtesting Results</div>
              <div style='font-size:9px; color:#64748b; font-style:italic;'>No results available.</div>
            </div>
"""
            
            # Format strategy names for display
            def format_strategy_name(name: str) -> str:
                name_map = {
                    'trend_following': 'Trend',
                    'enhanced_trend_following': 'Enhanced Trend',
                    'mean_reversion': 'Mean Rev',
                    'macd_rsi_exhaustion': 'MACD/RSI'
                }
                return name_map.get(name, name[:12])
            
            # Filter by strategy if provided
            if strategy_key and 'strategy_name' in df.columns:
                df = df[df['strategy_name'] == strategy_key]
                if len(df) == 0:
                    return """
            <div style='padding:8px; background:white; border-radius:6px; border:1px solid #e2e8f0;'>
              <div style='font-size:11px; font-weight:600; color:#475569; margin-bottom:6px;'>Backtesting Results</div>
              <div style='font-size:9px; color:#64748b; font-style:italic;'>No results available for this strategy.</div>
            </div>
"""
            
            html = """
            <div style='padding:8px; background:white; border-radius:6px; border:1px solid #e2e8f0;'>
              <div style='font-size:11px; font-weight:600; color:#475569; margin-bottom:6px;'>Backtesting Results</div>
"""
            
            # Show top 3 strategies by total return (or all if filtered to one strategy)
            df_sorted = df.sort_values('total_return_pct', ascending=False)
            
            for idx, row in df_sorted.head(3).iterrows():
                strategy_display = format_strategy_name(str(row['strategy_name']))
                return_pct = row['total_return_pct']
                win_rate = row['win_rate']
                trades = int(row['total_trades'])
                
                # Color code return
                return_color = '#059669' if return_pct >= 0 else '#dc2626'
                
                html += f"""
              <div style='margin-bottom:4px; padding-bottom:4px; border-bottom:1px solid #f1f5f9;'>
                <div style='display:flex; justify-content:space-between; align-items:center; margin-bottom:2px;'>
                  <div style='font-size:8px; color:#334155; font-weight:600;'>{strategy_display}</div>
                  <div style='font-size:8px; color:{return_color}; font-weight:600;'>{return_pct:+.1f}%</div>
                </div>
                <div style='font-size:7px; color:#64748b;'>WR: {win_rate:.0f}% | Trades: {trades}</div>
              </div>
"""
            
            # Show summary if more than 3 strategies (or if showing all strategies for a filtered view)
            if len(df_sorted) > 3 and not strategy_key:
                avg_return = df_sorted['total_return_pct'].mean()
                total_trades = int(df_sorted['total_trades'].sum())
                html += f"""
              <div style='margin-top:4px; padding-top:4px; border-top:1px solid #e2e8f0; font-size:7px; color:#64748b;'>
                Avg: {avg_return:+.1f}% | Total: {total_trades} trades
              </div>
"""
            
            html += """
            </div>
"""
            return html
            
        except Exception as e:
            logger.warning(f"Error loading backtest results: {e}")
            return """
            <div style='padding:8px; background:white; border-radius:6px; border:1px solid #e2e8f0;'>
              <div style='font-size:11px; font-weight:600; color:#475569; margin-bottom:6px;'>Backtesting Results</div>
              <div style='font-size:9px; color:#64748b; font-style:italic;'>Error loading results</div>
            </div>
"""
    
    def _generate_strategy_section(
        self,
        title: str,
        strategy_key: str,
        signals: Dict,
        ice_chat_formatter
    ) -> str:
        """Generate strategy section with signals."""
        buy_signals = signals.get('buy_signals', [])
        sell_signals = signals.get('sell_signals', [])
        total_signals = len(buy_signals) + len(sell_signals)
        
        # Get all signals (before filtering) for stats calculation
        all_buy_signals = signals.get('all_buy_signals', buy_signals)
        all_sell_signals = signals.get('all_sell_signals', sell_signals)
        
        strategy_config = self.config['strategies'].get(strategy_key, {})
        strategy_name = strategy_config.get('name', title)
        strategy_desc = strategy_config.get('description', '')
        
        # Calculate stats from ALL signals (before filtering)
        all_signals_for_stats = {
            'buy_signals': all_buy_signals,
            'sell_signals': all_sell_signals
        }
        stats = self._calculate_strategy_stats(all_signals_for_stats)
        
        html = f"""
  <div class='uet-card'>
    <h2>{title} ({total_signals} signals)</h2>
    
        <div style='display:grid; grid-template-columns: 1fr 1fr; gap:16px; margin-bottom:16px;'>
            <div class='uet-explanation-card'>
                <h4>{strategy_name}</h4>
                <p><strong>Purpose:</strong> {strategy_desc}</p>
                <p><strong>Key Requirements:</strong></p>
                <ul>
                    <li>Entry Point: {self._get_entry_description(strategy_key)}</li>
                    <li>Confluence Indicators: {self._get_confluence_list(strategy_key)}</li>
                    <li>Min Points >= {self.config.get('min_points_threshold', 'N/A')}</li>
                    <li>Max Signals < {self.config.get('max_signals_per_type', 'N/A')} for each signal type</li>
                </ul>
            </div>
            {self._generate_strategy_stats_html(stats, strategy_key)}
        </div>
        
    <div class='uet-grid cols-2'>
      <div>
        <h3><span class='uet-banner buy'>BUY</span></h3>
"""
        
        if len(buy_signals) == 0:
            html += '        <div class=\'uet-note\'>No qualifying signals</div>'
        else:
            html += self._generate_signals_table(buy_signals, ice_chat_formatter, 'buy')
        
        html += """
      </div>
      <div>
        <h3><span class='uet-banner sell'>SELL</span></h3>
"""
        
        if len(sell_signals) == 0:
            html += '        <div class=\'uet-note\'>No qualifying signals</div>'
        else:
            html += self._generate_signals_table(sell_signals, ice_chat_formatter, 'sell')
        
        html += """
      </div>
    </div>
  </div>
"""
        return html
    
    def _get_entry_description(self, strategy_key: str) -> str:
        """Get entry point description for strategy."""
        if strategy_key == 'trend_following':
            return "MACD Cross over buy/sell as the entry point"
        elif strategy_key == 'enhanced_trend_following':
            return "EMA crossover, Supertrend, MACD cross, or Aroon strong trend with ADX confirmation"
        elif strategy_key == 'mean_reversion':
            return "Price < 25 Percentile (buy) or > 75 Percentile (sell)"
        elif strategy_key == 'macd_rsi_exhaustion':
            return "MACD or RSI exhausted to extremes (MACD < 20th percentile + zero line/crossover for buy, RSI < 20th percentile OR < 30 + momentum up for buy, vice versa for sell)"
        return ""
    
    def _get_confluence_list(self, strategy_key: str) -> str:
        """Get confluence indicators list for strategy."""
        strategy_config = self.config['strategies'].get(strategy_key, {})
        confluence = strategy_config.get('confluence_bonuses', {})
        names = []
        for key in confluence.keys():
            if 'rsi' in key:
                names.append('RSI')
            elif 'stochastic' in key:
                names.append('Stoch')
            elif 'cci' in key:
                names.append('CCI')
            elif 'adx' in key:
                names.append('ADX')
            elif 'bollinger' in key:
                names.append('Bollinger Bands')
            elif 'correlation' in key:
                names.append('Correlation')
            elif 'cointegration' in key:
                names.append('Cointegration')
            elif 'macd' in key:
                names.append('MACD')
        return ', '.join(names) if names else 'Various indicators'
    
    def _generate_signals_table(
        self,
        signals: List[Dict],
        ice_chat_formatter,
        signal_type: str
    ) -> str:
        """Generate signals table with embedded ICE Chat rows."""
        # Color header based on signal type (green for buy, red for sell)
        header_bg = '#10b981' if signal_type.lower() == 'buy' else '#ef4444'  # Green for buy, red for sell
        header_text_color = '#ffffff'  # White text for contrast
        
        html = '<table class="uet-table">'
        html += f'<thead><tr style="background-color: {header_bg}; color: {header_text_color};">'
        html += '<th>ICE Symbol</th><th>Strategy_Type</th><th>Signal</th>'
        html += '<th>Price</th><th>Stop</th><th>Target</th><th>Pos %</th><th>Score</th>'
        if self.ai_align_enabled:
            html += '<th>AI Align</th><th>AI Conf</th>'
        html += '<th>PRWK</th><th>Entry Date</th>'
        html += '</tr></thead><tbody>'
        
        # Get data_date from ice_chat_formatter if available (for AI alignment)
        data_date = None
        if hasattr(ice_chat_formatter, 'data_date'):
            data_date = ice_chat_formatter.data_date
        
        for signal in signals:
            html += self._generate_signal_row_with_ice_chat(signal, ice_chat_formatter, signal_type, data_date)
        
        html += '</tbody></table>'
        return html
    
    def _format_price_value(self, value: float, signal_type: str, field_type: str, is_spread: bool = False) -> str:
        """
        Format price/stop/target value with Pay/Rcv prefix.
        
        Args:
            value: Numeric value (price, stop, or target)
            signal_type: 'buy' or 'sell'
            field_type: 'price', 'stop', or 'target'
            is_spread: Whether this is a spread (True) or outright (False)
        
        Returns:
            Formatted string like "Pay $0.0135" or "Rcv $0.0849"
        """
        import numpy as np
        
        if pd.isna(value) or value == 0:
            return "N/A"
        
        abs_value = abs(value)
        formatted_value = f"${abs_value:.4f}"
        
        if is_spread:
            # SPREAD LOGIC
            if signal_type == 'buy':
                # Buy spread = Buy leg 1, Sell leg 2
                if field_type == 'price':
                    # Entry price: positive = pay, negative = receive
                    prefix = "Pay" if value >= 0 else "Rcv"
                elif field_type == 'stop':
                    # Stop loss: For BUY spread, stop = Entry - ATR×0.56 (price goes DOWN/worse)
                    # You're SELLING to close (exit BUY position)
                    # Selling at negative price = Pay, Selling at positive price = Rcv
                    prefix = "Pay" if value < 0 else "Rcv"
                else:  # target
                    # Target profit: For BUY spread, target = Entry + ATR×0.83 (price goes UP/better)
                    # You're SELLING to close (exit BUY position)
                    # Selling at positive price = Rcv, Selling at negative price = Pay
                    prefix = "Rcv" if value >= 0 else "Pay"
            else:  # sell
                # Sell spread = Sell leg 1, Buy leg 2
                if field_type == 'price':
                    # Entry price: negative = pay (buying more expensive leg), positive = receive (selling more expensive leg)
                    prefix = "Pay" if value < 0 else "Rcv"
                elif field_type == 'stop':
                    # Stop loss: For SELL spread, stop = Entry + ATR×0.56 (price goes UP/worse)
                    # You're BUYING to close (exit SELL position)
                    # Buying at positive price = Pay, Buying at negative price = Rcv
                    prefix = "Pay" if value >= 0 else "Rcv"
                else:  # target
                    # Target profit: For SELL spread, target = Entry - ATR×0.83 (price goes DOWN/better)
                    # You're BUYING to close (exit SELL position)
                    # Buying at negative price = Rcv, Buying at positive price = Pay
                    prefix = "Rcv" if value < 0 else "Pay"
        else:
            # OUTRIGHT LOGIC (simpler)
            if signal_type == 'buy':
                # Buy outright
                if field_type == 'price':
                    prefix = "Pay"  # Always pay to buy
                elif field_type == 'stop':
                    prefix = "Pay"  # Stop loss = pay more
                else:  # target
                    prefix = "Rcv"  # Target profit = receive
            else:  # sell
                # Sell outright
                if field_type == 'price':
                    prefix = "Rcv"  # Always receive when selling
                elif field_type == 'stop':
                    prefix = "Pay"  # Stop loss = pay more
                else:  # target
                    prefix = "Pay"  # Target profit = pay less (or receive more, but we show as pay)
        
        return f"{prefix} {formatted_value}"
    
    def _generate_signal_row_with_ice_chat(self, signal: Dict, ice_chat_formatter, signal_type: str, data_date: Optional[datetime] = None) -> str:
        """Generate table row for a signal with embedded ICE Chat rows."""
        entry_date = signal.get('entry_date', '')
        if pd.notna(entry_date) and entry_date:
            if isinstance(entry_date, str):
                date_str = entry_date[:10] if len(entry_date) >= 10 else entry_date
            else:
                date_str = entry_date.strftime('%Y-%m-%d')
        else:
            date_str = 'N/A'
        
        price = signal.get('entry_price', 0)
        stop = signal.get('stop', 0)
        target = signal.get('target', 0)
        pos_pct = signal.get('pos_pct', 0)
        points = signal.get('points', 0)
        alignment = signal.get('alignment_score', 0)
        is_fallback = signal.get('is_fallback', False)
        
        # Determine if this is a spread
        row_data = signal.get('row_data', {})
        is_outright = row_data.get('is_outright', True)
        is_spread = not is_outright if isinstance(is_outright, bool) else False
        
        # Format price, stop, target with Pay/Rcv
        price_str = self._format_price_value(price, signal_type, 'price', is_spread)
        stop_str = self._format_price_value(stop, signal_type, 'stop', is_spread)
        target_str = self._format_price_value(target, signal_type, 'target', is_spread)
        
        # Get strategy type name from row_data or infer from signal context
        strategy_name = row_data.get('strategy_name', '')
        if strategy_name == 'trend_following':
            strategy_type = "Trend"
        elif strategy_name == 'enhanced_trend_following':
            strategy_type = "Enhanced Trend"
        elif strategy_name == 'mean_reversion':
            strategy_type = "Mean Reversion"
        elif strategy_name == 'macd_rsi_exhaustion':
            strategy_type = "MACD+RSI Exhaustion"
        else:
            # Fallback: try to infer from signal characteristics
            strategy_type = "Unknown"
        
        # Get AI alignment if enabled
        ai_align_label = ""
        ai_align_confidence = ""
        ai_response = None  # Store full response for summary row
        if self.ai_align_enabled:
            try:
                # Build trade payload
                trade_payload = build_trade_payload(signal, ice_chat_formatter, data_date)
                
                # Build trade signature for cache
                trade_signature = {
                    "week_date": trade_payload.get("week_date", date_str),
                    "structure_type": trade_payload.get("structure_type", "outright"),
                    "symbol": signal.get("symbol", ""),
                    "signal_direction": trade_payload.get("signal_direction", signal_type.title()),
                    "strategy_type": strategy_type
                }
                
                # Get AI alignment (with caching)
                cache_date = date.today()
                if data_date:
                    if isinstance(data_date, datetime):
                        cache_date = data_date.date()
                    elif isinstance(data_date, date):
                        cache_date = data_date
                
                ai_response = get_or_fetch_ai_alignment(
                    trade_signature,
                    trade_payload,
                    cache_date=cache_date,
                    multi_pass=self.ai_align_config.get('multi_pass', True),
                    num_passes=self.ai_align_config.get('passes', 3),
                    model=self.ai_align_config.get('openai_model', 'gpt-4'),
                    temperature=self.ai_align_config.get('openai_temperature', 0.3),
                    max_tokens=self.ai_align_config.get('openai_max_tokens', 1000)
                )
                
                ai_align_label = ai_response.get("alignment_label", "AI Error")
                ai_align_confidence = ai_response.get("confidence", 0)
                
            except Exception as e:
                logger.error(f"Error getting AI alignment for signal {signal.get('symbol', '')}: {e}", exc_info=True)
                ai_align_label = "AI Error"
                ai_align_confidence = 0
                ai_response = None
        
        # Format symbol with badge if fallback
        symbol_display = _html.escape(str(signal.get("symbol", "")))
        if is_fallback:
            symbol_display += '<span class="uet-fallback-badge">Best Available Below</span>'
        
        # Format score with badge if fallback
        score_display = f'{points:.1f}'
        if is_fallback:
            score_display += '<span class="uet-fallback-badge">Best Available Below</span>'
        
        # Add fallback row class for styling
        row_class = ' class="uet-fallback-row"' if is_fallback else ''
        
        # Check prior week status
        was_active_prior_week = signal.get('was_active_prior_week', False)
        if was_active_prior_week:
            prior_week_display = '<span style="color: #10b981; font-weight: bold; font-size: 14px;">✓</span>'  # Green check
        else:
            prior_week_display = '<span style="color: #ef4444; font-weight: bold; font-size: 14px;">✗</span>'  # Red X
        
        # Initialize HTML string
        html = ''
        
        # Calculate colspan for detail rows (base 10, add 2 if AI enabled)
        colspan = 12 if self.ai_align_enabled else 10
        
        html += f'<tr{row_class}>'
        html += f'<td><strong>{_html.escape(str(signal.get("symbol", "")))}</strong></td>'  # ICE Symbol (bold)
        html += f'<td>{strategy_type}</td>'
        html += f'<td>{signal_type.title()}</td>'
        html += f'<td>{price_str}</td>'
        html += f'<td>{stop_str}</td>'
        html += f'<td>{target_str}</td>'
        html += f'<td class="uet-num">{pos_pct:.1f}%</td>'
        html += f'<td class="uet-num">{score_display}</td>'
        if self.ai_align_enabled:
            # Add visual status indicator based on alignment (not just API success)
            if ai_align_label and ai_align_label != "AI Error" and ai_response and "error" not in ai_response:
                # Show icon based on alignment label
                if ai_align_label in ["Strongly Agree", "Agree"]:
                    status_icon = '<span style="color: #10b981; font-weight: bold;">✓</span> '  # Green check for agree
                elif ai_align_label == "Neutral":
                    status_icon = '<span style="color: #f59e0b; font-weight: bold;">⚠</span> '  # Yellow warning for neutral
                elif ai_align_label in ["Disagree", "Strongly Disagree"]:
                    status_icon = '<span style="color: #dc2626; font-weight: bold;">✗</span> '  # Red X for disagree
                else:
                    status_icon = '<span style="color: #6b7280; font-weight: bold;">?</span> '  # Gray question for unknown
            else:
                status_icon = '<span style="color: #dc2626; font-weight: bold;">✗</span> '  # Red X for error
            html += f'<td class="uet-center">{status_icon}{_html.escape(str(ai_align_label))}</td>'
            html += f'<td class="uet-num">{ai_align_confidence}</td>'
        html += f'<td class="uet-center">{prior_week_display}</td>'
        html += f'<td>{date_str}</td>'
        html += '</tr>'
        
        # Add score details row - light blue background
        score_breakdown = ice_chat_formatter.format_score_breakdown(signal)
        risk_details = ice_chat_formatter.format_risk_details(signal)
        html += f'<tr class="uet-scoredetails"><td colspan="{colspan}"><div class="icechat-line">Score details: {score_breakdown} | {risk_details}</div></td></tr>'
        
        # Add ICE Chat row - light yellow background
        ice_chat_msg = ice_chat_formatter.format_ice_chat_message(signal)
        html += f'<tr class="uet-icechat"><td colspan="{colspan}"><div class="icechat-line">{_html.escape(ice_chat_msg)}</div></td></tr>'
        
        # Add AI Analysis summary row - light green background (only if AI succeeded)
        if self.ai_align_enabled and ai_response and ai_align_label != "AI Error" and "error" not in ai_response:
            technical_view = ai_response.get("technical_view", "")
            fundamental_view = ai_response.get("fundamental_view", "")
            overall_comment = ai_response.get("overall_comment", "")
            
            # Build compact one-line bullet points
            ai_summary_html = '<div class="icechat-line" style="font-size: 12px; line-height: 1.4;">'
            bullet_points = []
            if technical_view:
                bullet_points.append(f'<strong>• Technical:</strong> {_html.escape(technical_view.strip())}')
            if fundamental_view:
                bullet_points.append(f'<strong>• Fundamental:</strong> {_html.escape(fundamental_view.strip())}')
            if overall_comment:
                bullet_points.append(f'<strong>• Overall:</strong> {_html.escape(overall_comment.strip())}')
            
            # Join with separator (pipe) for compact display
            ai_summary_html += ' | '.join(bullet_points)
            ai_summary_html += '</div>'
            
            html += f'<tr class="uet-ai-analysis"><td colspan="{colspan}">{ai_summary_html}</td></tr>'
        elif self.ai_align_enabled and (ai_align_label == "AI Error" or not ai_response or "error" in (ai_response or {})):
            # Show error message if AI failed
            error_msg = "AI Analysis unavailable"
            if ai_response and "error" in ai_response:
                error_msg += f": {ai_response.get('error', 'Unknown error')}"
            html += f'<tr class="uet-ai-analysis"><td colspan="{colspan}"><div class="icechat-line" style="color: #dc2626;"><em>{error_msg}</em></div></td></tr>'
        
        return html
    
    def _get_alignment_icon(self, alignment_score: float) -> str:
        """Get alignment icon based on score."""
        if alignment_score >= 90:
            return "🔥"
        elif alignment_score >= 80:
            return "⭐"
        elif alignment_score >= 70:
            return "⚡"
        elif alignment_score >= 60:
            return "⚠️"
        else:
            return "💥"
    
    def _extract_referenced_symbols(self, trend_signals: Dict, enhanced_trend_signals: Dict = None, mean_reversion_signals: Dict = None, macd_rsi_exhaustion_signals: Dict = None, ice_chat_formatter = None) -> set:
        """
        Extract all symbols referenced in signals (for highlighting in curve table).
        
        Args:
            trend_signals: Standard trend following signals
            enhanced_trend_signals: Enhanced trend following signals
            mean_reversion_signals: Mean reversion signals
            ice_chat_formatter: ICEChatFormatter instance (for accessing symbol matrix)
        
        Returns:
            Set of symbol strings (e.g., {'%AFE F!-IEU', '%CL V!'})
        """
        if enhanced_trend_signals is None:
            enhanced_trend_signals = {'buy_signals': [], 'sell_signals': []}
        if mean_reversion_signals is None:
            mean_reversion_signals = {'buy_signals': [], 'sell_signals': []}
        if macd_rsi_exhaustion_signals is None:
            macd_rsi_exhaustion_signals = {'buy_signals': [], 'sell_signals': []}
        
        referenced = set()
        
        all_signals = [
            trend_signals.get('buy_signals', []),
            trend_signals.get('sell_signals', []),
            enhanced_trend_signals.get('buy_signals', []),
            enhanced_trend_signals.get('sell_signals', []),
            mean_reversion_signals.get('buy_signals', []),
            mean_reversion_signals.get('sell_signals', []),
            macd_rsi_exhaustion_signals.get('buy_signals', []),
            macd_rsi_exhaustion_signals.get('sell_signals', [])
        ]
        
        for signal_list in all_signals:
            for signal in signal_list:
                symbol = signal.get('symbol', '')
                if symbol:
                    referenced.add(symbol)
                
                # For spreads, extract symbol_1 and symbol_2 from metadata
                if ice_chat_formatter:
                    metadata = ice_chat_formatter._get_symbol_metadata(symbol)
                    if metadata:
                        # Check if it's a spread by looking for symbol_1 and symbol_2
                        symbol_1 = metadata.get('symbol_1', '')
                        symbol_2 = metadata.get('symbol_2', '')
                        if symbol_1 or symbol_2:  # Has symbol_1 or symbol_2 means it's a spread
                            if symbol_1:
                                referenced.add(symbol_1)
                            if symbol_2:
                                referenced.add(symbol_2)
        
        return referenced
    
    def _generate_forward_curve_section(
        self,
        trend_signals: Dict,
        enhanced_trend_signals: Dict,
        mean_reversion_signals: Dict,
        macd_rsi_exhaustion_signals: Dict = None,
        curve_data: Dict = None,
        ice_chat_formatter = None,
        data_date: datetime = None,
        run_date: datetime = None
    ) -> str:
        """
        Generate Forward Curve Summary section.
        
        Args:
            trend_signals: Trend following signals
            enhanced_trend_signals: Enhanced trend following signals
            mean_reversion_signals: Mean reversion signals
            curve_data: Curve price data from CurveBuilder
            ice_chat_formatter: ICEChatFormatter instance
            data_date: Actual date from the data (for determining which months to show)
        
        Returns:
            HTML string for forward curve section
        """
        if not curve_data:
            return """
        <div class="uet-card">
            <h2>📊 Forward Curve Summary</h2>
            <p><em>Curve data not available. Prices used for delta volume adjustments.</em></p>
        </div>
"""
        
        # Extract referenced symbols
        referenced_symbols = self._extract_referenced_symbols(
            trend_signals, enhanced_trend_signals, mean_reversion_signals, macd_rsi_exhaustion_signals, ice_chat_formatter
        )
        
        # Always use current date for Forward Curve Summary (shows current market prices)
        # This ensures we see current curve data regardless of historical analysis date
        from datetime import datetime
        reference_date = datetime.now()
        reference_year = reference_date.year
        reference_month = reference_date.month
        
        months_to_show = []
        month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        # Start from current month, show next 12 months (current curve data)
        for i in range(12):
            month_idx = (reference_month - 1 + i) % 12
            year = reference_year + ((reference_month - 1 + i) // 12)
            year_short = str(year)[-2:]
            months_to_show.append(f"{month_names[month_idx]}_{year_short}")
        
        # Format dates for display
        if run_date is None:
            from datetime import datetime
            run_date = datetime.now()
        if data_date is None:
            data_date = run_date
        
        run_date_str = run_date.strftime('%Y-%m-%d %H:%M:%S')
        data_date_str = data_date.strftime('%Y-%m-%d')
        
        # Build table - compact version
        html = f"""
        <div class="uet-card" style="padding:10px 12px;">
            <h3 style="font-size:13px; margin:0 0 6px 0; font-weight:600;">📊 Forward Curve Summary</h3>
            <p style="font-size:9px; color:var(--muted); margin:0 0 8px 0;">Prices in $/usg. Yellow = referenced in signals. Generated: {run_date_str} | Data Date: {data_date_str}</p>
            <div style="overflow-x: auto; margin: 8px 0;">
                <table class="uet-table" style="font-size: 0.65em; table-layout:auto;">
                    <thead>
                        <tr>
                            <th style="font-size:9px; padding:4px 6px;">Commodity</th>
"""
        
        # Add month headers
        for month_col in months_to_show:
            html += f'<th style="font-size:9px; padding:4px 6px; text-align:center;">{month_col.replace("_", " ")}</th>'
        
        html += """
                        </tr>
                    </thead>
                    <tbody>
"""
        
        # Commodity root to display name mapping
        commodity_names = {
            'PRL': 'Propane (MB LST)',
            'PRN': 'Propane (MB Non-TET)',
            'PRC': 'Propane (Conway)',
            'AFE': 'AFE Propane (FEI)',
            'NBI': 'Normal Butane (MB Non-TET)',
            'NBR': 'LST Normal Butane (MB LST)',
            'IBC': 'Normal Butane (Conway)',
            'ABF': 'Far East Butane',
            'ISO': 'Isobutane (MB Non-TET)',
            'ISC': 'Isobutane (Conway)',
            'NGE': 'Natural Gasoline (MB Non-TET)',
            'NGC': 'Natural Gasoline (Conway)',
            'CL': 'WTI Crude Oil',
            'NG': 'Natural Gas (HH)',
            'XRB': 'RBOB Gasoline',
            'HO': 'Heating Oil'
        }
        
        # Load symbol matrix to get conversion factors
        conversion_factors = {}
        if ice_chat_formatter and hasattr(ice_chat_formatter, 'symbol_matrix') and ice_chat_formatter.symbol_matrix is not None:
            # Get conversion factors from symbol matrix
            for _, row in ice_chat_formatter.symbol_matrix.iterrows():
                root = row.get('symbol_root', '').upper()
                convert_to_usg = row.get('convert_to_$usg', 'n/a')
                if root and convert_to_usg and convert_to_usg != 'n/a' and convert_to_usg != '':
                    # Parse conversion factor (e.g., "/521" or "/42" or already a float like 521.0)
                    try:
                        if isinstance(convert_to_usg, str) and convert_to_usg.startswith('/'):
                            divisor = float(convert_to_usg[1:])
                            conversion_factors[root] = divisor
                        elif isinstance(convert_to_usg, (int, float)) and convert_to_usg > 0:
                            # Already a numeric divisor
                            conversion_factors[root] = float(convert_to_usg)
                    except (ValueError, TypeError):
                        pass
                # If no conversion factor or already in $/usg, use 1.0 (no conversion)
                if root and root not in conversion_factors:
                    conversion_factors[root] = 1.0
        
        # Default conversion factors if symbol matrix not available
        # Based on common conversions: AFE = /521 ($/mt to $/usg), ABF = /453 ($/mt to $/usg), CL = /42 ($/bbl to $/usg)
        default_conversions = {
            'AFE': 521.0,  # $/mt to $/usg
            'ABF': 453.0,  # $/mt to $/usg (Far East Butane uses different conversion than AFE Propane)
            'CL': 42.0,    # $/bbl to $/usg
        }
        for root, divisor in default_conversions.items():
            if root not in conversion_factors:
                conversion_factors[root] = divisor
        
        # Sort commodities for display
        sorted_roots = sorted(curve_data.keys())
        
        for root in sorted_roots:
            commodity_name = commodity_names.get(root, root)
            prices = curve_data.get(root, {})
            
            # Get conversion factor for this root
            conversion_divisor = conversion_factors.get(root, 1.0)
            
            html += f'<tr><td style="font-weight: bold; font-size:9px; padding:3px 6px;">{commodity_name}</td>'
            
            for month_col in months_to_show:
                price = prices.get(month_col)
                
                # Check if this symbol is referenced
                # Build symbol string to check (e.g., '%AFE F!-IEU' for Jan_26)
                month_name = month_col.split('_')[0]
                month_code_map = {
                    'Jan': 'F', 'Feb': 'G', 'Mar': 'H', 'Apr': 'J',
                    'May': 'K', 'Jun': 'M', 'Jul': 'N', 'Aug': 'Q',
                    'Sep': 'U', 'Oct': 'V', 'Nov': 'X', 'Dec': 'Z'
                }
                month_code = month_code_map.get(month_name, '')
                
                if month_code:
                    # Check if this symbol is referenced (simplified - could be enhanced)
                    is_referenced = False
                    symbol_to_check = f'%{root} {month_code}!'
                    for ref_symbol in referenced_symbols:
                        if symbol_to_check in ref_symbol or ref_symbol.startswith(f'%{root}'):
                            # Check if month matches (simplified check)
                            if month_code in ref_symbol:
                                is_referenced = True
                                break
                
                cell_style = 'background-color: #fff9c4;' if is_referenced else ''
                
                if price is not None:
                    # Convert to $/usg
                    price_usg = price / conversion_divisor
                    # Format with $ prefix and 5 decimal places
                    html += f'<td style="{cell_style} font-size:9px; padding:3px 6px; text-align:right;">${price_usg:.5f}</td>'
                else:
                    html += f'<td style="{cell_style} font-size:9px; padding:3px 6px; text-align:center;">-</td>'
            
            html += '</tr>'
        
        html += """
                    </tbody>
                </table>
            </div>
        </div>
"""
        
        return html
    
    def _generate_ice_connect_section(
        self,
        trend_signals: Dict,
        enhanced_trend_signals: Dict = None,
        mean_reversion_signals: Dict = None,
        macd_rsi_exhaustion_signals: Dict = None,
        ice_chat_formatter = None
    ) -> str:
        """
        Generate ICE Connect Copy-Paste Summary section.
        
        Args:
            trend_signals: Standard trend following signals
            enhanced_trend_signals: Enhanced trend following signals
            mean_reversion_signals: Mean reversion signals
            ice_chat_formatter: ICEChatFormatter instance
        
        Returns:
            HTML string for ICE Connect section
        """
        if not ice_chat_formatter:
            return """
        <div class="uet-card">
            <h2>📋 ICE Connect Copy-Paste Summary</h2>
            <p><em>ICE Chat formatter not available.</em></p>
        </div>
"""
        
        # Collect high-conviction signals (from config - no hardcoded default)
        if 'min_points_threshold' not in self.config:
            logger.warning("Config missing 'min_points_threshold' - cannot filter signals")
            return []
        min_points = self.config['min_points_threshold']
        
        if enhanced_trend_signals is None:
            enhanced_trend_signals = {'buy_signals': [], 'sell_signals': []}
        
        if macd_rsi_exhaustion_signals is None:
            macd_rsi_exhaustion_signals = {'buy_signals': [], 'sell_signals': []}
        strategy_signals = {
            'MACD Trend Following System': trend_signals,
            'Enhanced Trend Following Signals': enhanced_trend_signals,
            'Standard Mean Reversion Signals': mean_reversion_signals,
            'MACD/RSI Exhaustion Signals': macd_rsi_exhaustion_signals
        }
        
        html = """
        <div class="uet-card">
            <h2>📋 ICE Connect Copy-Paste Summary</h2>
            <p>Copy the text below and paste directly into ICE Connect (high-conviction signals ≥75 points only):</p>
            <div style="background-color: #f5f5f5; padding: 15px; border-radius: 4px; white-space: pre-wrap; font-family: monospace; font-size: 0.8em; overflow-x: auto;">
"""
        
        for strategy_name, signals_dict in strategy_signals.items():
            buy_signals = [s for s in signals_dict.get('buy_signals', []) if s.get('points', 0) >= min_points]
            sell_signals = [s for s in signals_dict.get('sell_signals', []) if s.get('points', 0) >= min_points]
            
            if not buy_signals and not sell_signals:
                continue
            
            html += f'\n##"{strategy_name}"\n'
            
            html += '##BUY\n'
            if buy_signals:
                for signal in buy_signals:
                    # Use raw symbol/formula, not formatted ICE Chat message
                    raw_symbol = signal.get('ice_connect_symbol') or signal.get('symbol', '')
                    html += f'{raw_symbol}\n'
            else:
                html += '-\n'
            
            html += '##SELL\n'
            if sell_signals:
                for signal in sell_signals:
                    # Use raw symbol/formula, not formatted ICE Chat message
                    raw_symbol = signal.get('ice_connect_symbol') or signal.get('symbol', '')
                    html += f'{raw_symbol}\n'
            else:
                html += '-\n'
        
        html += """
            </div>
        </div>
"""
        
        return html
    
    def generate_ice_connect_text_file(
        self,
        trend_signals: Dict,
        enhanced_trend_signals: Dict = None,
        mean_reversion_signals: Dict = None,
        macd_rsi_exhaustion_signals: Dict = None,
        ice_chat_formatter = None,
        data_date: datetime = None
    ) -> Path:
        """
        Generate plain text file for ICE Connect copy-paste.
        
        Args:
            trend_signals: Standard trend following signals
            enhanced_trend_signals: Enhanced trend following signals (optional)
            mean_reversion_signals: Mean reversion signals
            macd_rsi_exhaustion_signals: MACD/RSI exhaustion signals (optional)
            ice_chat_formatter: ICEChatFormatter instance
            data_date: Date from the data (for filename)
        
        Returns:
            Path to the generated text file
        """
        if not ice_chat_formatter:
            logger.warning("ICE Chat formatter not available. Cannot generate text file.")
            return None
        
        if data_date is None:
            data_date = datetime.now()
        
        # Collect high-conviction signals (from config - no hardcoded default)
        if 'min_points_threshold' not in self.config:
            logger.warning("Config missing 'min_points_threshold' - cannot filter signals")
            return []
        min_points = self.config['min_points_threshold']
        
        # Order: Mean Reversion first, then others (matching user's preferred format)
        strategy_signals = {}
        if mean_reversion_signals:
            strategy_signals['Standard Mean Reversion Signals'] = mean_reversion_signals
        if enhanced_trend_signals:
            strategy_signals['Enhanced Trend-Following Signals'] = enhanced_trend_signals
        if macd_rsi_exhaustion_signals:
            strategy_signals['MACD/RSI Exhaustion Signals'] = macd_rsi_exhaustion_signals
        if trend_signals:
            strategy_signals['Standard Trend-Following Signals'] = trend_signals
        
        # Build text content
        text_lines = []
        text_lines.append("📋 ICE Connect Copy-Paste Summary")
        text_lines.append("")
        text_lines.append("Copy the text below and paste directly into ICE Connect (high-conviction signals ≥75 points only):")
        text_lines.append("")
        
        for strategy_name, signals_dict in strategy_signals.items():
            buy_signals = [s for s in signals_dict.get('buy_signals', []) if s.get('points', 0) >= min_points]
            sell_signals = [s for s in signals_dict.get('sell_signals', []) if s.get('points', 0) >= min_points]
            
            if not buy_signals and not sell_signals:
                continue
            
            text_lines.append(f'##"{strategy_name}"')
            text_lines.append("")
            text_lines.append("##BUY")
            
            if buy_signals:
                for signal in buy_signals:
                    # Use ICE Chat formatted message (not raw symbol)
                    ice_chat_msg = ice_chat_formatter.format_ice_chat_message(signal)
                    text_lines.append(ice_chat_msg)
            else:
                text_lines.append("")
            
            text_lines.append("")
            text_lines.append("##SELL")
            
            if sell_signals:
                for signal in sell_signals:
                    # Use ICE Chat formatted message (not raw symbol)
                    ice_chat_msg = ice_chat_formatter.format_ice_chat_message(signal)
                    text_lines.append(ice_chat_msg)
            else:
                text_lines.append("")
            
            text_lines.append("")
        
        # Join all lines
        text_content = "\n".join(text_lines)
        
        # Save to file
        output_dir = Path(__file__).parent.parent / 'output'
        output_dir.mkdir(parents=True, exist_ok=True)
        
        date_str = data_date.strftime('%Y-%m-%d')
        filename = f"ice_connect_signals_{date_str}.txt"
        output_path = output_dir / filename
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(text_content)
            logger.info(f"✓ ICE Connect text file saved: {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Error saving ICE Connect text file: {e}")
            return None
    
    def _generate_html_footer(self) -> str:
        """Generate HTML footer."""
        return """
</div>
</body>
</html>
"""
    
    def save_report(self, html: str, output_dir: Path = None, filename: str = None) -> Path:
        """
        Save HTML report to file.
        
        Args:
            html: HTML content
            output_dir: Output directory (default: signal_generator/output)
            filename: Filename (default: technical_signals_report_YYYY-MM-DD.html)
        
        Returns:
            Path to saved file
        """
        if output_dir is None:
            output_dir = Path(__file__).parent.parent / 'output'
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        if filename is None:
            date_str = datetime.now().strftime('%Y-%m-%d')
            prefix = self.report_settings.get('filename_prefix', 'technical_signals_report')
            filename = f"{prefix}_{date_str}.html"
        
        output_path = output_dir / filename
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logger.info(f"Saved report to {output_path}")
        return output_path
