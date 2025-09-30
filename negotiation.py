import streamlit as st
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import json
from dataclasses import dataclass
from enum import Enum
from datetime import datetime, timedelta
import os
import io
import tempfile
import pyodbc
import toml
import duckdb
from dlt_sources import create_medicloud_connection, create_eacount_connection

# AI Integration (OpenAI) - Updated for new API
try:
    from openai import OpenAI
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False

def normalize_code(value: str) -> str:
    """Normalize procedure codes for robust matching: lowercase, trim, remove internal spaces."""
    if value is None:
        return ""
    try:
        return str(value).strip().lower().replace(" ", "")
    except Exception:
        return str(value)

class PriceLevel(Enum):
    EXCELLENT = 1  # Lowest price level
    GOOD = 2
    ACCEPTABLE = 3
    MAXIMUM = 4    # Highest acceptable price

@dataclass
class ProcedureStandard: 
    procedure_code: str
    procedure_name: str
    frequency: int  # Annual frequency/volume
    price_level_1: float  # Excellent (lowest)
    price_level_2: float  # Good
    price_level_3: float  # Acceptable
    price_level_4: float  # Maximum acceptable
    nhia_price: float = 0.0  # NHIA reference price
    market_avg: float = 0.0  # Market average from claims
    utilization_rate: float = 0.0  # Utilization rate from claims

class HospitalPriceNegotiator:
    def __init__(self, openai_api_key: str = None, enable_ai: bool = True):
        """
        Initialize the negotiator with pricing standards and optional AI capabilities
        """
        self.standards = {}
        self.negotiation_results = []
        self.ai_enabled = False
        self.openai_client = None
        self.claims_data = None
        self.tariff_data = None
        self.current_tariff = None
        
        # Initialize AI if available and requested
        if enable_ai and AI_AVAILABLE and openai_api_key:
            try:
                # Validate API key format
                if not openai_api_key.startswith('sk-'):
                    st.warning("⚠ Invalid OpenAI API key format. Key should start with 'sk-'")
                    self.ai_enabled = False
                else:
                    self.openai_client = OpenAI(api_key=openai_api_key)
                    # Test the API key with a simple call
                    try:
                        # Make a minimal test call to validate the key
                        test_response = self.openai_client.chat.completions.create(
                            model="gpt-3.5-turbo",
                            messages=[{"role": "user", "content": "test"}],
                            max_tokens=1
                        )
                        self.ai_enabled = True
                        st.success("✓ OpenAI API key validated successfully")
                    except Exception as test_error:
                        error_str = str(test_error).lower()
                        if "quota" in error_str or "429" in error_str or "insufficient_quota" in error_str:
                            st.error("❌ OpenAI API quota exceeded. AI features disabled.")
                            st.info("💡 Please add funds to your OpenAI account or wait for quota reset.")
                            self.ai_enabled = False
                            # Disable AI completely when quota is exceeded
                        else:
                            st.warning(f"⚠ OpenAI API key validation failed: {str(test_error)}")
                            self.ai_enabled = False
            except Exception as e:
                st.warning(f"⚠ Failed to initialize OpenAI client: {str(e)}")
                self.ai_enabled = False
        elif enable_ai and not AI_AVAILABLE:
            st.warning("⚠ OpenAI package not installed. AI features disabled.")
        elif enable_ai and not openai_api_key:
            st.info("ℹ OpenAI API key not provided. AI features disabled.")
    
    def load_standards_from_dataframe(self, df: pd.DataFrame):
        """Load pricing standards from DataFrame with robust data type handling"""
        try:
            # Ensure procedure_code is string
            df['procedure_code'] = df['procedure_code'].astype(str)

            # Normalize column names to lowercase without spaces/underscores for flexible detection
            df.columns = [c.strip() for c in df.columns]

            # Normalize procedure codes for matching
            df['procedure_code_norm'] = df['procedure_code'].apply(normalize_code)
            
            required_columns = ['procedure_code', 'procedure_name', 'price_level_1', 'price_level_2', 'price_level_3', 'price_level_4']
            
            if not all(col in df.columns for col in required_columns):
                missing_cols = [col for col in required_columns if col not in df.columns]
                raise ValueError(f"CSV must contain columns: {required_columns}. Missing: {missing_cols}")
            
            # Convert price columns to numeric
            price_columns = ['price_level_1', 'price_level_2', 'price_level_3', 'price_level_4']
            for col in price_columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Add frequency column if not present (default to 0)
            if 'frequency' not in df.columns:
                df['frequency'] = 0
            
            # Add NHIA price if not present (use level 1 as fallback)
            if 'nhia_price' not in df.columns:
                df['nhia_price'] = df['price_level_1']  # Use level 1 as fallback if NHIA price not provided
            
            # Remove rows with invalid data
            original_count = len(df)
            df = df.dropna(subset=price_columns)
            df = df[df['procedure_code'].notna() & (df['procedure_code'] != '')]
            
            if len(df) < original_count:
                st.warning(f"⚠ Removed {original_count - len(df)} rows with invalid data")
            
            successful_loads = 0
            errors = 0
            
            for _, row in df.iterrows():
                try:
                    # Additional validation
                    if (row['frequency'] >= 0 and 
                        row['price_level_1'] > 0 and 
                        row['price_level_2'] > 0 and 
                        row['price_level_3'] > 0 and 
                        row['price_level_4'] > 0):
                        
                        # Ensure price levels are in ascending order
                        if (row['price_level_1'] <= row['price_level_2'] <= 
                            row['price_level_3'] <= row['price_level_4']):
                            
                            standard = ProcedureStandard(
                                procedure_code=str(row['procedure_code_norm']).strip(),
                                procedure_name=str(row['procedure_name']).strip(),
                                frequency=int(row.get('frequency', 0)),
                                price_level_1=float(row['price_level_1']),
                                price_level_2=float(row['price_level_2']),
                                price_level_3=float(row['price_level_3']),
                                price_level_4=float(row['price_level_4']),
                                nhia_price=float(row.get('nhia_price', row['price_level_1']))
                            )
                            self.standards[standard.procedure_code] = standard
                            successful_loads += 1
                        else:
                            st.warning(f"⚠ Skipping {row['procedure_code']}: Price levels not in ascending order")
                            errors += 1
                    else:
                        st.warning(f"⚠ Skipping {row['procedure_code']}: Invalid price values")
                        errors += 1
                        
                except (ValueError, TypeError) as e:
                    st.warning(f"⚠ Skipping {row['procedure_code']}: {e}")
                    errors += 1
                    continue
                
            st.success(f"✓ Successfully loaded {successful_loads} pricing standards")
            if errors > 0:
                st.warning(f"⚠ Skipped {errors} invalid records")
            
        except Exception as e:
            st.error(f"✗ Error loading standards: {e}")
            raise e
    
    def load_claims_data(self):
        """Load claims data from MediCloud for market analysis"""
        try:
            with st.spinner("Loading claims data from MediCloud..."):
                conn = create_medicloud_connection()
                query = """
                    SELECT 
                        procedurecode,
                        AVG(approvedamount) as avg_approved_amount,
                        COUNT(*) as claim_count,
                        AVG(chargeamount) as avg_charge_amount,
                        AVG(deniedamount) as avg_denied_amount
                    FROM dbo.claims
                    WHERE datesubmitted >= DATEADD(year, -1, GETDATE())
                    AND procedurecode IS NOT NULL
                    AND approvedamount > 0
                    GROUP BY procedurecode
                """
                df = pd.read_sql(query, conn)
                conn.close()
                
                self.claims_data = df
                st.success(f"✓ Loaded claims data for {len(df)} procedures")
                return df
        except Exception as e:
            st.warning(f"⚠ Failed to load claims data: {str(e)}")
            self.claims_data = pd.DataFrame()
            return pd.DataFrame()
    
    def load_tariff_data(self):
        """Load current tariff data from MediCloud"""
        try:
            with st.spinner("Loading tariff data from MediCloud..."):
                conn = create_medicloud_connection()
                query = """
                    SELECT 
                        trfp.procedurecode,
                        AVG(trfp.tariffamount) as avg_tariff_amount,
                        COUNT(*) as tariff_count,
                        trf.tariffname
                    FROM dbo.procedure_tariff trfp
                    JOIN dbo.tariff trf ON trf.tariffid = trfp.tariffid
                    WHERE CAST(trf.expirydate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
                    AND trfp.procedurecode IS NOT NULL
                    GROUP BY trfp.procedurecode, trf.tariffname
                """
                df = pd.read_sql(query, conn)
                conn.close()
                
                self.tariff_data = df
                st.success(f"✓ Loaded tariff data for {len(df)} procedures")
                return df
        except Exception as e:
            st.warning(f"⚠ Failed to load tariff data: {str(e)}")
            self.tariff_data = pd.DataFrame()
            return pd.DataFrame()
    
    def enhance_standards_with_market_data(self):
        """Enhance standards with market data from claims and tariffs"""
        if self.claims_data is None:
            self.load_claims_data()
        if self.tariff_data is None:
            self.load_tariff_data()
        
        enhanced_count = 0
        for code, standard in self.standards.items():
            # Add claims data
            if not self.claims_data.empty:
                # Normalize source codes for robust matching
                df_claims = self.claims_data.copy()
                if 'procedurecode' in df_claims.columns:
                    df_claims['procedurecode_norm'] = df_claims['procedurecode'].astype(str).apply(normalize_code)
                    claims_match = df_claims[df_claims['procedurecode_norm'] == normalize_code(code)]
                else:
                    claims_match = pd.DataFrame()
            else:
                claims_match = pd.DataFrame()
            if not claims_match.empty:
                standard.market_avg = float(claims_match.iloc[0]['avg_approved_amount'])
                standard.utilization_rate = float(claims_match.iloc[0]['claim_count'])
                enhanced_count += 1
            
            # Add tariff data
            if not self.tariff_data.empty:
                df_tariff = self.tariff_data.copy()
                if 'procedurecode' in df_tariff.columns:
                    df_tariff['procedurecode_norm'] = df_tariff['procedurecode'].astype(str).apply(normalize_code)
                    tariff_match = df_tariff[df_tariff['procedurecode_norm'] == normalize_code(code)]
                else:
                    tariff_match = pd.DataFrame()
            else:
                tariff_match = pd.DataFrame()
            if not tariff_match.empty:
                standard.current_tariff = float(tariff_match.iloc[0]['avg_tariff_amount'])
        
        if enhanced_count > 0:
            st.success(f"✓ Enhanced {enhanced_count} standards with market data")
    
    def analyze_hospital_tariff(self, hospital_tariff: Dict[str, float], 
                              hospital_name: str = "Hospital",
                              hospital_context: str = "",
                              use_ai: bool = True,
                              negotiation_strategy: Dict = None) -> pd.DataFrame:
        """
        Analyze hospital tariff against standards and generate negotiation strategy
        """
        results = []
        
        for procedure_code, hospital_price in hospital_tariff.items():
            # Ensure hospital_price is a float
            try:
                hospital_price = float(hospital_price)
            except (ValueError, TypeError):
                st.warning(f"⚠ Skipping {procedure_code}: Invalid price value {hospital_price}")
                continue
            
            if procedure_code not in self.standards:
                results.append({
                    'procedure_code': procedure_code,
                    'procedure_name': 'Unknown Procedure',
                    'hospital_price': hospital_price,
                    'current_level': 'NOT_IN_STANDARD',
                    'negotiation_strategy': 'Request removal or benchmark against similar procedures',
                    'target_price': None,
                    'potential_savings': None,
                    'priority': 'LOW',
                    'frequency': 0,
                    'annual_impact': 0,
                    'ai_strategy': 'Not available - procedure not in standards',
                    'ai_confidence': 0.0
                })
                continue
            
            standard = self.standards[procedure_code]
            
            # Determine current price level
            current_level, strategy_message, target_price = self._determine_negotiation_strategy(
                hospital_price, standard, negotiation_strategy
            )
            
            potential_savings = max(0, hospital_price - target_price) if target_price else 0
            annual_impact = potential_savings * standard.frequency
            
            # Determine priority
            priority = self._calculate_priority(annual_impact, current_level, standard.frequency)
            
            result_row = {
                'procedure_code': procedure_code,
                'procedure_name': standard.procedure_name,
                'hospital_price': hospital_price,
                'target_price': target_price,
                'level_1_price': standard.price_level_1,
                'level_2_price': standard.price_level_2,
                'level_3_price': standard.price_level_3,
                'level_4_price': standard.price_level_4,
                'current_level': current_level,
                'negotiation_strategy': strategy_message,
                'potential_savings': potential_savings,
                'frequency': standard.frequency,
                'annual_impact': annual_impact,
                'priority': priority,
                'ai_strategy': 'Standard strategy applied',
                'ai_confidence': 0.0,
                'nhia_price': standard.nhia_price,
                'market_avg': standard.market_avg,
                'utilization_rate': standard.utilization_rate
            }
            
            results.append(result_row)
        
        df = pd.DataFrame(results)
        df = df.sort_values(['priority', 'annual_impact'], ascending=[False, False])
        
        # Add AI-enhanced strategies if enabled
        if use_ai and self.ai_enabled and self.openai_client:
            try:
                with st.spinner("🤖 Generating AI-powered negotiation strategies..."):
                    df = self._enhance_with_ai_strategies(df, hospital_context)
            except Exception as e:
                st.warning(f"⚠ AI enhancement failed: {str(e)}. Continuing with standard analysis.")
                # Continue with the analysis without AI enhancement
        
        return df
    
    def _enhance_with_ai_strategies(self, analysis_df: pd.DataFrame, 
                                  hospital_context: str = "") -> pd.DataFrame:
        """Enhance analysis with AI-generated strategies"""
        
        # Validate API client before proceeding
        if not self.openai_client:
            st.warning("⚠ OpenAI client not available. Skipping AI enhancement.")
            return analysis_df
        
        # Focus on high-priority items to manage API costs - reduced to 3 for minimal usage
        priority_items = analysis_df[
            analysis_df['priority'].isin(['CRITICAL'])
        ].head(3)  # Only process 3 CRITICAL items to minimize API usage
        
        enhanced_df = analysis_df.copy()
        
        if len(priority_items) == 0:
            st.info("ℹ No high-priority items found for AI enhancement.")
            return enhanced_df
        
        progress_bar = st.progress(0)
        total_items = len(priority_items)
        
        # Add rate limiting to prevent quota issues
        import time
        
        for i, (idx, row) in enumerate(priority_items.iterrows()):
            try:
                ai_result = self._generate_ai_strategy(row.to_dict(), hospital_context)
                enhanced_df.at[idx, 'ai_strategy'] = ai_result['ai_strategy']
                enhanced_df.at[idx, 'ai_confidence'] = ai_result['confidence_score']
                
                # Add small delay between API calls to prevent rate limiting
                if i < total_items - 1:  # Don't delay after the last call
                    time.sleep(0.5)
                    
            except Exception as e:
                enhanced_df.at[idx, 'ai_strategy'] = f"AI analysis failed: {str(e)[:50]}..."
                enhanced_df.at[idx, 'ai_confidence'] = 0.0
            
            progress_bar.progress((i + 1) / total_items)
        
        return enhanced_df
    
    def _generate_ai_strategy(self, procedure_data: Dict, hospital_context: str = "") -> Dict:
        """Generate AI-powered negotiation strategy for a specific procedure"""
        
        # Validate API key and client before making calls
        if not self.openai_client:
            return {
                "ai_strategy": "AI analysis unavailable: OpenAI client not initialized",
                "confidence_score": 0.0
            }
        
        prompt = f"""
Analyze this healthcare procedure pricing situation and provide a specific negotiation strategy:

PROCEDURE DETAILS:
- Code: {procedure_data.get('procedure_code', 'N/A')}
- Name: {procedure_data.get('procedure_name', 'N/A')}
- Hospital Price: ${procedure_data.get('hospital_price', 0):,.2f}
- Target Price: ${procedure_data.get('target_price', 0):,.2f}
- Current Level: {procedure_data.get('current_level', 'N/A')}
- Annual Frequency: {procedure_data.get('frequency', 0)}
- Annual Financial Impact: ${procedure_data.get('annual_impact', 0):,.2f}
- Priority: {procedure_data.get('priority', 'N/A')}

YOUR MARKET PRICE LEVELS:
- Level 1 (Excellent): ${procedure_data.get('level_1_price', 0):,.2f}
- Level 2 (Good): ${procedure_data.get('level_2_price', 0):,.2f}
- Level 3 (Acceptable): ${procedure_data.get('level_3_price', 0):,.2f}
- Level 4 (Maximum): ${procedure_data.get('level_4_price', 0):,.2f}

REFERENCE PRICES:
- NHIA Price: ${procedure_data.get('nhia_price', 0):,.2f}
- Market Average (Claims): ${procedure_data.get('market_avg', 0):,.2f}
- Utilization Rate: {procedure_data.get('utilization_rate', 0)}

HOSPITAL CONTEXT: {hospital_context}

Provide a concise negotiation strategy (2-3 sentences) including:
1. Key talking points
2. Leverage points (volume, competition, etc.)
3. Fallback options

Keep response professional and actionable.
"""
        
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are an expert healthcare pricing negotiator. Provide strategic, professional, and results-oriented advice in 2-3 sentences."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.7,
                max_tokens=150  # Reduced from 200 to save tokens
            )
            
            ai_strategy = response.choices[0].message.content.strip()
            confidence_score = self._calculate_ai_confidence(procedure_data)
            
            return {
                "ai_strategy": ai_strategy,
                "confidence_score": confidence_score
            }
            
        except Exception as e:
            error_msg = str(e)
            if "quota" in error_msg.lower() or "429" in error_msg:
                return {
                    "ai_strategy": "AI analysis unavailable: API quota exceeded. Please check your OpenAI billing and try again later.",
                    "confidence_score": 0.0
                }
            elif "insufficient_quota" in error_msg.lower():
                return {
                    "ai_strategy": "AI analysis unavailable: Insufficient API quota. Please add funds to your OpenAI account.",
                    "confidence_score": 0.0
                }
            else:
                return {
                    "ai_strategy": f"AI analysis unavailable: {error_msg[:100]}...",
                    "confidence_score": 0.0
                }
    
    def _calculate_ai_confidence(self, procedure_data: Dict) -> float:
        """Calculate confidence score for AI recommendations"""
        score = 1.0
        
        if not procedure_data.get('frequency'):
            score -= 0.2
        if not procedure_data.get('hospital_price'):
            score -= 0.3
        if procedure_data.get('current_level') == 'NOT_IN_STANDARD':
            score -= 0.4
        if procedure_data.get('market_avg', 0) > 0:
            score += 0.1  # Bonus for having market data
            
        return max(0.1, min(1.0, score))
    
    def _determine_negotiation_strategy(self, hospital_price: float, 
                                     standard: ProcedureStandard, 
                                     negotiation_strategy: Dict = None) -> Tuple[str, str, float]:
        """Determine negotiation strategy based on selected negotiation level and realistic hospital pricing"""
        
        hospital_price = float(hospital_price)
        level_1 = float(standard.price_level_1)  # Best price (excellent)
        level_2 = float(standard.price_level_2)  # Good price
        level_3 = float(standard.price_level_3)  # Acceptable price
        level_4 = float(standard.price_level_4)  # Maximum acceptable
        nhia_price = float(standard.nhia_price) if standard.nhia_price else level_1
        
        # Default strategy if none provided
        if not negotiation_strategy:
            negotiation_strategy = {
                "primary_target": "price_level_1",
                "fallback_target": "nhia_price",
                "description": "First Negotiation (Aggressive)"
            }
        
        # Determine primary and fallback targets based on negotiation level
        primary_target = negotiation_strategy.get("primary_target", "price_level_1")
        fallback_target = negotiation_strategy.get("fallback_target", "nhia_price")
        
        # Get target prices
        target_prices = {
            "price_level_1": level_1,
            "price_level_2": level_2, 
            "price_level_3": level_3,
            "price_level_4": level_4,
            "nhia_price": nhia_price
        }
        
        primary_price = target_prices[primary_target]
        fallback_price = target_prices[fallback_target]
        
        # Calculate markup percentages for context
        markup_from_nhia = ((hospital_price / nhia_price - 1) * 100) if nhia_price > 0 else 0
        markup_from_level1 = ((hospital_price / level_1 - 1) * 100) if level_1 > 0 else 0
        
        # Determine strategy based on hospital price vs targets
        if hospital_price <= primary_price:
            # Hospital price is at or below our primary target
            if primary_target == "price_level_1":
                return ("EXCELLENT", 
                       f"EXCELLENT - Hospital price (${hospital_price:.2f}) is at/below our best target (${primary_price:.2f}). Still negotiate 2-5% lower. NHIA: ${nhia_price:.2f}", 
                       hospital_price * 0.95)
            else:
                return ("GOOD", 
                       f"GOOD - Hospital price (${hospital_price:.2f}) meets our {primary_target.replace('_', ' ')} target (${primary_price:.2f}). NHIA: ${nhia_price:.2f}", 
                       hospital_price)
        
        elif hospital_price <= fallback_price:
            # Hospital price is between primary and fallback targets
            return ("ACCEPTABLE", 
                   f"ACCEPTABLE - Hospital price (${hospital_price:.2f}) is between targets. Push for {primary_target.replace('_', ' ')} (${primary_price:.2f}). NHIA: ${nhia_price:.2f}", 
                   primary_price)
        
        elif hospital_price <= level_4:
            # Hospital price is above fallback but within maximum acceptable
            return ("NEGOTIATE", 
                   f"NEGOTIATE - Hospital price (${hospital_price:.2f}) is {markup_from_nhia:.0f}% above NHIA. Target {primary_target.replace('_', ' ')} (${primary_price:.2f}). NHIA: ${nhia_price:.2f}", 
                   primary_price)
        
        else:
            # Hospital price exceeds maximum acceptable
            if hospital_price > nhia_price * 2.0:  # More than 100% above NHIA
                return ("REJECT", 
                       f"REJECT - Hospital price (${hospital_price:.2f}) is {markup_from_nhia:.0f}% above NHIA (${nhia_price:.2f}). Unrealistic markup. Target {primary_target.replace('_', ' ')} (${primary_price:.2f}) or exclude", 
                       primary_price)
            else:
                return ("REJECT", 
                       f"REJECT - Hospital price (${hospital_price:.2f}) exceeds maximum acceptable. Target {primary_target.replace('_', ' ')} (${primary_price:.2f}) or exclude. NHIA: ${nhia_price:.2f}", 
                       primary_price)
    
    def _calculate_priority(self, annual_impact: float, current_level: str, frequency: int) -> str:
        """Calculate negotiation priority based on new strategy levels"""
        if current_level in ["REJECT", "UNACCEPTABLE"] or annual_impact > 50000:
            return "CRITICAL"
        elif current_level in ["NEGOTIATE", "MAXIMUM"] or annual_impact > 10000 or frequency > 100:
            return "HIGH"
        elif current_level == "ACCEPTABLE" or annual_impact > 1000 or frequency > 50:
            return "MEDIUM"
        else:
            return "LOW"
    
    def generate_negotiation_report(self, analysis_df: pd.DataFrame, 
                                  hospital_name: str = "Hospital",
                                  include_ai: bool = True) -> str:
        """Generate a comprehensive negotiation report"""
        
        total_potential_savings = analysis_df['annual_impact'].sum()
        critical_items = len(analysis_df[analysis_df['priority'] == 'CRITICAL'])
        high_priority_items = len(analysis_df[analysis_df['priority'] == 'HIGH'])
        
        unacceptable_items = analysis_df[analysis_df['current_level'] == 'UNACCEPTABLE']
        
        # AI-enhanced items
        ai_enhanced_count = len(analysis_df[analysis_df['ai_confidence'] > 0])
        
        report = f"""
HOSPITAL PRICE NEGOTIATION REPORT - {hospital_name}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
{'='*70}

EXECUTIVE SUMMARY:
- Total Procedures Analyzed: {len(analysis_df)}
- Total Annual Potential Savings: ${total_potential_savings:,.2f}
- Critical Priority Items: {critical_items}
- High Priority Items: {high_priority_items}
- AI-Enhanced Strategies: {ai_enhanced_count} procedures

UNACCEPTABLE PRICES (MUST REJECT/RENEGOTIATE):
"""
        
        if len(unacceptable_items) > 0:
            report += unacceptable_items[['procedure_code', 'procedure_name', 'hospital_price', 'level_4_price', 'annual_impact']].to_string(index=False)
        else:
            report += "None"
        
        report += f"""

TOP 10 NEGOTIATION PRIORITIES:
{analysis_df.head(10)[['procedure_code', 'procedure_name', 'current_level', 'potential_savings', 'annual_impact']].to_string(index=False)}

"""
        
        if include_ai and ai_enhanced_count > 0:
            ai_items = analysis_df[analysis_df['ai_confidence'] > 0].head(5)
            report += f"""
AI-POWERED NEGOTIATION STRATEGIES (Top 5):
{'='*50}
"""
            for _, row in ai_items.iterrows():
                report += f"""
{row['procedure_name']} ({row['procedure_code']}):
Strategy: {row['ai_strategy']}
Confidence: {row['ai_confidence']:.1%}
---
"""
        
        priority_summary = analysis_df.groupby('priority').agg({
            'annual_impact': 'sum',
            'procedure_code': 'count'
        }).round(2)
        
        report += f"""
NEGOTIATION SUMMARY BY PRIORITY:
{priority_summary.to_string()}

RECOMMENDED NEXT STEPS:
1. Schedule immediate meeting for CRITICAL items
2. Prepare benchmark data for HIGH priority procedures
3. Consider volume commitments for additional discounts
4. Set minimum acceptable savings target: ${total_potential_savings * 0.6:,.2f}
"""
        
        return report
    
    def generate_executive_brief(self, analysis_df: pd.DataFrame, hospital_name: str) -> str:
        """Generate executive-level negotiation brief"""
        
        total_savings = analysis_df['annual_impact'].sum()
        critical_count = len(analysis_df[analysis_df['priority'] == 'CRITICAL'])
        
        # Get top 3 procedures by impact
        top_procedures = analysis_df.head(3)
        
        brief = f"""
EXECUTIVE NEGOTIATION BRIEF - {hospital_name}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
{'='*60}

STRATEGIC OVERVIEW:
Total Annual Savings Opportunity: ${total_savings:,.2f}
Critical Negotiation Items: {critical_count}
Recommended Approach: {"Aggressive" if critical_count > 5 else "Collaborative"}

TOP 3 NEGOTIATION PRIORITIES:
"""
        
        for i, (_, proc) in enumerate(top_procedures.iterrows(), 1):
            ai_insight = f"\nAI Strategy: {proc['ai_strategy'][:80]}..." if proc['ai_confidence'] > 0 else ""
            brief += f"""
{i}. {proc['procedure_name']} ({proc['procedure_code']})
   Current: ${proc['hospital_price']:,.2f} → Target: ${proc['target_price']:,.2f}
   Annual Impact: ${proc['annual_impact']:,.2f}{ai_insight}

"""
        
        brief += f"""
EXECUTIVE RECOMMENDATIONS:
• Minimum Acceptable Savings: ${total_savings * 0.5:,.2f} (50% of potential)
• Critical items must be resolved or excluded from contract
• Leverage competitive market data and volume commitments
• Consider phased implementation if hospital resistance is high
"""
        
        return brief

def load_hospital_tariff_from_dataframe(df: pd.DataFrame) -> Dict[str, float]:
    """Load hospital tariff from DataFrame"""
    
    # Look for procedure code column
    procedure_col = None
    possible_procedure_cols = ['procedure_code', 'code', 'proc_code', 'procedure', 'cpt_code', 'cpt']
    for col in possible_procedure_cols:
        if col in df.columns:
            procedure_col = col
            break
    
    # Look for price column
    price_col = None
    possible_price_cols = ['price', 'hospital_price', 'tariff', 'amount', 'cost', 'rate']
    for col in possible_price_cols:
        if col in df.columns:
            price_col = col
            break
    
    if not procedure_col:
        raise ValueError(f"Could not find procedure code column. Expected one of: {possible_procedure_cols}")
    
    if not price_col:
        raise ValueError(f"Could not find price column. Expected one of: {possible_price_cols}")
    
    # Normalize procedure codes and price column
    df[procedure_col] = df[procedure_col].astype(str).apply(normalize_code)
    df[price_col] = pd.to_numeric(df[price_col], errors='coerce')
    
    # Convert to dictionary
    hospital_tariff = {}
    successful_loads = 0
    errors = 0
    
    for _, row in df.iterrows():
        try:
            code = str(row[procedure_col]).strip()
            price = float(row[price_col])
            if code and not pd.isna(price) and price > 0:
                hospital_tariff[code] = price
                successful_loads += 1
            else:
                errors += 1
        except (ValueError, TypeError):
            errors += 1
            continue
    
    st.success(f"✓ Successfully loaded {successful_loads} hospital prices")
    if errors > 0:
        st.warning(f"⚠ Skipped {errors} invalid records")
        
    return hospital_tariff

def create_excel_download(analysis_df: pd.DataFrame, hospital_name: str) -> bytes:
    """Create Excel file with multiple sheets for download"""
    output = io.BytesIO()
    
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        # Main analysis
        analysis_df.to_excel(writer, sheet_name='Full Analysis', index=False)
        
        # Critical items only
        critical_df = analysis_df[analysis_df['priority'] == 'CRITICAL']
        if len(critical_df) > 0:
            critical_df.to_excel(writer, sheet_name='Critical Items', index=False)
        
        # AI-enhanced items
        ai_df = analysis_df[analysis_df['ai_confidence'] > 0]
        if len(ai_df) > 0:
            ai_df[['procedure_code', 'procedure_name', 'current_level', 
                   'annual_impact', 'ai_strategy', 'ai_confidence']].to_excel(
                writer, sheet_name='AI Strategies', index=False)
        
        # Summary by priority
        summary_df = analysis_df.groupby('priority').agg({
            'annual_impact': 'sum',
            'potential_savings': 'mean',
            'procedure_code': 'count'
        }).round(2)
        summary_df.to_excel(writer, sheet_name='Priority Summary')
    
    return output.getvalue()

def main():
    st.set_page_config(
        page_title="Hospital Price Negotiation System",
        page_icon="🏥",
        layout="wide"
    )
    
    st.title("🏥 Hospital Price Negotiation System with AI & Market Data")
    st.markdown("---")
    
    # Sidebar for configuration
    st.sidebar.header("Configuration")
    
    # Hospital information
    hospital_name = st.sidebar.text_input("Hospital Name", value="Regional Medical Center")
    hospital_context = st.sidebar.text_area(
        "Hospital Context (for AI analysis)", 
        placeholder="Enter context about the hospital (size, specialties, market position, etc.)",
        height=100
    )
    
    # Negotiation Strategy Configuration
    st.sidebar.header("Negotiation Strategy")
    negotiation_level = st.sidebar.selectbox(
        "Negotiation Level",
        options=[
            "First Negotiation (Aggressive)",
            "Second Negotiation (Moderate)", 
            "Third Negotiation (Conservative)",
            "Final Negotiation (Acceptable)"
        ],
        index=0,
        help="Select the negotiation level to determine target pricing strategy"
    )
    
    # Map negotiation level to strategy
    negotiation_strategies = {
        "First Negotiation (Aggressive)": {
            "primary_target": "price_level_1",
            "fallback_target": "nhia_price",
            "description": "Target best prices (Level 1 + NHIA) - hospitals expect high markups"
        },
        "Second Negotiation (Moderate)": {
            "primary_target": "price_level_2", 
            "fallback_target": "price_level_1",
            "description": "Target good prices (Level 2) after initial rejection"
        },
        "Third Negotiation (Conservative)": {
            "primary_target": "price_level_3",
            "fallback_target": "price_level_2", 
            "description": "Target acceptable prices (Level 3) for difficult negotiations"
        },
        "Final Negotiation (Acceptable)": {
            "primary_target": "price_level_4",
            "fallback_target": "price_level_3",
            "description": "Target maximum acceptable prices (Level 4) as final offer"
        }
    }
    
    selected_strategy = negotiation_strategies[negotiation_level]
    st.sidebar.info(f"**Strategy:** {selected_strategy['description']}")
    
    # AI Configuration
    # Check if AI is disabled via environment variable
    ai_disabled_by_env = os.getenv('DISABLE_AI', '').lower() in ['true', '1', 'yes']
    
    if ai_disabled_by_env:
        st.sidebar.info("ℹ️ AI features disabled via environment variable")
        use_ai = False
    else:
        use_ai = st.sidebar.checkbox("Enable AI-Enhanced Strategies", value=True)
    openai_api_key = None
    
    if use_ai:
        # Try to load API key from secrets.toml first
        try:
            secrets = toml.load("secrets.toml")
            openai_api_key = secrets.get("openai", {}).get("api_key")
        except:
            openai_api_key = None
        
        # If not found in secrets, ask user to input
        if not openai_api_key:
            openai_api_key = st.sidebar.text_input(
                "OpenAI API Key", 
                type="password",
                help="Enter your OpenAI API key to enable AI-powered negotiation strategies"
            )
        else:
            st.sidebar.success("✓ OpenAI API key loaded from configuration")
            if st.sidebar.button("Use Different Key"):
                openai_api_key = st.sidebar.text_input(
                    "New OpenAI API Key", 
                    type="password",
                    help="Enter a different OpenAI API key"
                )
        
        if not openai_api_key:
            st.sidebar.warning("⚠ Please enter your OpenAI API key to use AI features")
    
    # Show quota status if AI is enabled
    if use_ai and openai_api_key:
        st.sidebar.info("ℹ️ AI features will be tested when you run the analysis. If quota is exceeded, the script will continue without AI.")
    
    # Data source options
    st.sidebar.header("Data Sources")
    use_market_data = st.sidebar.checkbox("Use Market Data from MediCloud", value=True)
    use_claims_data = st.sidebar.checkbox("Use Claims Data for Analysis", value=True)
    
    # Main content area
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.header("📋 Upload Pricing Standards File")
        st.markdown("Upload your pricing standards CSV file with the following columns:")
        st.code("""
procedure_code,procedure_name,price_level_1,price_level_2,price_level_3,price_level_4,nhia_price
99213,Office Visit Level 3,75.00,85.00,95.00,110.00,80.00
        """)
        
        standards_file = st.file_uploader(
            "Choose pricing standards CSV file", 
            type=['csv'],
            key="standards"
        )
    
    with col2:
        st.header("🏥 Upload Hospital Tariff")
        st.markdown("Upload the hospital's tariff CSV file with procedure codes and prices:")
        st.code("""
procedure_code,price
99213,90.00
99214,180.00
        """)
        
        hospital_file = st.file_uploader(
            "Choose hospital tariff CSV file", 
            type=['csv'],
            key="hospital"
        )
    
    # Analysis section
    if standards_file and hospital_file:
        try:
            # Load data
            with st.spinner("Loading data files..."):
                # Load standards
                standards_df = pd.read_csv(standards_file)
                st.success(f"✓ Loaded {len(standards_df)} pricing standards")
                
                # Load hospital tariff
                hospital_df = pd.read_csv(hospital_file)
                hospital_tariff = load_hospital_tariff_from_dataframe(hospital_df)
                
                if not hospital_tariff:
                    st.error("No valid hospital prices found. Please check your file format.")
                    return
            
            # Initialize negotiator
            negotiator = HospitalPriceNegotiator(
                openai_api_key=openai_api_key if use_ai else None,
                enable_ai=use_ai
            )
            
            # Load standards (normalize inside)
            negotiator.load_standards_from_dataframe(standards_df)
            
            if not negotiator.standards:
                st.error("No valid standards loaded. Please check your file format.")
                return
            
            # Load market data if requested
            if use_market_data or use_claims_data:
                negotiator.enhance_standards_with_market_data()
            
            # Run analysis
            st.markdown("---")
            st.header("🔍 Analysis Results")
            
            with st.spinner("Analyzing hospital tariff against standards..."):
                analysis_df = negotiator.analyze_hospital_tariff(
                    hospital_tariff,
                    hospital_name,
                    hospital_context,
                    use_ai=use_ai and bool(openai_api_key),
                    negotiation_strategy=selected_strategy
                )
            
            # Display summary metrics
            col1, col2, col3, col4 = st.columns(4)
            
            total_savings = analysis_df['annual_impact'].sum()
            critical_count = len(analysis_df[analysis_df['priority'] == 'CRITICAL'])
            high_count = len(analysis_df[analysis_df['priority'] == 'HIGH'])
            procedures_count = len(analysis_df)
            
            with col1:
                st.metric("Total Procedures", procedures_count)
            
            with col2:
                st.metric("Potential Annual Savings", f"${total_savings:,.0f}")
            
            with col3:
                st.metric("Critical Items", critical_count)
            
            with col4:
                st.metric("High Priority Items", high_count)
            
            # Display results table
            st.subheader("📊 Detailed Analysis")
            
            # Filter options
            priority_filter = st.multiselect(
                "Filter by Priority",
                options=['CRITICAL', 'HIGH', 'MEDIUM', 'LOW'],
                default=['CRITICAL', 'HIGH']
            )
            
            if priority_filter:
                filtered_df = analysis_df[analysis_df['priority'].isin(priority_filter)]
            else:
                filtered_df = analysis_df
            
            # Display filtered results
            st.dataframe(
                filtered_df,
                use_container_width=True,
                height=400
            )
            
            # Reports section
            st.markdown("---")
            st.header("📄 Generated Reports")
            
            tab1, tab2 = st.tabs(["Comprehensive Report", "Executive Brief"])
            
            with tab1:
                comprehensive_report = negotiator.generate_negotiation_report(
                    analysis_df, 
                    hospital_name,
                    include_ai=use_ai
                )
                st.text(comprehensive_report)
            
            with tab2:
                executive_brief = negotiator.generate_executive_brief(analysis_df, hospital_name)
                st.text(executive_brief)
            
            # Download section
            st.markdown("---")
            st.header("💾 Download Results")
            
            col1, col2 = st.columns(2)
            
            with col1:
                # Excel download
                excel_data = create_excel_download(analysis_df, hospital_name)
                st.download_button(
                    label="📊 Download Excel Analysis",
                    data=excel_data,
                    file_name=f"{hospital_name.replace(' ', '_')}_negotiation_analysis.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            with col2:
                # CSV download
                csv_data = analysis_df.to_csv(index=False)
                st.download_button(
                    label="📋 Download CSV Data",
                    data=csv_data,
                    file_name=f"{hospital_name.replace(' ', '_')}_analysis.csv",
                    mime="text/csv"
                )
            
        except Exception as e:
            st.error(f"An error occurred during analysis: {str(e)}")
            st.exception(e)
    
    else:
        st.info("👆 Please upload both the standards file and hospital tariff file to begin analysis.")
        
        # Show sample data format
        st.markdown("---")
        st.header("📝 Sample Data Formats")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Pricing Standards File Format")
            sample_standards = pd.DataFrame({
                'procedure_code': ['99213', '99214', '71020'],
                'procedure_name': ['Office Visit Level 3', 'Office Visit Level 4', 'Chest X-Ray'],
                'price_level_1': [75.00, 125.00, 45.00],
                'price_level_2': [85.00, 140.00, 55.00],
                'price_level_3': [95.00, 155.00, 65.00],
                'price_level_4': [110.00, 175.00, 80.00],
                'nhia_price': [80.00, 150.00, 50.00]
            })
            st.dataframe(sample_standards)
        
        with col2:
            st.subheader("Hospital Tariff Format")
            sample_hospital = pd.DataFrame({
                'procedure_code': ['99213', '99214', '71020'],
                'price': [90.00, 180.00, 50.00]
            })
            st.dataframe(sample_hospital)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    **Instructions:**
    1. Prepare your pricing standards CSV with:
       - `procedure_code`, `procedure_name`: Procedure identification
       - `price_level_1` to `price_level_4`: Your market price levels (excellent to maximum)
       - `nhia_price`: NHIA reference price for comparison
    2. Prepare your hospital tariff CSV with procedure codes and their prices
    3. Upload both files using the file uploaders above
    4. Optionally enable AI features by providing your OpenAI API key
    5. Review the analysis results and download the comprehensive reports
    
    **Pricing Structure:**
    - **Your Market Levels**: price_level_1 (excellent) to price_level_4 (maximum acceptable)
    - **NHIA Price**: Official NHIA reference price for regulatory compliance
    - **Market Average**: Real claims data from your MediCloud database
    
    **Note:** AI features require a valid OpenAI API key and will incur API usage costs.
    
    **Recent Updates:**
    - Integrated with MediCloud for real-time market data
    - Added claims data analysis for better pricing insights
    - Enhanced AI strategies with market context
    - Improved negotiation recommendations based on utilization patterns
    """)

if __name__ == "__main__":
    main()