#!/usr/bin/env python3
"""
Test the negotiation script without AI features
"""

import os
import sys
import pandas as pd
from negotiation import HospitalPriceNegotiator, load_hospital_tariff_from_dataframe

def test_basic_functionality():
    """Test basic negotiation functionality without AI"""
    
    print("🧪 Testing Hospital Price Negotiation (No AI)")
    print("=" * 50)
    
    # Create sample data
    standards_data = {
        'procedure_code': ['99213', '99214', '71020'],
        'procedure_name': ['Office Visit Level 3', 'Office Visit Level 4', 'Chest X-Ray'],
        'price_level_1': [75.00, 125.00, 45.00],
        'price_level_2': [85.00, 140.00, 55.00],
        'price_level_3': [95.00, 155.00, 65.00],
        'price_level_4': [110.00, 175.00, 80.00],
        'nhia_price': [80.00, 150.00, 50.00]
    }
    
    hospital_data = {
        'procedure_code': ['99213', '99214', '71020'],
        'price': [90.00, 180.00, 50.00]
    }
    
    standards_df = pd.DataFrame(standards_data)
    hospital_df = pd.DataFrame(hospital_data)
    
    print("📊 Sample data created:")
    print("Standards:", len(standards_df), "procedures")
    print("Hospital:", len(hospital_df), "procedures")
    
    # Initialize negotiator without AI
    print("\n🔧 Initializing negotiator (AI disabled)...")
    negotiator = HospitalPriceNegotiator(openai_api_key=None, enable_ai=False)
    
    # Load standards
    print("📋 Loading pricing standards...")
    negotiator.load_standards_from_dataframe(standards_df)
    
    # Load hospital tariff
    print("🏥 Loading hospital tariff...")
    hospital_tariff = load_hospital_tariff_from_dataframe(hospital_df)
    
    # Run analysis with first negotiation strategy
    print("🔍 Running analysis...")
    negotiation_strategy = {
        "primary_target": "price_level_1",
        "fallback_target": "nhia_price",
        "description": "First Negotiation (Aggressive)"
    }
    
    analysis_df = negotiator.analyze_hospital_tariff(
        hospital_tariff,
        "Test Hospital",
        "",
        use_ai=False,
        negotiation_strategy=negotiation_strategy
    )
    
    print("\n✅ Analysis completed successfully!")
    print(f"📊 Results: {len(analysis_df)} procedures analyzed")
    
    # Show summary
    total_savings = analysis_df['annual_impact'].sum()
    critical_count = len(analysis_df[analysis_df['priority'] == 'CRITICAL'])
    
    print(f"\n📈 Summary:")
    print(f"   Total Potential Savings: ${total_savings:,.2f}")
    print(f"   Critical Items: {critical_count}")
    print(f"   AI Strategies: {len(analysis_df[analysis_df['ai_confidence'] > 0])}")
    
    # Show top 3 results
    print(f"\n🏆 Top 3 Negotiation Priorities:")
    top_3 = analysis_df.head(3)
    for i, (_, row) in enumerate(top_3.iterrows(), 1):
        print(f"   {i}. {row['procedure_name']} ({row['procedure_code']})")
        print(f"      Current: ${row['hospital_price']:,.2f} → Target: ${row['target_price']:,.2f}")
        print(f"      Strategy: {row['negotiation_strategy']}")
        print()
    
    print("✅ Test completed successfully! The script works without AI features.")
    return True

if __name__ == "__main__":
    try:
        test_basic_functionality()
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
