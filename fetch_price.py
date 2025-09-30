import os
import re
import statistics
import time
from serpapi import GoogleSearch
from playwright.sync_api import sync_playwright

def detect_quantity_and_normalize(price, text, drug_name):
    """Detect quantity from text and normalize price to per-unit"""
    text_lower = text.lower()
    drug_lower = drug_name.lower()
    
    # Check if it's already per unit
    if re.search(r'per\s*(?:tab|caps?|vial|bottle|tube)', text_lower):
        return price
    
    # Common quantity patterns
    quantity_patterns = [
        r'(\d+)\s*(?:tabs?|tablets?)',  # 10 tabs, 20 tablets
        r'(\d+)\s*(?:caps?|capsules?)',  # 10 caps, 20 capsules
        r'(\d+)\s*(?:vials?|ampoules?)',  # 5 vials, 10 ampoules
        r'(\d+)\s*(?:bottles?|syrups?)',  # 2 bottles, 3 syrups
        r'(\d+)\s*(?:tubes?|creams?)',  # 1 tube, 2 creams
        r'x(\d+)',  # x10, x20 (common in drug names)
        r'(\d+)\s*pack',  # 10 pack
        r'(\d+)\s*strip',  # 10 strip
        r'(\d+)\s*blister',  # 10 blister
        r'(\d+)\s*ml',  # 100ml, 200ml
    ]
    
    # Look for quantity patterns
    for pattern in quantity_patterns:
        matches = re.findall(pattern, text_lower)
        for match in matches:
            try:
                quantity = int(match)
                if quantity > 1:  # Only normalize if quantity > 1
                    per_unit_price = price / quantity
                    # Reasonable per-unit price range
                    if 10 <= per_unit_price <= 10000:
                        return int(per_unit_price)
            except (ValueError, ZeroDivisionError):
                continue
    
    # Special handling for different drug forms
    if any(word in drug_lower for word in ['syrup', 'suspension', 'bottle']):
        # Syrups and suspensions are typically per bottle
        return price
    elif any(word in drug_lower for word in ['injection', 'vial', 'ampoule']):
        # Injections are typically per vial/ampoule
        return price
    else:
        # For tablets/capsules, try to find quantity in the text
        # Look for patterns like "10 tablets", "20 caps", etc.
        quantity_match = re.search(r'(\d+)\s*(?:tabs?|caps?|tablets?|capsules?)', text_lower)
        if quantity_match:
            try:
                quantity = int(quantity_match.group(1))
                if quantity > 1:
                    per_unit_price = price / quantity
                    if 10 <= per_unit_price <= 10000:
                        return int(per_unit_price)
            except (ValueError, ZeroDivisionError):
                pass
    
    # If no quantity found, return original price
    return price

# 1. Search Google for the drug
def search_prices(drug_name):
    api_key = "65dfba613dfc3b3b10403977c1e170c433d1e8282e65e2e06eac6304568eb4d0"
    if not api_key:
        print("Error: SERPAPI_KEY environment variable not set")
        return []
    
    params = {
        "engine": "google",
        "q": f"{drug_name} price Nigeria",
        "api_key": api_key,
        "num": 10  # Get more results
    }
    try:
        print(f"Searching with query: {params['q']}")
        search = GoogleSearch(params)
        results = search.get_dict()
        print(f"Raw results keys: {list(results.keys())}")
        
        if "error" in results:
            print(f"API Error: {results['error']}")
            return []
            
        organic_results = results.get("organic_results", [])
        print(f"Organic results count: {len(organic_results)}")
        
        links = [r['link'] for r in organic_results[:8]]
        print(f"Found {len(links)} search results")
        return links
    except Exception as e:
        print(f"Search error: {e}")
        return []

# 2. Scrape pharmacy pages for prices in ₦
def scrape_prices(links, drug_name):
    prices = []
    if not links:
        return prices
        
    with sync_playwright() as p:
        browser = p.firefox.launch(headless=True)
        page = browser.new_page()
        
        for i, url in enumerate(links):
            try:
                print(f"Scraping {i+1}/{len(links)}: {url}")
                page.goto(url, timeout=30000)
                page.wait_for_load_state('networkidle')
                text = page.content()
                
                # Multiple price patterns
                patterns = [
                    r"₦\s?[\d,]+",  # ₦1,000
                    r"NGN\s?[\d,]+",  # NGN 1000
                    r"Naira\s?[\d,]+",  # Naira 1000
                    r"Price[:\s]*₦?\s?[\d,]+",  # Price: ₦1000
                    r"Cost[:\s]*₦?\s?[\d,]+",  # Cost: ₦1000
                ]
                
                for pattern in patterns:
                    found = re.findall(pattern, text, re.IGNORECASE)
                    for f in found:
                        # Extract number from the match
                        number_match = re.search(r'[\d,]+', f)
                        if number_match:
                            price = int(number_match.group().replace(",", ""))
                            # Filter reasonable prices (₦50 - ₦50,000)
                            if 50 <= price <= 50000:
                                # Try to normalize to per-unit
                                normalized_price = detect_quantity_and_normalize(price, text, drug_name)
                                if normalized_price != price:
                                    prices.append(normalized_price)
                                    print(f"  Found price: ₦{price} → normalized to ₦{normalized_price} per unit")
                                else:
                                    prices.append(price)
                                    print(f"  Found price: ₦{price}")
                
                time.sleep(1)  # Be respectful to servers
                
            except Exception as e:
                print(f"Error scraping {url}: {e}")
                
        browser.close()
    return prices

# 3. Aggregate results
def analyze(prices):
    if not prices:
        return {"error": "No prices found"}
    
    # Remove duplicates and sort
    unique_prices = sorted(list(set(prices)))
    
    return {
        "min_price": min(unique_prices),
        "max_price": max(unique_prices),
        "average_price": round(statistics.mean(unique_prices), 2),
        "median_price": round(statistics.median(unique_prices), 2),
        "count": len(unique_prices),
        "samples": unique_prices[:10]  # show some samples
    }

def main():
    # Try different variations of the drug name
    drug_variations = [
        "arthemeter lumefantrine 80mg/400mg tablet",
        "arthemeter lumefantrine tablet", 
        "artemether lumefantrine",
        "coartem tablet",
        "artemether lumefantrine 80/480mg",
        "malaria treatment tablet",
        "artemether",
        "lumefantrine"
    ]
    
    for drug in drug_variations:
        print(f"Searching for {drug} prices in Nigeria...")
        links = search_prices(drug)
        if links:
            print(f"Found {len(links)} results for: {drug}")
            break
        else:
            print(f"No results for: {drug}")
    
    if not links:
        print("No search results found for any variation.")
        print("Let's try with a very simple drug to test the script...")
        drug = "paracetamol"
        print(f"Searching for {drug} prices in Nigeria...")
        links = search_prices(drug)
        if not links:
            print("Script test failed. Check SERPAPI_KEY and internet connection.")
            return
    
    prices = scrape_prices(links, drug)
    summary = analyze(prices)
    
    print(f"\nPrice summary for {drug} in Nigeria:")
    if "error" in summary:
        print(f"❌ {summary['error']}")
        print("Try:")
        print("- Different drug name variations")
        print("- Check if SERPAPI_KEY is set correctly")
        print("- Verify internet connection")
    else:
        print(f"✅ Found {summary['count']} prices")
        print(f"💰 Min: ₦{summary['min_price']:,}")
        print(f"💰 Max: ₦{summary['max_price']:,}")
        print(f"💰 Average: ₦{summary['average_price']:,}")
        print(f"💰 Median: ₦{summary['median_price']:,}")
        print(f"📊 Sample prices: {summary['samples']}")

if __name__ == "__main__":
    main()

