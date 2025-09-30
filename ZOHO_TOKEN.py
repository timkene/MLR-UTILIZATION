import requests

# --- Your Zoho credentials ---
CLIENT_ID = "1000.N7OTEZEMAV4AS2X2FEC0P7P2PYJIZC"
CLIENT_SECRET = "39b86ec1a58c47c68ec3b5f3f044f2df7142bb1e2f"
REFRESH_TOKEN = "1000.0fafe8457278c99308fdd487c9c46709.c74a0b6a3f87cee8109e00eb37b48d43"

# --- Function to get a fresh access token ---
def get_access_token():
    url = "https://accounts.zoho.com/oauth/v2/token"
    data = {
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    response = requests.post(url, data=data)
    resp_json = response.json()

    if "access_token" in resp_json:
        return resp_json["access_token"]
    else:
        raise Exception(f"Failed to get access token: {resp_json}")

# --- Example usage ---
if __name__ == "__main__":
    token = get_access_token()
    print("✅ Fresh access token:", token)


