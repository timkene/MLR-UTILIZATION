import requests

# Your Zoho Mail account ID
account_id = '8896202000000008002'

# API endpoint for sending mail
url = f"https://mail.zoho.com/api/accounts/{account_id}/messages"

# Access token (replace if refreshed)
access_token = "1000.9b3f3d16adcf7e64e23997674013d9c9.fda552549302ab76ff5b3b9379c74d80" 

headers = {
    "Authorization": f"Zoho-oauthtoken {access_token}",
    "Content-Type": "application/json"
}

data = {
    "fromAddress": "hello@clearlinehmo.com",
    "toAddress": "leocasey0@gmail.com",
    "subject": "Test Email from Zoho Mail API",
    "content": "Hello Leo,\n\nThis is a test email sent via Zoho Mail API using Python, do not reply to this email.\n\nBest regards,\nChukwuka"
}

response = requests.post(url, headers=headers, json=data)

if response.status_code == 200:
    print("✅ Email sent successfully!")
else:
    print(f"❌ Failed to send email. Status: {response.status_code}")
    print(response.text)

import requests

ACCESS_TOKEN = "1000.c8ec6d55c5116a6025d598909df25584.30c58b9f70a9ac764af5e0121ac747ba"

url = "https://mail.zoho.com/api/accounts"
headers = {"Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}"}

resp = requests.get(url, headers=headers)

print(resp.status_code)
print(resp.json())

