import pandas as pd
import requests
import time
import logging

# 🧾 Load your existing tickers CSV
df = pd.read_csv("snowflake_chart.csv")

# 🌐 GraphQL endpoint and headers
url = "https://simplywall.st/graphql"
headers = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Origin": "https://simplywall.st",
    "Referer": "https://simplywall.st",
    "apollographql-client-name": "web",
    "apollographql-client-version": "mono-auto-89001bda"
}

# 🔐 Replace with your valid cookie(s)
cookies = {
    "cf_clearance": "TP8Nf.X7P3cO0IY3LY7xWqUtgZwG0RHyIkuW31pqTCk-1749991196-1.2.1.1-3LGRsne0cQfOCpH1jUkxBRM_aXf9GfcuMCcryUjGjo2iEnYM1LTiwhi87bk9T5Ad1EIm2pbRi4_A3Ts3vHUKDqzYT1.kn5bRR7kIdzMxKqagF9.k8JPgq712jNz7RZ3_53Gzzwud3HhhYIliprQy73ufwiffjvZuorMpO49BBpFk43MrMvxqxkr5fl0S0YhU0CeJ6Y2L8VSqfhs4yT7ljphY1i3WLityove0ZGs1ZKyQxHx4kZQ6YtYyJgPMx9uJB5pVVFd849UB0_KDBaQsrCCg86Ta4NET_kVW_lLtGWv_qd10snv2bago7SZl3iHt35u4x8lEAnVQM96xWgQ.DNp31UEJsFF0w8y6mNT98Ag"
}

# 🧠 GraphQL query for Snowflake score
query = """
query CompanySummary($canonicalUrl: String!) {
  Company(canonicalURL: $canonicalUrl) {
    score {
      value
      future
      past
      health
      dividend
    }
  }
}
"""

# 🔄 Iterate and fetch scores
for i, row in df.iterrows():
    ticker = row["tickers"]
    canonical_url = row["canonical_url"]

    if pd.isna(canonical_url) or not canonical_url.startswith("/stocks/"):
        print(f"⚠️ Skipping {ticker}: Invalid or missing canonical URL")
        continue

    payload = {
        "query": query,
        "variables": {"canonicalUrl": canonical_url},
        "operationName": "CompanySummary"
    }

    try:
        resp = requests.post(url, headers=headers, cookies=cookies, json=payload)
        resp.raise_for_status()
        data = resp.json()
        score = data.get("data", {}).get("Company", {}).get("score", {})

        if score:
            df.at[i, "value"] = score.get("value")
            df.at[i, "future"] = score.get("future")
            df.at[i, "past"] = score.get("past")
            df.at[i, "health"] = score.get("health")
            df.at[i, "dividend"] = score.get("dividend")
            print(f"✅ {ticker}: score updated")
        else:
            print(f"⚠️ {ticker}: no score returned")

    except Exception as e:
        print(f"❌ {ticker}: {e}")

    time.sleep(0.5)  # polite pause between requests

# 💾 Save updates back into CSV
df.to_csv("snowflake_chart.csv", index=False)
print("\n✅ All scores written back to snowflake_chart.csv")

