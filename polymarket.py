import requests

url = "https://data-api.polymarket.com/positions?sizeThreshold=1&limit=100&sortBy=TOKENS&sortDirection=DESC&user=0x92a54267b56800430b2be9af0f768d18134f9631"

response = requests.get(url)

print(response.text)