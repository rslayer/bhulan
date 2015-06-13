import requests

API_KEY = "0938198967c61b688987b732da2cb47854c825c4"
URL = "https://alikamil.cartodb.com/api/v1/imports/?api_key="


def sendtoCartodb(fileloc):
    files = {'file': open(fileloc)}
    r = requests.post(URL+API_KEY,files=files)
    print r.text
    return '0'

#headers = {'Authorization': 'bearer 0DwdUsqII2ApAJ8nPsZJfYxzFImzIsa8oeBus2VXPlUlMF95v32r2UmEC1SOzmIYsswuvfwXKicpqhpp6psi1YL2kmtWqAQVBi5iT0C9kXRGRwLEQ5MtiTeyUUwNvWkP0FnID8RUi0FxvYQCo321IIy7smrWXkfHHySI8yuJnE8sTSBRjwcr6NzBV7xk2KyjWunPHG6lzQA1QPIjMRf9JcxEachEKuQc691i1qXnGcVCtOLXLslUWYOIW3VDQj0l'}
#r = requests.get("http://api.wisesystems.com/v1/vehicles",headers=headers)

