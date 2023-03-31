import requests

# make test post request to localhost:8000/api/downloadCSV

if __name__ == "__main__":
    response = requests.post(
        "http://localhost:8000/api/downloadCSV",
        json={
            "terms": ["brazza"],
            "year": 1900,
            "end_year": 1905
        },
    )
    print(response.text)
