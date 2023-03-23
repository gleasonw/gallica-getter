# gallica-getter
JSON API proxy for the French National Archive. Find documents where a word occurs, context for the occurrence, full text for OCR document pages. Python class represents each Gallica endpoint.

I hope to make this API easy to setup and deploy anywhere. 

Install the dependencies:

Set up the venv in your working directory:
```
virtualenv venv
```
Install the project dependencies from the requirements folder:
```
python3.11 -m pip install -r requirements.txt
```
Run the FastAPI app:
```
python3.11 -m uvicorn main:app --reload
```
Tests:

An end-end test suite calls each endpoint and verifies the result. 

```
python3.11 -m pytest gallicaGetter/tests/test_gallicaWrapper.py
```
