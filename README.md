# gallica-getter
JSON API proxy for the French National Archive. Find documents where a word occurs, context for the occurrence, full text for OCR document pages. Python class represents each Gallica endpoint.

This API should be deployable anywhere. 

## Setup

Set up the venv in the cloned directory:
```
virtualenv venv
```
Activate the venv:
```
source venv/bin/activate
```
Install the project dependencies from the requirements folder:
```
python -m pip install -r requirements.txt
```
Run the FastAPI app locally:
```
python -m uvicorn main:app --reload
```
Tests:

An end-end test suite calls each endpoint and verifies the result. 

```
python -m pytest gallicaGetter/tests/test_gallicaWrapper.py
```
