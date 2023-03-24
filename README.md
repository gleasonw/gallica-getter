# gallica-getter
Find documents where a word occurs, context for the occurrence, full text for OCR document pages. Python class represents each Gallica endpoint.

Deploy this API anywhere. Railway, Google App Engine, AWS, a Raspberry PI. 
## Setup

Build your Python venv in the cloned gallica-getter directory:
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

## Test

An end-end test suite calls each Gallica wrapper endpoint and verifies the result. 

```
python -m pytest gallicaGetter/tests/test_gallicaWrapper.py
```

