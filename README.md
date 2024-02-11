# gallica-getter

Find documents where a word occurs, context for the occurrence, full text for OCR document pages. Compose Gallica services with service-specific classes.

Alongside Gallica wrappers, this project contains a JSON API. Deploy this API anywhere. Railway, Google App Engine, AWS, Fly.io, a Raspberry PI.

## Setup

Build your Python venv in the cloned gallica-getter directory:

```
virtualenv venv
```

Activate the venv:

```
source venv/bin/activate
```

Install the project dependencies from the requirements file:

```
python -m pip install -r requirements.txt
```

Run the FastAPI app locally:

```
python -m uvicorn main:app --reload
```

The server should be running at port 8000. Try a request:

```
http://localhost:8000/api/gallicaRecords?terms=portland&source=periodical&link_term=oregon&link_distance=20
```

## Test

An end-end test suite calls each Gallica wrapper endpoint and verifies the result.

```
python -m pytest app/tests/gallicaE2E.py
```
