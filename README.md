# lsa-cg-scraper

## Getting started

1. Create a virtual environment by running `python -m venv venv`
2. Activate your virtual environment by running `source venv/bin/activate`
3. Install dependencies by running `pip install -r requirements`

EC2 cronjob:

```plaintext
MAILTO=<INSERT_EMAIL_HERE>
 13 *  *   *   *     cd /home/ubuntu/lsa-cg-scraper/ && /home/ubuntu/lsa-cg-scraper/env/bin/python scraper.py > output.log 2>&1
```
