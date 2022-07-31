# lsa-cg-scraper

## Getting started

1. Create a virtual environment by running `python -m venv venv`
2. Activate your virtual environment by running `source venv/bin/activate`
3. Install dependencies by running `pip install -r requirements.txt`
4. Get a `aws_access_key_id` and `aws_secret_access_key` that has permission to write to S3.
5. Write the following into your `~/.aws/credentials` file:

   ```plaintext
   [cg-scraper]
   aws_access_key_id=XXX
   aws_secret_access_key=XXX
   ```

6. Run `python scraper.py`.
   You may need to set `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` on a [Mac for multi-threading](https://stackoverflow.com/a/52230415). Run `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES python scraper.py` instead.

EC2 cronjob:

```plaintext
MAILTO=<INSERT_EMAIL_HERE>
 13 *  *   *   *     cd /home/ubuntu/lsa-cg-scraper/ && /home/ubuntu/lsa-cg-scraper/env/bin/python scraper.py > output.log 2>&1
```
