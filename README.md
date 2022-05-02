# lsa-cg-scraper

EC2 cronjob:

```plaintext
MAILTO=<INSERT_EMAIL_HERE>
 13 *  *   *   *     cd /home/ubuntu/lsa-cg-scraper/ && /home/ubuntu/lsa-cg-scraper/env/bin/python scraper.py > output.log 2>&1
```