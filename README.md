### Brochure Page Views

## Description:
   This project runs a Spark job to create a dataset with user activities objects.
   The resulting data contains information about user's click on brochure. it contains:
   - user_ident: user identity id.
   - total_enters: Total number of pages the user entered.
   - total_exits: Total number of pages the user exited.
   - total_viewed: Total number of pages the user viewed (total_entered+total_page_turns).
   
## Pre-Reqs:
   - Python 3.6+
   - Java 8 
   
## Assumptions:
   I have assumed the following facts:
   - If user has entered the `DOUBLE_PAGE_MODE`, his `total_enters` are 2.
   - If user has exited from `DOUBLE_PAGE_MODE`, his `total_exits` are 2.
   - if user has turned to a page more than once, he has viewed it the same times. 
        i.e: if page=10 appears 3 times against a user, `total_viewed` are `total_enters+(3 * page view mode value)`.
   - if user has `entered` to a page, it means user has also `viewed` it.
        the page will be counted in `total_enters` as well as `total_viewed`.
   
## How to run:
   - run `pip install -r requirements.txt` to install required package(s).
   - run `python etl.py` to run the job.