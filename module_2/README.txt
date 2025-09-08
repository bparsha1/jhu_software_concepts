Name: Burch Parshall
JHED ID: 

Module Info: Module 2: Web Scraping due at 11:59:59 on 09/07/2025

Approach:  In scrape.py I set up my constant variables at the start for the URL, user agent, number of records I wanted, and output file.  Next, I created a function to check the robots.txt file for permission to scrape.  If scraping is allowed, I print that to screen, and print the opposite response if it isn't.
Next I have the scrape_data function, which handles the http requests with urllib3.  As I pull the data I store it in a dictionary per page.  I found that there were 20 entries per page, but I used regex to find the number of entries just in case.
Next is a save function to output the dictionary created by scrape_data as a json file.  Then in main I just call the functions in order, check permissions, scrape data, and save data.

In clean.py I used the tbody tag first because that gave me all the entries off of a page, and dropped the rest of the html.  Then I looked at rows.  There were either two or three for each entry.  There were three for entries that had comments.  I built functions for the first two, parse_status_and_date and parse_details_from_badges, because they followed a specific structure.  The comments row I dealt with in my clean_data function because it was just a single cell.  After getting to each of the different parts I used regex to make decisions about outputting the data.  I have a function that saves the data and one that loads the data from the previously saved json.  The main function runs the load function, then clean_data, which calls the two parsing functions, and then saves the data.



Known Bugs:
There are no known bugs, but there isn't a lot of error handling.  If something didn't work with the website it would cause problems.  When I ran the app.py provided, the universities came back as unknown and programs as "".  I'm not sure what caused that, but I didn't have time to look into it.