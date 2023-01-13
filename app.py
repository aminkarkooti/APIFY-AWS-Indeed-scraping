
from chalice import Chalice
from apify_client import ApifyClient
import pymysql
import os

app = Chalice(app_name='indeed_proj')

# @app.route('/')
# def index():
#     return {'hello': 'world'}


@app.schedule('rate(3 days)')
def every_three_days(event):

    # Initialize the ApifyClient with your API token
       
    client= ApifyClient(os.environ['client'])
   
    # Prepare the Apify actor input
    pos_list = ['Data Scientist','Data Analyst','Data Engineer']
    run_input= {
        "position":'',
        "country": "CA",
        "location": "Canada",
        "maxItems": 350,
        "maxConcurrency": 5,
        "extendOutputFunction": """($) => {
        const result = {};
        // Uncomment to add a title to the output
        // result.title = $('title').text().trim();

        return result;
    }""",
        "proxyConfiguration": { "useApifyProxy": True },
    }

    #create connection to RDS (mysql) database
    
    connection = pymysql.connect(host= os.environ['host'],user= os.environ['user'], password = os.environ['password'], database = os.environ['database'] , port= 3306, autocommit = True,)
    connection.show_warnings()
    cur = connection.cursor()
    

    # Run the Apify actor and wait for it to finish
    for pos in pos_list:
        run_input['position']= pos
        run = client.actor("hynekhruska/indeed-scraper").call(run_input = run_input)
        
    
    # Fetch and print actor results from the run's dataset (if there are any)
    
        for item in client.dataset(run["defaultDatasetId"]).iterate_items():
            
            #writing data into RDS Database
            sql = "insert into jobs_ad (positionname, salary, jobtype, company, location, rating, reviewscount, url, id, postedat, scrapedat, description, externalapplylink) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            if type(item['jobType'])== list:
                item['jobType']= ' '.join(item['jobType']) # convert list to string
            cur.execute(sql,(item['positionName'], item['salary'], item['jobType'], item['company'], item['location'], item['rating'], item['reviewsCount'], item['url'], item['id'], item['postedAt'], item['scrapedAt'], item['description'], item['externalApplyLink']))
            connection.commit()
    connection.close()


    